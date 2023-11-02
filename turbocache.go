// Package turbocache implements fast in-memory cache.
package turbocache

import (
	"fmt"
	"github.com/cespare/xxhash/v2"
	"sync"
	"sync/atomic"
)

const setBufSize = 64
const defaultMaxWriteSizeBatch = 250
const defaultFlushIntervalMillis = 5

const bucketsCount = 512

const chunkSize = 64 * 1024

const bucketSizeBits = 40

const genSizeBits = 64 - bucketSizeBits

const maxGen = 1<<genSizeBits - 1

const maxBucketSize uint64 = 1 << bucketSizeBits
const initItemsPerFlushChunk = 128
const kvLenBufSize = 4

// Stats represents cache stats.
//
// Use Cache.UpdateStats for obtaining fresh stats from the cache.
type Stats struct {
	// GetCalls is the number of Get calls.
	GetCalls       uint64
	BucketGetCalls []uint64
	// SetCalls is the number of Set calls.
	SetCalls        uint64
	BucketsSetCalls []uint64
	SetBatchCalls   uint64
	DuplicatedCount uint64
	// Misses is the number of cache misses.
	Misses uint64

	// Collisions is the number of cache collisions.
	//
	// Usually the number of collisions must be close to zero.
	// High number of collisions suggest something wrong with cache.
	Collisions uint64

	// Corruptions is the number of detected corruptions of the cache.
	//
	// Corruptions may occur when corrupted cache is loaded from file.
	Corruptions uint64

	// EntriesCount is the current number of entries in the cache.
	EntriesCount uint64

	// BytesSize is the current size of the cache in bytes.
	BytesSize uint64

	// MaxBytesSize is the maximum allowed size of the cache in bytes (aka capacity).
	MaxBytesSize uint64
	// drops due to buffer overflow
	DropsInQueue uint64
	//queue write
	WriteQueueSize uint64

	// BigStats contains stats for GetBig/SetBig methods.
	BigStats
}

// Reset resets s, so it may be re-used again in Cache.UpdateStats.
func (s *Stats) Reset() {
	*s = Stats{}
}

// BigStats contains stats for GetBig/SetBig methods.
type BigStats struct {
	// GetBigCalls is the number of GetBig calls.
	GetBigCalls uint64

	// SetBigCalls is the number of SetBig calls.
	SetBigCalls uint64

	// TooBigKeyErrors is the number of calls to SetBig with too big key.
	TooBigKeyErrors uint64

	// InvalidMetavalueErrors is the number of calls to GetBig resulting
	// to invalid metavalue.
	InvalidMetavalueErrors uint64

	// InvalidValueLenErrors is the number of calls to GetBig resulting
	// to a chunk with invalid length.
	InvalidValueLenErrors uint64

	// InvalidValueHashErrors is the number of calls to GetBig resulting
	// to a chunk with invalid hash value.
	InvalidValueHashErrors uint64
}

func (bs *BigStats) reset() {
	atomic.StoreUint64(&bs.GetBigCalls, 0)
	atomic.StoreUint64(&bs.SetBigCalls, 0)
	atomic.StoreUint64(&bs.TooBigKeyErrors, 0)
	atomic.StoreUint64(&bs.InvalidMetavalueErrors, 0)
	atomic.StoreUint64(&bs.InvalidValueLenErrors, 0)
	atomic.StoreUint64(&bs.InvalidValueHashErrors, 0)
}

// Cache is a fast thread-safe inmemory cache optimized for big number
// of entries.
//
// It has much lower impact on GC comparing to a simple `map[string][]byte`.
//
// Use New or LoadFromFile* for creating new cache instance.
// Concurrent goroutines may call any Cache methods on the same cache instance.
//
// Call Reset when the cache is no longer needed. This reclaims the allocated
// memory.
type Cache struct {
	buckets [bucketsCount]bucket

	bigStats BigStats

	syncWrite bool
}

// New returns new cache with the given maxBytes capacity in bytes.
//
// maxBytes must be smaller than the available RAM size for the app,
// since the cache holds data in memory.
//
// If maxBytes is less than 32MB, then the minimum cache capacity is 32MB.
func New(config *Config) *Cache {
	if config.maxBytes <= 0 {
		panic(fmt.Errorf("maxBytes must be greater than 0; got %d", config.maxBytes))
	}
	if config.flushIntervalMillis == 0 {
		config.flushIntervalMillis = defaultFlushIntervalMillis
	}
	if config.maxWriteBatch == 0 {
		config.maxWriteBatch = defaultMaxWriteSizeBatch
	}

	var c Cache
	c.syncWrite = config.maxBytes <= 1 || config.flushIntervalMillis <= 0
	maxBucketBytes := uint64((config.maxBytes + bucketsCount - 1) / bucketsCount)
	for i := range c.buckets[:] {
		c.buckets[i].Init(maxBucketBytes, config.flushIntervalMillis, config.maxWriteBatch, config.flushChunkCount, c.syncWrite)
	}
	return &c
}

// Set stores (k, v) in the cache.
//
// Get must be used for reading the stored entry.
//
// The stored entry may be evicted at any time either due to cache
// overflow or due to unlikely hash collision.
// Pass higher maxBytes value to New if the added items disappear
// frequently.
//
// (k, v) entries with summary size exceeding 64KB aren't stored in the cache.
// SetBig can be used for storing entries exceeding 64KB.
//
// k and v contents may be modified after returning from Set.
func (c *Cache) Set(k, v []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Set(k, v, h, c.syncWrite)
}

// Get appends value by the key k to dst and returns the result.
//
// Get allocates new byte slice for the returned value if dst is nil.
//
// Get returns only values stored in c via Set.
//
// k contents may be modified after returning from Get.
func (c *Cache) Get(dst, k []byte) []byte {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	dst, _, _ = c.buckets[idx].Get(dst, k, h, true)
	return dst
}

// HasGet works identically to Get, but also returns whether the given key
// exists in the cache. This method makes it possible to differentiate between a
// stored nil/empty value versus and non-existing value.
func (c *Cache) HasGet(dst, k []byte) ([]byte, bool) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	get, b, _ := c.buckets[idx].Get(dst, k, h, true)
	return get, b
}

// Has returns true if entry for the given key k exists in the cache.
func (c *Cache) Has(k []byte) bool {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	_, ok, _ := c.buckets[idx].Get(nil, k, h, false)
	return ok
}

// Del deletes value for the given k from the cache.
//
// k contents may be modified after returning from Del.
func (c *Cache) Del(k []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Del(h)
}

// Reset removes all the items from the cache.
func (c *Cache) Reset() {
	for i := range c.buckets[:] {
		c.buckets[i].Reset()
	}
	c.bigStats.reset()
}

func (c *Cache) Close() {
	c.Reset()
	c.stopFlushing()
}

func (c *Cache) stopFlushing() {
	for i := range c.buckets[:] {
		c.buckets[i].logger.stopFlushing()
	}
}

// UpdateStats adds cache stats to s.
//
// Call s.Reset before calling UpdateStats if s is re-used.
func (c *Cache) UpdateStats(s *Stats, details bool) {
	s.WriteQueueSize = 0
	s.BucketGetCalls = make([]uint64, 0, bucketsCount)
	s.BucketsSetCalls = make([]uint64, 0, bucketsCount)
	for i := range c.buckets[:] {
		c.buckets[i].UpdateStats(s, details)
	}
	s.GetBigCalls += atomic.LoadUint64(&c.bigStats.GetBigCalls)
	s.SetBigCalls += atomic.LoadUint64(&c.bigStats.SetBigCalls)
	s.TooBigKeyErrors += atomic.LoadUint64(&c.bigStats.TooBigKeyErrors)
	s.InvalidMetavalueErrors += atomic.LoadUint64(&c.bigStats.InvalidMetavalueErrors)
	s.InvalidValueLenErrors += atomic.LoadUint64(&c.bigStats.InvalidValueLenErrors)
	s.InvalidValueHashErrors += atomic.LoadUint64(&c.bigStats.InvalidValueHashErrors)
}

type bucket struct {
	mu sync.RWMutex

	// chunks is a ring buffer with encoded (k, v) pairs.
	// It consists of 64KB chunks.
	chunks [][]byte

	// m maps hash(k) to idx of (k, v) pair in chunks.
	m      map[uint64]uint64
	logger writeAheadLogger
	// idx points to chunks for writing the next (k, v) pair.
	idx uint64

	// gen is the generation of chunks.
	gen uint64

	getCalls      uint64
	setCalls      uint64
	batchSetCalls uint64
	misses        uint64
	collisions    uint64
	corruptions   uint64
}

func (b *bucket) Init(maxBytes uint64, flushInterval int64, maxBatch int, flushChunks int, syncWrite bool) {
	if maxBytes == 0 {
		panic(fmt.Errorf("maxBytes cannot be zero"))
	}
	if maxBytes >= maxBucketSize {
		panic(fmt.Errorf("too big maxBytes=%d; should be smaller than %d", maxBytes, maxBucketSize))
	}
	maxChunks := (maxBytes + chunkSize - 1) / chunkSize
	b.chunks = make([][]byte, maxChunks)
	b.m = make(map[uint64]uint64)
	b.Reset()
	if !syncWrite {
		b.logger = newLogger(b, maxBatch, flushChunks, b.idx, b.gen, uint64(len(b.chunks)), flushInterval)
	}
}

func (b *bucket) Reset() {
	b.mu.Lock()
	chunks := b.chunks
	for i := range chunks {
		putChunk(chunks[i])
		chunks[i] = nil //
	}
	b.m = make(map[uint64]uint64)
	b.idx = 0
	b.gen = 1
	b.mu.Unlock()
	atomic.StoreUint64(&b.getCalls, 0)
	atomic.StoreUint64(&b.setCalls, 0)
	atomic.StoreUint64(&b.misses, 0)
	atomic.StoreUint64(&b.collisions, 0)
	atomic.StoreUint64(&b.corruptions, 0)
}

func (b *bucket) UpdateStats(s *Stats, details bool) {
	getCalls := atomic.LoadUint64(&b.getCalls)
	s.GetCalls += getCalls
	if details {
		s.BucketGetCalls = append(s.BucketGetCalls, getCalls)
	}

	setCalls := atomic.LoadUint64(&b.setCalls)
	s.SetCalls += setCalls
	if details {
		s.BucketsSetCalls = append(s.BucketsSetCalls, setCalls)
	}
	s.Misses += atomic.LoadUint64(&b.misses)
	s.Collisions += atomic.LoadUint64(&b.collisions)
	s.Corruptions += atomic.LoadUint64(&b.corruptions)
	s.SetBatchCalls += atomic.LoadUint64(&b.batchSetCalls)

	loggerStats := b.logger.getStats()
	s.DropsInQueue += atomic.LoadUint64(&loggerStats.dropsInQueue)
	s.WriteQueueSize += atomic.LoadUint64(&loggerStats.writeBufferSize)
	s.DuplicatedCount += atomic.LoadUint64(&loggerStats.duplicatedCount)

	b.mu.RLock()
	s.EntriesCount += uint64(len(b.m))
	bytesSize := uint64(0)
	for _, chunk := range b.chunks {
		bytesSize += uint64(cap(chunk))
	}
	s.MaxBytesSize += uint64(len(b.chunks)) * chunkSize
	b.mu.RUnlock()
	s.BytesSize += bytesSize
}

func (b *bucket) Set(k, v []byte, h uint64, sync bool) {
	if len(k)+len(v)+kvLenBufSize >= chunkSize {
		return
	}
	if sync {
		b.syncSet(k, v, h)
	} else {
		b.logger.log(k, v, h)
	}
}

func (b *bucket) Get(dst, k []byte, h uint64, returnDst bool) ([]byte, bool, bool) {
	atomic.AddUint64(&b.getCalls, 1)
	found := false

	chunks := b.chunks
	b.mu.RLock()
	currentIdx := b.idx
	v := b.m[h]
	bGen := b.gen & ((1 << genSizeBits) - 1)
	if v > 0 {
		gen := v >> bucketSizeBits
		idx := v & ((1 << bucketSizeBits) - 1)
		if gen == bGen && idx < currentIdx || gen+1 == bGen && idx >= currentIdx || gen == maxGen && bGen == 1 && idx >= currentIdx {
			chunkIdx := idx / chunkSize
			if chunkIdx >= uint64(len(chunks)) {
				goto end
			}
			chunk := chunks[chunkIdx]
			idx %= chunkSize
			if idx+kvLenBufSize >= chunkSize {
				//removed stats for corruption
				goto end
			}
			kvLenBuf := chunk[idx : idx+kvLenBufSize]
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
			idx += kvLenBufSize
			if idx+keyLen+valLen >= chunkSize {
				//removed stats for corruption
				goto end
			}
			s := string(chunk[idx : idx+keyLen])
			if string(k) == s {
				idx += keyLen
				if returnDst {
					dst = append(dst, chunk[idx:idx+valLen]...)
				}
				found = true
			} else {
				//removed stats for collision
			}
		}
	}
end:
	b.mu.RUnlock()
	if !found {
		bytes, found := b.logger.lookup(dst, k, h, returnDst)
		if found {
			return bytes, found, true
		} else {
			atomic.AddUint64(&b.misses, 1)
		}
	}
	return dst, found, false
}

func (b *bucket) Del(h uint64) {
	b.mu.Lock()
	delete(b.m, h)
	b.mu.Unlock()
}

func (b *bucket) syncSet(k, v []byte, h uint64) {
	atomic.AddUint64(&b.setCalls, 1)
	if len(k) >= (1<<16) || len(v) >= (1<<16) {
		// Too big key or value - its length cannot be encoded
		// with 2 bytes (see below). Skip the entry.
		return
	}
	kvLenBuf := makeKvLenBuf(k, v)
	kvLen := uint64(len(kvLenBuf) + len(k) + len(v))
	if kvLen >= chunkSize {
		// Do not store too big keys and values, since they do not
		// fit a chunk.
		return
	}

	chunks := b.chunks
	needClean := false
	b.mu.Lock()
	idx := b.idx
	idxNew := idx + kvLen
	chunkIdx := idx / chunkSize
	chunkIdxNew := idxNew / chunkSize
	if chunkIdxNew > chunkIdx {
		if chunkIdxNew >= uint64(len(chunks)) {
			idx = 0
			idxNew = kvLen
			chunkIdx = 0
			b.gen++
			if b.gen&((1<<genSizeBits)-1) == 0 {
				b.gen++
			}
			needClean = true
		} else {
			idx = chunkIdxNew * chunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}
		chunks[chunkIdx] = chunks[chunkIdx][:0]
	}
	chunk := chunks[chunkIdx]
	if chunk == nil {
		chunk = getChunk()
		chunk = chunk[:0]
	}
	chunk = append(chunk, kvLenBuf[:]...)
	chunk = append(chunk, k...)
	chunk = append(chunk, v...)
	chunks[chunkIdx] = chunk
	b.m[h] = idx | (b.gen << bucketSizeBits)
	b.idx = idxNew
	if needClean {
		b.cleanLocked(idxNew)
	}
	b.mu.Unlock()
}

func (b *bucket) cleanLocked(idx uint64) {
	bGen := b.gen & ((1 << genSizeBits) - 1)
	bIdx := idx
	bm := b.m
	for k, v := range bm {
		gen := v >> bucketSizeBits
		idx = v & ((1 << bucketSizeBits) - 1)
		if (gen+1 == bGen || gen == maxGen && bGen == 1) && idx >= bIdx || gen == bGen && idx < bIdx {
			continue
		}
		delete(bm, k)
	}
}
