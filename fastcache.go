// Package fastcache implements fast in-memory cache.
//
// The package has been extracted from https://victoriametrics.com/
package turbocache

import (
	"fmt"
	"github.com/ShareChat/turbo-cache/internal/primeNumber"
	"github.com/ShareChat/turbo-cache/spinlock"
	xxhash "github.com/cespare/xxhash/v2"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const setBufSize = 128
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
	// Drops due to write limit
	DroppedWrites uint64
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

func (b *bucket) stopAsyncWriting() {
	b.stopWriting <- &struct{}{}
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
	c.syncWrite = config.syncWrite
	maxBucketBytes := uint64((config.maxBytes + bucketsCount - 1) / bucketsCount)
	for i := range c.buckets[:] {
		c.buckets[i].Init(maxBucketBytes, config.flushIntervalMillis, config.maxWriteBatch, config.syncWrite)
		c.buckets[i].dropWriting = config.dropWriteOnHighContention
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

func (c *Cache) setSync(k, v []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Set(k, v, h, true)
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
	if !c.syncWrite {
		for i := range c.buckets[:] {
			c.buckets[i].stopAsyncWriting()
		}
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

	setBuf      chan *insertValue
	stopWriting chan *struct{}
	dropWriting bool
	// m maps hash(k) to idx of (k, v) pair in chunks.
	m       map[uint64]uint64
	flusher *flusher
	// idx points to chunks for writing the next (k, v) pair.
	idx atomic.Uint64

	// gen is the generation of chunks.
	gen uint64

	getCalls        uint64
	setCalls        uint64
	batchSetCalls   uint64
	misses          uint64
	collisions      uint64
	corruptions     uint64
	writeBufferSize uint64
	dropsInQueue    uint64
	droppedWrites   uint64
	duplicatedCount uint64
	latestTimestamp int64
}

func (b *bucket) Init(maxBytes uint64, flushInterval int64, maxBatch int, syncWrite bool) {
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
		b.startProcessingWriteQueue(flushInterval, maxBatch)
	}
}

func (b *bucket) startProcessingWriteQueue(flushInterval int64, maxBatch int) {
	b.setBuf = make(chan *insertValue, setBufSize)
	b.stopWriting = make(chan *struct{})
	b.flusher = &flusher{
		count:               0,
		idx:                 b.idx.Load(),
		gen:                 b.gen,
		currentChunkId:      b.idx.Load() / chunkSize,
		currentFlushChunkId: 0,
		totalChunkCount:     uint64(len(b.chunks)),
		needClean:           false,
	}
	b.flusher.chunks = make([]flushChunk, 4)

	itemsPerChunk := maxBatch
	if initItemsPerFlushChunk < itemsPerChunk {
		itemsPerChunk = initItemsPerFlushChunk
	}
	for i := 0; i < 4; i++ {
		b.flusher.chunks[i] = flushChunk{
			chunkId:    0,
			chunk:      getChunk()[:0],
			h:          make([]uint64, 0, itemsPerChunk),
			idx:        make([]uint64, 0, itemsPerChunk),
			gen:        make([]uint64, 0, itemsPerChunk),
			size:       0,
			cleanChunk: false,
		}
	}
	b.flusher.chunkSynced.Store(b.flusher.chunks)
	index := make([]flushChunkIndexItem, primeNumber.NextPrime(uint64(maxBatch)))
	b.flusher.index.Store(index)
	go func() {
		b.randomDelay(flushInterval)
		t := time.Tick(time.Duration(flushInterval) * time.Millisecond)
		for {
			select {
			case i := <-b.setBuf:
				b.onNewItem(i, maxBatch, flushInterval)
			case _ = <-t:
				b.onFlushTick(flushInterval)
			case <-b.stopWriting:
				return
			}
		}
	}()
}

func (b *bucket) onFlushTick(flushInterval int64) {
	if b.writeBufferSize > 0 && time.Since(time.UnixMilli(b.latestTimestamp)).Milliseconds() >= flushInterval {
		b.setBatchInternalMostOptimised(b.flusher)
		b.cleanFlusher(b.flusher)
	}
}

func (b *bucket) onNewItem(i *insertValue, maxBatch int, flushInterval int64) {
	f := b.flusher

	index := b.flusher.index.Load().([]flushChunkIndexItem)
	indexItem := &index[i.h%uint64(len(index))]
	duplicated := indexItem.exists(i.h)
	forceFlush := false
	if !duplicated {
		kvLength := uint64(4) + uint64(len(i.K)) + uint64(len(i.V))
		idxNew, newChunk := f.incrementIndexes(kvLength)
		if newChunk {
			f.currentFlushChunkId++
		}
		if f.currentFlushChunkId <= len(f.chunks) {
			flushChunk := &f.chunks[f.currentFlushChunkId]

			lenBuf := makeKvLenBuf(i.K, i.V)
			flushChunk.chunk = append(flushChunk.chunk, lenBuf[:]...)
			flushChunk.chunk = append(flushChunk.chunk, i.K...)
			flushChunk.chunk = append(flushChunk.chunk, i.V...)
			flushChunk.h = append(flushChunk.h, i.h)
			flushChunk.idx = append(flushChunk.idx, f.idx)
			flushChunk.gen = append(flushChunk.gen, f.gen)
			flushChunk.chunkId = f.currentChunkId
			flushChunk.cleanChunk = newChunk

			indexItem.saveToIndex(i.h, flushChunk.size, f.currentFlushChunkId)
			b.flusher.index.Store(index)

			flushChunk.size += kvLength
			f.idx = idxNew
			f.count++
			if b.latestTimestamp == 0 {
				b.latestTimestamp = time.Now().UnixMilli()
			}
			b.flusher.chunkSynced.Store(b.flusher.chunks)
			atomic.AddUint64(&b.writeBufferSize, 1)
		} else {
			atomic.AddUint64(&b.droppedWrites, 1)
			forceFlush = true
		}

	} else {
		atomic.AddUint64(&b.duplicatedCount, 1)
	}

	if f.count >= maxBatch || (f.count > 0 && (time.Since(time.UnixMilli(b.latestTimestamp)).Milliseconds() >= flushInterval || forceFlush)) {
		b.setBatchInternalMostOptimised(f)
		b.cleanFlusher(f)
	}
}

func (i *flushChunkIndexItem) saveToIndex(h uint64, flushIdx uint64, chunk int) {
	for j := range i.h {
		if i.h[j] == 0 {
			i.h[j] = h
			i.currentIdx[j] = flushIdx
			i.flushChunk[j] = chunk
			break
		}
	}
}

func (b *bucket) cleanFlusher(f *flusher) {
	index := b.flusher.index.Load().([]flushChunkIndexItem)
	f.flushed.Store(true)
	f.spinlock.Lock()

	for i := 0; i < len(index); i++ {
		for j := range index[i].h {
			if index[i].h[j] != 0 {
				index[i].h[j] = 0
				index[i].currentIdx[j] = 0
				index[i].flushChunk[j] = 0
			} else {
				break
			}
		}
	}

	for j := range f.chunks {
		f.chunks[j].clean()
	}
	f.chunkSynced.Store(f.chunks)
	f.spinlock.Unlock()
	f.flushed.Store(false)

	f.currentFlushChunkId = 0
	f.count = 0
	b.latestTimestamp = 0
	atomic.StoreUint64(&b.writeBufferSize, 0)
}

func (b *flushChunk) clean() {
	b.h = b.h[:0]
	b.idx = b.idx[:0]
	b.chunk = b.chunk[:0]
	b.size = 0
	b.cleanChunk = false
	b.gen = b.gen[:0]
}

func (b *bucket) setBatchInternalMostOptimised(f *flusher) {
	atomic.AddUint64(&b.setCalls, uint64(f.count))
	atomic.AddUint64(&b.batchSetCalls, 1)
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := 0; i <= f.currentFlushChunkId; i++ {
		f := &f.chunks[i]
		chunk := b.chunks[f.chunkId]
		if chunk == nil {
			chunk = getChunk()[:0]
		} else if f.cleanChunk {
			chunk = chunk[:0]
		}

		b.chunks[f.chunkId] = append(chunk, f.chunk...)

		for j := 0; j < len(f.h); j++ {
			b.m[f.h[j]] = f.idx[j] | (f.gen[j] << bucketSizeBits)
		}
	}
	b.idx.Store(f.idx)
	b.gen = f.gen
	if f.needClean {
		b.cleanLocked(f.idx)
	}
}

func (b *bucket) randomDelay(maxDelayMillis int64) {
	jitterDelay := rand.Int63() % (maxDelayMillis * 1000)
	time.Sleep(time.Duration(jitterDelay) * time.Microsecond)
}

func (b *bucket) Reset() {
	b.mu.Lock()
	chunks := b.chunks
	for i := range chunks {
		putChunk(chunks[i])
		chunks[i] = nil //
	}
	b.m = make(map[uint64]uint64)
	b.idx.Store(0)
	b.gen = 1
	b.mu.Unlock()
	atomic.StoreUint64(&b.getCalls, 0)
	atomic.StoreUint64(&b.setCalls, 0)
	atomic.StoreUint64(&b.misses, 0)
	atomic.StoreUint64(&b.collisions, 0)
	atomic.StoreUint64(&b.corruptions, 0)
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
	s.DropsInQueue += atomic.LoadUint64(&b.dropsInQueue)
	s.DroppedWrites += atomic.LoadUint64(&b.droppedWrites)
	s.WriteQueueSize += atomic.LoadUint64(&b.writeBufferSize) + uint64(len(b.setBuf))
	s.SetBatchCalls += atomic.LoadUint64(&b.batchSetCalls)
	s.DuplicatedCount += atomic.LoadUint64(&b.duplicatedCount)

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

func (b *bucket) set(kv []byte, h uint64, idx uint64, chunkIdx uint64, needClean bool) (newNeedClean bool, newIdx uint64, newChunkId uint64) {
	chunks := b.chunks
	idxNew := idx + uint64(len(kv))
	chunkIdxNew := idxNew / chunkSize
	if chunkIdxNew > chunkIdx {
		if chunkIdxNew >= uint64(len(chunks)) {
			idx = 0
			idxNew = uint64(len(kv))
			chunkIdx = 0
			b.gen++
			if b.gen&((1<<genSizeBits)-1) == 0 {
				b.gen++
			}
			needClean = true
		} else {
			idx = chunkIdxNew * chunkSize
			idxNew = idx + uint64(len(kv))
			chunkIdx = chunkIdxNew
		}
		chunks[chunkIdx] = chunks[chunkIdx][:0]
	}

	chunk := chunks[chunkIdx]
	if chunk == nil {
		chunk = getChunk()[:0]
	}

	chunk = append(chunk, kv...)

	chunks[chunkIdx] = chunk
	b.m[h] = idx | (b.gen << bucketSizeBits)

	return needClean, idxNew, chunkIdx
}

func (b *bucket) Set(k, v []byte, h uint64, sync bool) {
	if len(k)+len(v)+kvLenBufSize >= chunkSize {
		return
	}
	if sync {
		b.setWithLock(k, v, h)
	} else {

		setBuf := &insertValue{
			K: k,
			V: v,
			h: h,
		}
		if b.dropWriting {
			select {
			case b.setBuf <- setBuf:
				return
			default:
				atomic.AddUint64(&b.dropsInQueue, 1)
			}
		} else {
			b.setBuf <- setBuf
		}

	}
}

// todo: copy previos implementation
func (b *bucket) setWithLock(k, v []byte, h uint64) {
	atomic.AddUint64(&b.setCalls, 1)
	if len(k) >= (1<<16) || len(v) >= (1<<16) {
		// Too big key or value - its length cannot be encoded
		// with 2 bytes (see below). Skip the entry.
		return
	}
	kvLenBuf := makeKvLenBuf(k, v)
	kv := make([]byte, 0, len(k)+len(v)+4)
	kv = append(kv, kvLenBuf[:]...)
	kv = append(kv, k...)
	kv = append(kv, v...)
	needClean := false
	b.mu.Lock()
	idx := b.idx.Load()
	chunkIdx := idx / chunkSize
	defer b.mu.Unlock()
	if needClean, idx, _ = b.set(kv, h, idx, chunkIdx, needClean); needClean {
		b.cleanLocked(idx)
	}
	b.idx.Store(idx)
}

func makeKvLenBuf(k []byte, v []byte) [4]byte {
	var kvLenBuf [4]byte
	kvLenBuf[0] = byte(uint16(len(k)) >> 8)
	kvLenBuf[1] = byte(len(k))
	kvLenBuf[2] = byte(uint16(len(v)) >> 8)
	kvLenBuf[3] = byte(len(v))
	return kvLenBuf
}

func (b *bucket) Get(dst, k []byte, h uint64, returnDst bool) ([]byte, bool, bool) {
	atomic.AddUint64(&b.getCalls, 1)

	bytes, found := b.tryFindInFlushBuffer(dst, k, h, returnDst)
	if found {
		return bytes, found, true
	}
	chunks := b.chunks
	b.mu.RLock()
	currentIdx := b.idx.Load()
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
			if string(k) == string(chunk[idx:idx+keyLen]) {
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
		atomic.AddUint64(&b.misses, 1)
	}
	return dst, found, false
}

func (b *bucket) tryFindInFlushBuffer(dst []byte, k []byte, h uint64, returnDst bool) ([]byte, bool) {
	if b.flusher == nil {
		return dst, false
	}

	found := false
	if !b.flusher.flushed.Load() {
		index := b.flusher.index.Load().([]flushChunkIndexItem)
		indexItem := (index)[h%uint64(len(index))]
		for i := range indexItem.h {
			if indexItem.h[i] == 0 {
				break
			} else if indexItem.h[i] == h {
				if b.flusher.spinlock.TryRLock() {
					index = b.flusher.index.Load().([]flushChunkIndexItem)
					indexItem = (index)[h%uint64(len(index))]
					if indexItem.h[i] == h {
						chunkId := indexItem.flushChunk[i]
						flushIdx := indexItem.currentIdx[i]
						chunk := b.flusher.chunkSynced.Load().([]flushChunk)[chunkId]
						kvLenBuf := chunk.chunk[flushIdx : flushIdx+kvLenBufSize]
						keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
						if keyLen == uint64(len(k)) && string(k) == string(chunk.chunk[flushIdx+kvLenBufSize:flushIdx+kvLenBufSize+keyLen]) {
							if returnDst {
								valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
								dst = append(dst, chunk.chunk[flushIdx+kvLenBufSize+keyLen:flushIdx+kvLenBufSize+keyLen+valLen]...)
								found = true
							}
						}
					}
					b.flusher.spinlock.RUnlock()
				}
			}
		}
		if found {
			return dst, found
		}
	}
	return dst, false
}

func (b *bucket) Del(h uint64) {
	b.mu.Lock()
	delete(b.m, h)
	b.mu.Unlock()
}

type insertValue struct {
	K, V []byte
	h    uint64
}

type Config struct {
	maxBytes                  int
	flushIntervalMillis       int64
	maxWriteBatch             int
	syncWrite                 bool
	dropWriteOnHighContention bool
}

func NewSyncWriteConfig(maxBytes int) *Config {
	return &Config{
		maxBytes:  maxBytes,
		syncWrite: true,
	}
}

func NewConfig(maxBytes int, flushInterval int64, maxWriteBatch int) *Config {
	return &Config{
		maxBytes:            maxBytes,
		flushIntervalMillis: flushInterval,
		maxWriteBatch:       maxWriteBatch,
	}
}

func NewConfigWithDroppingOnContention(maxBytes int, flushInterval int64, maxWriteBatch int) *Config {
	return &Config{
		maxBytes:                  maxBytes,
		flushIntervalMillis:       flushInterval,
		maxWriteBatch:             maxWriteBatch,
		dropWriteOnHighContention: true,
	}
}

type bufferItem struct {
	data           []byte
	locked         atomic.Bool
	prelockForEdit atomic.Bool
	index          []uint64
	hash           []uint64
}

func makeNewBufferItem(initDataSize int) *bufferItem {
	r := &bufferItem{
		data:  make([]byte, 0, initDataSize),
		index: make([]uint64, 0, 4),
		hash:  make([]uint64, 0, 4),
	}
	return r
}

func (item *bufferItem) doLockedReading(updateFunc func(item *bufferItem)) bool {
	if item.prelockForEdit.Load() {
		return false
	}
	return item.doLocked(updateFunc)
}

func (item *bufferItem) doLocked(updateFunc func(item *bufferItem)) bool {
	if item.locked.CompareAndSwap(false, true) {
		updateFunc(item)
		item.locked.Store(false)
		return true
	}
	return false
}

func (item *bufferItem) doLockedEdit(updateFunc func(item *bufferItem)) {
	item.prelockForEdit.Store(true)
	defer item.prelockForEdit.Store(false)
	for !item.doLocked(updateFunc) {
		time.Sleep(1 * time.Millisecond)
	}
}

type flushStruct struct {
	kv         []byte
	h          uint64
	chunk      uint64
	idx        uint64
	gen        uint64
	cleanChunk bool
}

type flusher struct {
	count               int
	chunks              []flushChunk
	chunkSynced         atomic.Value
	index               atomic.Value
	idx                 uint64
	gen                 uint64
	currentChunkId      uint64
	currentFlushChunkId int
	totalChunkCount     uint64
	needClean           bool
	flushed             atomic.Bool
	spinlock            spinlock.RWMutex
}

func (f *flusher) incrementIndexes(kvLength uint64) (idxNew uint64, newChunk bool) {
	idxNew = f.idx + kvLength
	chunkIdxNew := idxNew / chunkSize
	if chunkIdxNew > f.currentChunkId {
		if chunkIdxNew >= f.totalChunkCount {
			f.idx = 0
			idxNew = kvLength
			f.currentChunkId = 0
			f.gen++
			if f.gen&((1<<genSizeBits)-1) == 0 {
				f.gen++
			}
			f.needClean = true
		} else {
			f.idx = chunkIdxNew * chunkSize
			idxNew = f.idx + kvLength
			f.currentChunkId = chunkIdxNew
		}
		newChunk = true
	}
	return idxNew, newChunk
}

type flushChunk struct {
	chunkId    uint64
	chunk      []byte
	h          []uint64
	idx        []uint64
	gen        []uint64
	size       uint64
	cleanChunk bool
}

type flushChunkIndexItem struct {
	flushChunk [7]int
	h          [7]uint64
	currentIdx [7]uint64
}

func (i *flushChunkIndexItem) exists(h uint64) bool {
	for _, hv := range i.h {
		if hv == h {
			return true
		} else if hv == 0 {
			break
		}
	}
	return false
}

var insertValuePool = &sync.Pool{New: func() any {
	return &insertValue{}
}}

func getPooledInsertValue() *insertValue {
	return insertValuePool.Get().(*insertValue)
}

func releasePooledInsertValue(i *insertValue) {
	i.K = nil
	i.V = nil
	i.h = 0

	insertValuePool.Put(i)
}
