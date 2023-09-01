// Package fastcache implements fast in-memory cache.
//
// The package has been extracted from https://victoriametrics.com/
package turbocache

import (
	"fmt"
	"github.com/ShareChat/turbo-cache/internal/primeNumber"
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
	WriteQueueSize   uint64
	OnFlightSetCalls uint64

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

	writeLimiter *limiter
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
	c.writeLimiter = newLimiter(int32(config.concurrentWriteLimit))
	maxBucketBytes := uint64((config.maxBytes + bucketsCount - 1) / bucketsCount)
	for i := range c.buckets[:] {
		c.buckets[i].Init(maxBucketBytes, config.flushIntervalMillis, config.maxWriteBatch, config.syncWrite, c.writeLimiter)
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
	dst, _ = c.buckets[idx].Get(dst, k, h, true)
	return dst
}

// HasGet works identically to Get, but also returns whether the given key
// exists in the cache. This method makes it possible to differentiate between a
// stored nil/empty value versus and non-existing value.
func (c *Cache) HasGet(dst, k []byte) ([]byte, bool) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	return c.buckets[idx].Get(dst, k, h, true)
}

// Has returns true if entry for the given key k exists in the cache.
func (c *Cache) Has(k []byte) bool {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	_, ok := c.buckets[idx].Get(nil, k, h, false)
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
	s.OnFlightSetCalls = uint64(c.writeLimiter.onFlight.Load())
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
	m           map[uint64]uint64
	writeCache  []atomic.Value
	flushBuffer []flushStruct
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
	limiter         *limiter
}

func (b *bucket) Init(maxBytes uint64, flushInterval int64, maxBatch int, syncWrite bool, writeLimiter *limiter) {
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
	b.limiter = writeLimiter
	b.writeCache = make([]atomic.Value, primeNumber.NextPrime(uint64(maxBatch*2)))
	for i := range b.writeCache {
		b.writeCache[i].Store(makeNewBufferItem(4096))
	}
	if !syncWrite {
		b.startProcessingWriteQueue(flushInterval, maxBatch)
	}
}

func (b *bucket) startProcessingWriteQueue(flushInterval int64, maxBatch int) {
	maxBufferValueSize := 4 * chunkSize
	b.setBuf = make(chan *insertValue, setBufSize)
	b.stopWriting = make(chan *struct{})
	b.flushBuffer = make([]flushStruct, maxBatch)
	go func() {
		b.randomDelay(flushInterval)
		t := time.Tick(time.Duration(flushInterval) * time.Millisecond)
		var firstTimeTimestamp int64
		writeBufSize := uint64(len(b.writeCache))
		index := 0
		for {
			select {
			case i := <-b.setBuf:
				h := xxhash.Sum64(i.K)
				bufValue := b.writeCache[h%writeBufSize].Load().(*bufferItem)
				duplicated := false
				droppedWriting := false
				for j := range bufValue.hash {
					if bufValue.hash[j] == h {
						duplicated = true
					}
				}
				if !duplicated {
					lenBuf := kvLenBuf(i.K, i.V)
					currentIndex := len(bufValue.data)
					if currentIndex+len(lenBuf)+len(i.K)+len(i.V) > maxBufferValueSize {
						droppedWriting = true
					}
					if !droppedWriting {
						bufValue.doLockedEdit(func(item *bufferItem) {
							item.data = append(item.data, lenBuf[:]...)
							item.data = append(item.data, i.K...)
							item.data = append(item.data, i.V...)
							item.hash = append(item.hash, h)
							item.index = append(item.index, uint64(currentIndex))
						})
						b.writeCache[h%writeBufSize].Store(bufValue)
						index++
						atomic.AddUint64(&b.writeBufferSize, 1)
						if firstTimeTimestamp == 0 {
							firstTimeTimestamp = time.Now().UnixMilli()
						}
					}

				}
				if droppedWriting {
					atomic.AddUint64(&b.duplicatedCount, 1)
				}

				if droppedWriting {
					atomic.AddUint64(&b.droppedWrites, 1)
				}

				if index >= maxBatch || (index > 0 && time.Since(time.UnixMilli(firstTimeTimestamp)).Milliseconds() >= flushInterval) {
					b.flushToCache(index)
					atomic.StoreUint64(&b.writeBufferSize, 0)
					b.resetWriteBuffer(writeBufSize)
					firstTimeTimestamp = 0
					index = 0
				}
			case _ = <-t:
				if index > 0 && time.Since(time.UnixMilli(firstTimeTimestamp)).Milliseconds() >= flushInterval {
					b.flushToCache(index)
					atomic.StoreUint64(&b.writeBufferSize, 0)
					b.resetWriteBuffer(writeBufSize)
					firstTimeTimestamp = 0
					index = 0
				}
			case <-b.stopWriting:
				return
			}
		}
	}()
}

func (b *bucket) resetWriteBuffer(writeBufSize uint64) {
	for i := uint64(0); i < writeBufSize; i++ {
		bufItem := b.writeCache[i].Load().(*bufferItem)
		bufItem.doLockedEdit(func(item *bufferItem) {
			item.data = item.data[:0]
			item.index = item.index[:0]
			item.hash = item.hash[:0]
		})
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
		chunks[i] = nil
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
		chunk = getChunk()
		chunk = chunk[:0]
	}

	chunk = append(chunk, kv...)

	chunks[chunkIdx] = chunk
	b.m[h] = idx | (b.gen << bucketSizeBits)

	return needClean, idxNew, chunkIdx
}

func (b *bucket) Set(k, v []byte, h uint64, sync bool) {
	if sync {
		b.setWithLock(k, v, h)
	} else {
		setBuf := &insertValue{
			K: k,
			V: v,
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

func (b *bucket) setWithLock(k, v []byte, h uint64) {
	atomic.AddUint64(&b.setCalls, 1)
	if len(k) >= (1<<16) || len(v) >= (1<<16) {
		// Too big key or value - its length cannot be encoded
		// with 2 bytes (see below). Skip the entry.
		return
	}
	kvLenBuf := kvLenBuf(k, v)
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

func kvLenBuf(k []byte, v []byte) [4]byte {
	var kvLenBuf [4]byte
	kvLenBuf[0] = byte(uint16(len(k)) >> 8)
	kvLenBuf[1] = byte(len(k))
	kvLenBuf[2] = byte(uint16(len(v)) >> 8)
	kvLenBuf[3] = byte(len(v))
	return kvLenBuf
}

func (b *bucket) flushToCache(itemCount int) {
	atomic.AddUint64(&b.batchSetCalls, 1)
	atomic.AddUint64(&b.setCalls, uint64(itemCount))
	writeBufSize := len(b.writeCache)

	needClean := false
	idx := b.idx.Load()
	var idxNew uint64
	gen := b.gen
	chunkIdx := idx / chunkSize
	chunks := b.chunks
	writeBufIndex := 0
	for i := 0; i < writeBufSize; i++ {
		bufItem := b.writeCache[i].Load().(*bufferItem)
		currentChunkIndex := uint64(0)
		for j := range bufItem.index {
			var kvEnd uint64
			if j < len(bufItem.index)-1 {
				kvEnd = bufItem.index[j+1]
			} else {
				kvEnd = uint64(len(bufItem.data))
			}
			kv := bufItem.data[currentChunkIndex:kvEnd]
			b.flushBuffer[writeBufIndex].kv = kv
			b.flushBuffer[writeBufIndex].h = bufItem.hash[j]
			idxNew = idx + uint64(len(kv))
			chunkIdxNew := idxNew / chunkSize
			if chunkIdxNew > chunkIdx {
				if chunkIdxNew >= uint64(len(chunks)) {
					idx = 0
					idxNew = uint64(len(kv))
					chunkIdx = 0
					gen++
					if gen&((1<<genSizeBits)-1) == 0 {
						gen++
					}
					needClean = true
				} else {
					idx = chunkIdxNew * chunkSize
					idxNew = idx + uint64(len(kv))
					chunkIdx = chunkIdxNew
				}
				b.flushBuffer[writeBufIndex].cleanChunk = true
			} else {
				b.flushBuffer[writeBufIndex].cleanChunk = false
			}
			b.flushBuffer[writeBufIndex].idx = idx
			b.flushBuffer[writeBufIndex].gen = gen
			b.flushBuffer[writeBufIndex].chunk = chunkIdx
			writeBufIndex++
			currentChunkIndex = kvEnd
			idx = idxNew
			chunkIdx = chunkIdxNew
		}
	}

	b.setBatchInternal(b.flushBuffer[:itemCount], needClean, idxNew, gen)
}

func (b *bucket) setBatchInternal(flushBuffer []flushStruct, needClean bool, idxNew uint64, genNew uint64) {
	b.mu.Lock()
	for i := 0; i < len(flushBuffer); i++ {
		flushBuf := flushBuffer[i]
		chunk := b.chunks[flushBuf.chunk]
		if flushBuf.cleanChunk {
			chunk = chunk[:0]
		}

		if chunk == nil {
			chunk = getChunk()
			chunk = chunk[:0]
		}

		b.chunks[flushBuf.chunk] = append(chunk, flushBuf.kv...)
		b.m[flushBuf.h] = flushBuf.idx | (flushBuf.gen << bucketSizeBits)
	}
	b.idx.Store(idxNew)
	b.gen = genNew
	if needClean {
		b.cleanLocked(idxNew)
	}
	b.mu.Unlock()
}

func (b *bucket) Get(dst, k []byte, h uint64, returnDst bool) ([]byte, bool) {
	atomic.AddUint64(&b.getCalls, 1)
	found := false

	bufItem := b.writeCache[h%uint64(len(b.writeCache))].Load().(*bufferItem)
	if len(bufItem.hash) > 0 {
		if bufItem.doLocked(func(buffer *bufferItem) {
			for i := range buffer.hash {
				if bufItem.hash[i] == h {
					startKv := bufItem.index[i]
					var kvEnd uint64
					if i < len(bufItem.index)-1 {
						kvEnd = bufItem.index[i+1]

					} else {
						kvEnd = uint64(len(bufItem.data))
					}
					kv := buffer.data
					kvLenBuf := kv[startKv : startKv+4]
					keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
					if keyLen == uint64(len(k)) && string(k) == string(kv[startKv+4:startKv+4+keyLen]) {
						if returnDst {
							dst = append(dst, kv[startKv+4+keyLen:kvEnd]...)
							found = true
						}
					}
				}
			}

		}) && found {
			return dst, true
		}
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
			if idx+4 >= chunkSize {
				//removed stats for corruption
				goto end
			}
			kvLenBuf := chunk[idx : idx+4]
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
			idx += 4
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
	return dst, found
}

func (b *bucket) Del(h uint64) {
	b.mu.Lock()
	delete(b.m, h)
	b.mu.Unlock()
}

type insertValue struct {
	K, V []byte
}

type Config struct {
	maxBytes                  int
	flushIntervalMillis       int64
	maxWriteBatch             int
	syncWrite                 bool
	dropWriteOnHighContention bool
	concurrentWriteLimit      int
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

func NewConfigWithDroppingOnContention(maxBytes int, flushInterval int64, maxWriteBatch int, writeConcurrentLimit int) *Config {
	return &Config{
		maxBytes:                  maxBytes,
		flushIntervalMillis:       flushInterval,
		maxWriteBatch:             maxWriteBatch,
		dropWriteOnHighContention: true,
		concurrentWriteLimit:      writeConcurrentLimit,
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
