// Package fastcache implements fast in-memory cache.
//
// The package has been extracted from https://victoriametrics.com/
package turbocache

import (
	"github.com/ShareChat/turbo-cache/spinlock"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

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

type flushChunk struct {
	chunkId    uint64
	chunk      []byte
	h          []uint64
	idx        []uint64
	gen        []uint64
	size       uint64
	cleanChunk bool
}

func (b *bucket) onFlushTick(flushInterval int64) {
	if b.writeBufferSize > 0 && time.Since(time.UnixMilli(b.latestTimestamp)).Milliseconds() >= flushInterval {
		b.setBatch(b.flusher)
		b.cleanFlusher(b.flusher)
	}
}

func (b *bucket) onNewItem(i *insertValue, maxBatch int, flushInterval int64) {
	defer releasePooledInsertValue(i)

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
		b.setBatch(f)
		b.cleanFlusher(f)
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

func (b *bucket) setBatch(f *flusher) {
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

var insertValuePool = &sync.Pool{New: func() any {
	return &insertValue{}
}}

func getPooledInsertValue(k, v []byte, h uint64) *insertValue {
	result := insertValuePool.Get().(*insertValue)

	result.K = k
	result.V = v
	result.h = h

	return result
}

func releasePooledInsertValue(i *insertValue) {
	i.K = nil
	i.V = nil
	i.h = 0

	insertValuePool.Put(i)
}
