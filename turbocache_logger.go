// Package turbocache implements fast in-memory cache.
package turbocache

import (
	"github.com/ShareChat/turbo-cache/internal/spinlock"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type aheadLogger struct {
	setBuf          chan *queuedStruct
	count           int
	chunks          []flushChunk
	idx             uint64
	gen             uint64
	chunkCount      int32
	needClean       bool
	index           []flushChunkIndexItem
	currentChunkId  uint64
	totalChunkCount uint64
	spinlock        spinlock.RWMutex
	latestTimestamp int64
	writeBufferSize uint64
	dropsInQueue    uint64
}

type flushChunk struct {
	chunkId    uint64
	chunk      [chunkSize]byte
	h          []uint64
	idx        []uint64
	gen        []uint64
	size       uint64
	cleanChunk bool
}

func (l *aheadLogger) log(k, v []byte, h uint64) {
	iv := getQueuedStruct(k, v, h)

	select {
	case l.setBuf <- iv:
		return
	default:
		atomic.AddUint64(&l.dropsInQueue, 1)
	}
}

func (b *bucket) onFlushTick(flushInterval int64) {
	f := b.logger
	if f.count > 0 && time.Since(time.UnixMilli(f.latestTimestamp)).Milliseconds() >= flushInterval {
		b.setBatch(f.chunks[:f.chunkCount+1], f.idx, f.gen, f.needClean, f.count)
		f.clean()
	}
}

func (b *bucket) onNewItem(i *queuedStruct, maxBatch int, flushInterval int64) {
	defer releaseQueuedStruct(i)

	f := b.logger

	index := f.index
	indexId := i.h % uint64(len(index))
	duplicated := index[indexId].exists(i.h)
	forceFlush := false
	if !duplicated {
		kvLength := uint64(4) + uint64(len(i.K)) + uint64(len(i.V))
		idxNew, newChunk := f.incrementIndexes(kvLength)
		if newChunk {
			f.chunkCount++
		}
		if f.chunkCount <= int32(len(f.chunks)) {
			flushChunk := &f.chunks[f.chunkCount]

			lenBuf := makeKvLenBuf(i.K, i.V)

			copy(flushChunk.chunk[flushChunk.size:], lenBuf[:])
			copy(flushChunk.chunk[flushChunk.size+4:], i.K)
			copy(flushChunk.chunk[flushChunk.size+4+uint64(len(i.K)):], i.V)

			flushChunk.h = append(flushChunk.h, i.h)
			flushChunk.idx = append(flushChunk.idx, f.idx)
			flushChunk.gen = append(flushChunk.gen, f.gen)
			flushChunk.chunkId = f.currentChunkId
			flushChunk.cleanChunk = newChunk

			for j := range index[indexId].h {
				if atomic.LoadUint64(&index[indexId].h[j]) == 0 {
					atomic.StoreUint64(&index[indexId].currentIdx[j], flushChunk.size)
					atomic.StoreInt32(&index[indexId].flushChunk[j], f.chunkCount)
					atomic.StoreUint64(&index[indexId].h[j], i.h)
					break
				}
			}

			flushChunk.size += kvLength
			f.idx = idxNew
			f.count++
			if b.logger.latestTimestamp == 0 {
				b.logger.latestTimestamp = time.Now().UnixMilli()
			}
			atomic.AddUint64(&b.logger.writeBufferSize, 1)
		} else {
			atomic.AddUint64(&b.droppedWrites, 1)
			forceFlush = true
		}

	} else {
		atomic.AddUint64(&b.duplicatedCount, 1)
	}

	if f.count >= maxBatch || (f.count > 0 && (time.Since(time.UnixMilli(b.logger.latestTimestamp)).Milliseconds() >= flushInterval || forceFlush)) {
		b.setBatch(f.chunks[:f.chunkCount+1], f.idx, f.gen, f.needClean, f.count)
		f.clean()
	}
}

func (l *aheadLogger) clean() {
	index := l.index
	l.spinlock.Lock()

	for i := 0; i < len(index); i++ {
		for j := range index[i].h {
			if index[i].h[j] != 0 {
				index[i].currentIdx[j] = 0
				index[i].flushChunk[j] = 0
				index[i].h[j] = 0
			} else {
				break
			}
		}
	}
	for j := range l.chunks {
		l.chunks[j].clean()
	}
	l.spinlock.Unlock()

	l.chunkCount = 0
	l.needClean = false
	l.count = 0
	l.latestTimestamp = 0
	atomic.StoreUint64(&l.writeBufferSize, 0)
}

func (b *flushChunk) clean() {
	b.h = b.h[:0]
	b.idx = b.idx[:0]
	b.size = 0
	b.cleanChunk = false
	b.gen = b.gen[:0]
}

func randomDelay(maxDelayMillis int64) {
	jitterDelay := rand.Int63() % (maxDelayMillis * 1000)
	time.Sleep(time.Duration(jitterDelay) * time.Microsecond)
}

func (l *aheadLogger) incrementIndexes(kvLength uint64) (idxNew uint64, newChunk bool) {
	idxNew = l.idx + kvLength
	chunkIdxNew := idxNew / chunkSize
	if chunkIdxNew > l.currentChunkId {
		if chunkIdxNew >= l.totalChunkCount {
			l.idx = 0
			idxNew = kvLength
			l.currentChunkId = 0
			l.gen++
			if l.gen&((1<<genSizeBits)-1) == 0 {
				l.gen++
			}
			l.needClean = true
		} else {
			l.idx = chunkIdxNew * chunkSize
			idxNew = l.idx + kvLength
			l.currentChunkId = chunkIdxNew
		}
		newChunk = true
	}
	return idxNew, newChunk
}

func makeKvLenBuf(k []byte, v []byte) [4]byte {
	var kvLenBuf [4]byte
	kvLenBuf[0] = byte(uint16(len(k)) >> 8)
	kvLenBuf[1] = byte(len(k))
	kvLenBuf[2] = byte(uint16(len(v)) >> 8)
	kvLenBuf[3] = byte(len(v))
	return kvLenBuf
}

var insertValuePool = &sync.Pool{New: func() any {
	return &queuedStruct{}
}}

func getQueuedStruct(k, v []byte, h uint64) *queuedStruct {
	result := insertValuePool.Get().(*queuedStruct)

	result.K = k
	result.V = v
	result.h = h

	return result
}

func releaseQueuedStruct(i *queuedStruct) {
	i.K = nil
	i.V = nil
	i.h = 0

	insertValuePool.Put(i)
}

type queuedStruct struct {
	K, V []byte
	h    uint64
}
