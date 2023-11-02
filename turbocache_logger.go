// Package turbocache implements fast in-memory cache.
package turbocache

import (
	"github.com/ShareChat/turbo-cache/internal/primeNumber"
	"github.com/ShareChat/turbo-cache/internal/spinlock"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type aheadLogger struct {
	writer                 cacheWriter
	setBuf                 chan *queuedStruct
	count                  int
	chunks                 []flushChunk
	index                  []flushChunkIndexItem
	idx                    uint64
	gen                    uint64
	currentFlunkChunkIndex int32
	needClean              bool
	currentChunkId         uint64
	totalChunkCount        uint64
	spinlock               spinlock.RWMutex
	latestTimestamp        int64
	stats                  *loggerStats
}

type loggerStats struct {
	writeBufferSize uint64
	dropsInQueue    uint64
	droppedWrites   uint64
	duplicatedCount uint64
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

func newLogger(bucket *bucket, maxBatch int, flushChunkCount int, idx uint64, gen uint64, chunks uint64, interval int64) writeAheadLogger {
	result := &aheadLogger{
		count:                  0,
		idx:                    idx,
		gen:                    gen,
		currentChunkId:         idx / chunkSize,
		currentFlunkChunkIndex: 0,
		totalChunkCount:        chunks,
		needClean:              false,
		setBuf:                 make(chan *queuedStruct, setBufSize),
		chunks:                 make([]flushChunk, flushChunkCount),
		writer:                 bucket,
		stats:                  &loggerStats{},
	}

	itemsPerChunk := maxBatch
	if initItemsPerFlushChunk < itemsPerChunk {
		itemsPerChunk = initItemsPerFlushChunk
	}

	for i := 0; i < flushChunkCount; i++ {
		array := getChunkArray()
		result.chunks[i] = flushChunk{
			chunkId:    0,
			chunk:      *array,
			h:          make([]uint64, 0, itemsPerChunk),
			idx:        make([]uint64, 0, itemsPerChunk),
			gen:        make([]uint64, 0, itemsPerChunk),
			size:       0,
			cleanChunk: false,
		}
	}
	result.index = make([]flushChunkIndexItem, primeNumber.NextPrime(uint64(maxBatch)))

	go result.startProcessingWriteQueue(interval, maxBatch)

	return result
}

func (l *aheadLogger) startProcessingWriteQueue(interval int64, maxBatch int) {
	maxDelay := interval
	if maxDelay > 5 {
		maxDelay = 5
	}
	randomDelay(maxDelay)
	t := time.Tick(time.Duration(interval) * time.Millisecond)
	for {
		select {
		case i := <-l.setBuf:
			if i == nil {
				return
			}
			k, v := i.K, i.V
			h := i.h
			releaseQueuedStruct(i)
			l.onNewItem(k, v, h, maxBatch, interval)
		case _ = <-t:
			l.onFlushTick(interval)
		}
	}
}

func (l *aheadLogger) log(k, v []byte, h uint64) {
	iv := getQueuedStruct(k, v, h)

	select {
	case l.setBuf <- iv:
		return
	default:
		atomic.AddUint64(&l.stats.dropsInQueue, 1)
	}
}

func (l *aheadLogger) onFlushTick(flushInterval int64) {
	if l.count > 0 && time.Since(time.UnixMilli(l.latestTimestamp)).Milliseconds() >= flushInterval {
		l.flush()
	}
}

func (l *aheadLogger) onNewItem(k, v []byte, h uint64, maxBatch int, flushInterval int64) {
	index := l.index
	indexId := h % uint64(len(index))
	duplicated := index[indexId].exists(h)
	if !duplicated {
		kvLength := uint64(4) + uint64(len(k)) + uint64(len(v))
		idxNew, newChunk := l.incrementIndexes(kvLength)
		if newChunk {
			if l.currentFlunkChunkIndex+1 >= int32(len(l.chunks)) {
				l.flush()
			} else {
				l.currentFlunkChunkIndex++
			}
		}
		flushChunk := &l.chunks[l.currentFlunkChunkIndex]

		lenBuf := makeKvLenBuf(k, v)

		copy(flushChunk.chunk[flushChunk.size:], lenBuf[:])
		copy(flushChunk.chunk[flushChunk.size+4:], k)
		copy(flushChunk.chunk[flushChunk.size+4+uint64(len(k)):], v)

		flushChunk.h = append(flushChunk.h, h)
		flushChunk.idx = append(flushChunk.idx, l.idx)
		flushChunk.gen = append(flushChunk.gen, l.gen)
		flushChunk.chunkId = l.currentChunkId
		flushChunk.cleanChunk = newChunk

		for j := range index[indexId].h {
			if atomic.LoadUint64(&index[indexId].h[j]) == 0 {
				atomic.StoreUint64(&index[indexId].currentIdx[j], flushChunk.size)
				atomic.StoreInt32(&index[indexId].flushChunk[j], l.currentFlunkChunkIndex)
				atomic.StoreUint64(&index[indexId].h[j], h)
				break
			}
		}

		flushChunk.size += kvLength
		l.idx = idxNew
		l.count++
		if l.latestTimestamp == 0 {
			l.latestTimestamp = time.Now().UnixMilli()
		}
		atomic.AddUint64(&l.stats.writeBufferSize, 1)
	} else {
		atomic.AddUint64(&l.stats.duplicatedCount, 1)
	}

	if l.count >= maxBatch || (l.count > 0 && (time.Since(time.UnixMilli(l.latestTimestamp)).Milliseconds() >= flushInterval)) {
		l.flush()
	}
}

func (l *aheadLogger) flush() {
	l.writer.setBatch(l.chunks[:l.currentFlunkChunkIndex+1], l.idx, l.gen, l.needClean, l.count)
	l.clean()
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

	l.currentFlunkChunkIndex = 0
	l.needClean = false
	l.count = 0
	l.latestTimestamp = 0
	atomic.StoreUint64(&l.stats.writeBufferSize, 0)
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

func (l *aheadLogger) getStats() *loggerStats {
	return l.stats
}

func (l *aheadLogger) stopFlushing() {
	if l.setBuf != nil {
		close(l.setBuf)
	}
}

type queuedStruct struct {
	K, V []byte
	h    uint64
}

type cacheWriter interface {
	setBatch(chunks []flushChunk, idx uint64, gen uint64, needClean bool, keyCount int)
}

type writeAheadLogger interface {
	log(k, v []byte, h uint64)
	lookup(dst []byte, k []byte, h uint64, returnDst bool) ([]byte, bool)
	getStats() *loggerStats
	stopFlushing()
}
