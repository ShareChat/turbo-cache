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

const setBufSize = 64
const defaultMaxWriteSizeBatch = 250
const defaultFlushIntervalMillis = 5
const initItemsPerFlushChunk = 128

type cacheLogger struct {
	spinlock               spinlock.RWMutex
	writer                 cacheWriter
	setQueue               chan *queuedStruct
	count                  int
	chunks                 []flushChunk
	index                  []flushChunkIndexItem
	idx                    uint64
	gen                    uint64
	currentFlunkChunkIndex int32
	needClean              bool
	currentChunkId         uint64
	totalChunkCount        uint64
	oldestKeyTimestamp     int64
	stats                  *loggerStats
	flushInterval          int64
	maxBatch               int
}

type loggerStats struct {
	dropsInQueue    uint64
	duplicatedCount uint64
}

type writeAheadLogger interface {
	log(k, v []byte, h uint64)
	lookup(dst []byte, k []byte, h uint64, returnDst bool) ([]byte, bool)
	getStats() *loggerStats
	stopFlushing()
}

func newLogger(cacheWriter cacheWriter, maxBatch int, flushChunkCount int, idx uint64, gen uint64, chunks uint64, interval int64) writeAheadLogger {
	result := &cacheLogger{
		count:                  0,
		idx:                    idx,
		gen:                    gen,
		currentChunkId:         idx / chunkSize,
		currentFlunkChunkIndex: 0,
		totalChunkCount:        chunks,
		needClean:              false,
		setQueue:               make(chan *queuedStruct, setBufSize),
		chunks:                 make([]flushChunk, flushChunkCount),
		writer:                 cacheWriter,
		stats:                  &loggerStats{},
		flushInterval:          interval,
		maxBatch:               maxBatch,
	}

	itemsPerChunk := maxBatch
	if initItemsPerFlushChunk < itemsPerChunk {
		itemsPerChunk = initItemsPerFlushChunk
	}

	for i := 0; i < flushChunkCount; i++ {
		array := getChunkArray()
		result.chunks[i] = flushChunk{
			cacheChunkId:    0,
			chunk:           *array,
			h:               make([]uint64, 0, itemsPerChunk),
			cacheIdx:        make([]uint64, 0, itemsPerChunk),
			cacheGen:        make([]uint64, 0, itemsPerChunk),
			chunkSize:       0,
			cleanCacheChunk: false,
		}
	}
	result.index = make([]flushChunkIndexItem, primeNumber.NextPrime(uint64(maxBatch)))

	go result.startProcessingWriteQueue()

	return result
}

func (l *cacheLogger) startProcessingWriteQueue() {
	randomDelay()
	t := time.Tick(time.Duration(l.flushInterval) * time.Millisecond)
	for {
		select {
		case i := <-l.setQueue:
			if i == nil {
				return
			}
			k, v := i.K, i.V
			h := i.h
			releaseQueuedStruct(i)
			l.onNewItem(k, v, h)
		case _ = <-t:
			l.onFlushTick()
		}
	}
}

func (l *cacheLogger) log(k, v []byte, h uint64) {
	select {
	case l.setQueue <- getQueuedStruct(k, v, h):
		return
	default:
		atomic.AddUint64(&l.stats.dropsInQueue, 1)
	}
}

func (l *cacheLogger) onFlushTick() {
	if l.flushTime() {
		l.flush()
	}
}

func (l *cacheLogger) flushTime() bool {
	return l.count > 0 && time.Since(time.UnixMilli(l.oldestKeyTimestamp)).Milliseconds() >= l.flushInterval
}

func (l *cacheLogger) onNewItem(k, v []byte, h uint64) {
	index := l.index
	indexItem := &index[h%uint64(len(index))]
	if !indexItem.exists(h) {
		kvLength := kvLenBufSize + uint64(len(k)) + uint64(len(v))
		idxNew, newChunk := l.incrementIndexes(kvLength)
		if newChunk {
			if l.currentFlunkChunkIndex+1 >= int32(len(l.chunks)) {
				l.flush()
			} else {
				l.currentFlunkChunkIndex++
			}
		}
		flushChunk := &l.chunks[l.currentFlunkChunkIndex]

		flushChunk.write(h, k, v)

		flushChunk.cacheIdx = append(flushChunk.cacheIdx, l.idx)
		flushChunk.cacheGen = append(flushChunk.cacheGen, l.gen)
		flushChunk.cacheChunkId = l.currentChunkId
		flushChunk.cleanCacheChunk = newChunk

		indexItem.save(h, l.currentFlunkChunkIndex, flushChunk.chunkSize)

		flushChunk.chunkSize += kvLength
		l.idx = idxNew
		l.count++
		if l.oldestKeyTimestamp == 0 {
			l.oldestKeyTimestamp = time.Now().UnixMilli()
		}
	} else {
		atomic.AddUint64(&l.stats.duplicatedCount, 1)
	}

	if l.count >= l.maxBatch || l.flushTime() {
		l.flush()
	}
}

func (l *cacheLogger) flush() {
	l.writer.setBatch(l.chunks[:l.currentFlunkChunkIndex+1], l.idx, l.gen, l.needClean, l.count)
	l.clean()
}

func (l *cacheLogger) clean() {
	index := l.index
	l.spinlock.Lock()

	for i := 0; i < len(index); i++ {
		for j := range index[i].h {
			if index[i].h[j] != 0 {
				index[i].currentIdx[j] = 0
				index[i].flushChunk[j] = 0
				atomic.StoreUint64(&index[i].h[j], 0)
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
	l.oldestKeyTimestamp = 0
}

func randomDelay() {
	jitterDelay := rand.Int63() % (5 * 1000)
	time.Sleep(time.Duration(jitterDelay) * time.Microsecond)
}

func (l *cacheLogger) incrementIndexes(kvLength uint64) (idxNew uint64, newChunk bool) {
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

func makeKvLenBuf(k []byte, v []byte) [kvLenBufSize]byte {
	var kvLenBuf [kvLenBufSize]byte
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

func (l *cacheLogger) getStats() *loggerStats {
	return l.stats
}

func (l *cacheLogger) stopFlushing() {
	if l.setQueue != nil {
		close(l.setQueue)
	}
}

type queuedStruct struct {
	K, V []byte
	h    uint64
}
