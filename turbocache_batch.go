package turbocache

import "sync/atomic"

// flush chunk for batch set
type flushChunk struct {
	chunkId    uint64
	chunk      [chunkSize]byte
	h          []uint64
	idx        []uint64
	gen        []uint64
	chunkSize  uint64
	cleanChunk bool
}

type cacheWriter interface {
	setBatch(chunks []flushChunk, newIdx uint64, newGen uint64, needClean bool, batchSize int)
}

func (b *flushChunk) clean() {
	b.h = b.h[:0]
	b.idx = b.idx[:0]
	b.chunkSize = 0
	b.cleanChunk = false
	b.gen = b.gen[:0]
}

func (b *flushChunk) write(k, v []byte) {
	lenBuf := makeKvLenBuf(k, v)

	copy(b.chunk[b.chunkSize:], lenBuf[:])
	copy(b.chunk[b.chunkSize+kvLenBufSize:], k)
	copy(b.chunk[b.chunkSize+kvLenBufSize+uint64(len(k)):], v)
}

func (b *bucket) setBatch(chunks []flushChunk, newIdx uint64, newGen uint64, needClean bool, batchSize int) {
	atomic.AddUint64(&b.setCalls, uint64(batchSize))
	atomic.AddUint64(&b.batchSetCalls, 1)
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := 0; i < len(chunks); i++ {
		f := chunks[i]
		chunk := b.chunks[f.chunkId]
		if chunk == nil {
			chunk = getChunk()[:0]
		} else if f.cleanChunk {
			chunk = chunk[:0]
		}

		b.chunks[f.chunkId] = append(chunk, f.chunk[:f.chunkSize]...)

		for j := 0; j < len(f.h); j++ {
			b.m[f.h[j]] = f.idx[j] | (f.gen[j] << bucketSizeBits)
		}
	}
	b.idx = newIdx
	b.gen = newGen
	if needClean {
		b.cleanLocked(newIdx)
	}
}
