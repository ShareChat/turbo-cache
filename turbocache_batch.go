package turbocache

import "sync/atomic"

// flush chunk for batch set
type flushChunk struct {
	cacheChunkId    uint64
	chunk           [chunkSize]byte
	h               []uint64
	cacheIdx        []uint64
	cacheGen        []uint64
	chunkSize       uint64
	cleanCacheChunk bool
}

type cacheWriter interface {
	setBatch(chunks []flushChunk, newIdx uint64, newGen uint64, needClean bool, batchSize int)
}

func (ch *flushChunk) clean() {
	ch.h = ch.h[:0]
	ch.cacheIdx = ch.cacheIdx[:0]
	ch.chunkSize = 0
	ch.cleanCacheChunk = false
	ch.cacheGen = ch.cacheGen[:0]
}

func (ch *flushChunk) write(h uint64, k, v []byte) {
	ch.h = append(ch.h, h)

	lenBuf := makeKvLenBuf(k, v)
	copy(ch.chunk[ch.chunkSize:], lenBuf[:])
	copy(ch.chunk[ch.chunkSize+kvLenBufSize:], k)
	copy(ch.chunk[ch.chunkSize+kvLenBufSize+uint64(len(k)):], v)
}

func (b *bucket) setBatch(chunks []flushChunk, newIdx uint64, newGen uint64, needClean bool, batchSize int) {
	atomic.AddUint64(&b.setCalls, uint64(batchSize))
	atomic.AddUint64(&b.batchSetCalls, 1)
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := 0; i < len(chunks); i++ {
		f := chunks[i]
		chunk := b.chunks[f.cacheChunkId]
		if chunk == nil {
			chunk = getChunk()[:0]
		} else if f.cleanCacheChunk {
			chunk = chunk[:0]
		}

		b.chunks[f.cacheChunkId] = append(chunk, f.chunk[:f.chunkSize]...)

		for j := 0; j < len(f.h); j++ {
			b.m[f.h[j]] = f.cacheIdx[j] | (f.cacheGen[j] << bucketSizeBits)
		}
	}
	b.idx = newIdx
	b.gen = newGen
	if needClean {
		b.cleanLocked(newIdx)
	}
}
