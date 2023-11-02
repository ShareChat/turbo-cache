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
	setBatch(chunks []flushChunk, idx uint64, gen uint64, needClean bool, keyCount int)
}

func (b *flushChunk) clean() {
	b.h = b.h[:0]
	b.idx = b.idx[:0]
	b.chunkSize = 0
	b.cleanChunk = false
	b.gen = b.gen[:0]
}

func (b *bucket) setBatch(chunks []flushChunk, idx uint64, gen uint64, needClean bool, keyCount int) {
	atomic.AddUint64(&b.setCalls, uint64(keyCount))
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
	b.idx = idx
	b.gen = gen
	if needClean {
		b.cleanLocked(idx)
	}
}
