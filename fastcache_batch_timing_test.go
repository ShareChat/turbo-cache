package turbocache

import (
	"github.com/cespare/xxhash/v2"
	"testing"
)

func BenchmarkBatchSet(b *testing.B) {
	const items = 1 << 12
	const buffSize = 1024
	c := New(newCacheConfigWithDefaultParams(12000 * items))
	defer c.Reset()
	k := []byte("\x00\x00\x00\x00")
	v := []byte("xyza")

	buffer := make([]flushStruct, buffSize)
	for i := 0; i < buffSize; i++ {
		kvLenBuf := kvLenBuf(k, v)
		buffer[i].kv = append(buffer[i].kv, kvLenBuf[:]...)
		buffer[i].kv = append(buffer[i].kv, k...)
		buffer[i].kv = append(buffer[i].kv, v...)
		buffer[i].h = xxhash.Sum64(k)
		buffer[i].chunk = uint64(i % len(c.buckets[0].chunks))
	}
	b.ReportAllocs()
	//b.SetBytes(items)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < items; i++ {
				c.buckets[i%bucketsCount].setBatchInternal(buffer, true, 1, 1)

				buffer[i%buffSize].kv[i%(len(buffer[i%buffSize].kv)-4)+4]++
			}
		}
	})
}

func BenchmarkSingleSet(b *testing.B) {
	const items = 1 << 12
	const buffSize = 1024
	c := New(newCacheConfigWithDefaultParams(12 * items))
	defer c.Reset()
	k := []byte("\x00\x00\x00\x00")
	v := []byte("xyza")

	buffer := make([]flushStruct, buffSize)
	for i := 0; i < buffSize; i++ {
		kvLenBuf := kvLenBuf(k, v)
		buffer[i].kv = append(buffer[i].kv, kvLenBuf[:]...)
		buffer[i].kv = append(buffer[i].kv, k...)
		buffer[i].kv = append(buffer[i].kv, v...)
		buffer[i].h = xxhash.Sum64(k)
	}
	b.ReportAllocs()
	//b.SetBytes(items)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < items; i++ {

				for i := 0; i < len(buffer); i++ {
					c.buckets[i%bucketsCount].SetSingeSampleImpl(k, v, xxhash.Sum64(k))
				}
				k[i%len(k)]++
			}
		}
	})
}

func (b *bucket) SetSingeSampleImpl(k, v []byte, h uint64) {

	if len(k) >= (1<<16) || len(v) >= (1<<16) {
		// Too big key or value - its length cannot be encoded
		// with 2 bytes (see below). Skip the entry.
		return
	}
	var kvLenBuf [4]byte
	kvLenBuf[0] = byte(uint16(len(k)) >> 8)
	kvLenBuf[1] = byte(len(k))
	kvLenBuf[2] = byte(uint16(len(v)) >> 8)
	kvLenBuf[3] = byte(len(v))
	kvLen := uint64(len(kvLenBuf) + len(k) + len(v))
	if kvLen >= chunkSize {
		// Do not store too big keys and values, since they do not
		// fit a chunk.
		return
	}

	b.mu.Lock()
	idx := b.idx.Load()
	idxNew := idx + kvLen
	chunkIdx := idx / chunkSize
	chunkIdxNew := idxNew / chunkSize
	if chunkIdxNew > chunkIdx {
		if chunkIdxNew >= uint64(len(b.chunks)) {
			idx = 0
			idxNew = kvLen
			chunkIdx = 0
			b.gen++
			if b.gen&((1<<genSizeBits)-1) == 0 {
				b.gen++
			}
		} else {
			idx = chunkIdxNew * chunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}
		b.chunks[chunkIdx] = b.chunks[chunkIdx][:0]
	}
	chunk := b.chunks[chunkIdx]
	if chunk == nil {
		chunk = getChunk()
		chunk = chunk[:0]
	}
	chunk = append(chunk, kvLenBuf[:]...)
	chunk = append(chunk, k...)
	chunk = append(chunk, v...)
	b.chunks[chunkIdx] = chunk
	b.m[h] = idx | (b.gen << bucketSizeBits)
	b.idx.Store(idxNew)
	b.mu.Unlock()
}
