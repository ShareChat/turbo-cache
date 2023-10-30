// Package turbocache implements fast in-memory cache.
package turbocache

import "sync/atomic"

type flushChunkIndexItem struct {
	//flush chunk number
	flushChunk [7]int32
	// key hash value
	h [7]uint64
	//index in flush chunk
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

func (f *flusher) tryFindInFlushIndex(dst []byte, k []byte, h uint64, returnDst bool) ([]byte, bool) {
	if f == nil {
		return dst, false
	}

	found := false
	if !f.flushing.Load() {
		index := f.index
		indexPoint := h % uint64(len(index))
		for i := 0; i < len(index[indexPoint].h); i++ {
			hashValue := atomic.LoadUint64(&index[indexPoint].h[i])
			if hashValue == 0 {
				break
			} else if hashValue == h {
				if f.spinlock.TryRLock() {
					index = f.index
					if atomic.LoadUint64(&index[indexPoint].h[i]) == h {
						chunkId := atomic.LoadInt32(&index[indexPoint].flushChunk[i])
						flushIdx := atomic.LoadUint64(&index[indexPoint].currentIdx[i])
						chunks := f.chunkSynced.Load().([]flushChunk)
						kvLenBuf := chunks[chunkId].chunk[flushIdx : flushIdx+kvLenBufSize]
						keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
						if keyLen == uint64(len(k)) && string(k) == string(chunks[chunkId].chunk[flushIdx+kvLenBufSize:flushIdx+kvLenBufSize+keyLen]) {
							if returnDst {
								valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
								dst = append(dst, chunks[chunkId].chunk[flushIdx+kvLenBufSize+keyLen:flushIdx+kvLenBufSize+keyLen+valLen]...)
								found = true
							}
						}
					}
					f.spinlock.RUnlock()
				}
			}
		}
		if found {
			return dst, found
		}
	}
	return dst, false
}
