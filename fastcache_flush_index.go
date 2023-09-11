// Package fastcache implements fast in-memory cache.
//
// The package has been extracted from https://victoriametrics.com/
package turbocache

type flushChunkIndexItem struct {
	flushChunk [7]int
	h          [7]uint64
	currentIdx [7]uint64
}

func (i *flushChunkIndexItem) saveToIndex(h uint64, flushIdx uint64, chunk int) {
	for j := range i.h {
		if i.h[j] == 0 {
			i.h[j] = h
			i.currentIdx[j] = flushIdx
			i.flushChunk[j] = chunk
			break
		}
	}
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
	if !f.flushed.Load() {
		index := f.index.Load().([]flushChunkIndexItem)
		indexItem := (index)[h%uint64(len(index))]
		for i := range indexItem.h {
			if indexItem.h[i] == 0 {
				break
			} else if indexItem.h[i] == h {
				if f.spinlock.TryRLock() {
					index = f.index.Load().([]flushChunkIndexItem)
					indexItem = (index)[h%uint64(len(index))]
					if indexItem.h[i] == h {
						chunkId := indexItem.flushChunk[i]
						flushIdx := indexItem.currentIdx[i]
						chunk := f.chunkSynced.Load().([]flushChunk)[chunkId]
						kvLenBuf := chunk.chunk[flushIdx : flushIdx+kvLenBufSize]
						keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
						if keyLen == uint64(len(k)) && string(k) == string(chunk.chunk[flushIdx+kvLenBufSize:flushIdx+kvLenBufSize+keyLen]) {
							if returnDst {
								valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
								dst = append(dst, chunk.chunk[flushIdx+kvLenBufSize+keyLen:flushIdx+kvLenBufSize+keyLen+valLen]...)
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
