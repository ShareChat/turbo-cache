//go:build appengine || windows
// +build appengine windows

package turbocache

func getChunk() []byte {
	return make([]byte, chunkSize)
}

func putChunk(chunk []byte) {
	// No-op.
}
