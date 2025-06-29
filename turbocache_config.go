// Package turbocache implements fast in-memory cache.
package turbocache

type Config struct {
	//max bytes for storing keys in chunks
	maxBytes int
	// flush intervals to writing keys to the chunks
	flushIntervalMillis int64
	//max batch size for writing in chunks. batch size 1 make turbo cache to sync cache
	maxWriteBatch int
	//count of the accumulating buffers (chunks) before flush. Every flush chunks has 64KB. min 2
	flushChunkCount int
}

func NewConfig(maxBytes int, flushInterval int64, maxWriteBatch int, flushChunks int) *Config {
	return &Config{
		maxBytes:            maxBytes,
		flushIntervalMillis: flushInterval,
		maxWriteBatch:       maxWriteBatch,
		flushChunkCount:     flushChunks,
	}
}
