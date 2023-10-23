// Package fastcache implements fast in-memory cache.
//
// The package has been extracted from https://victoriametrics.com/
package turbocache

type Config struct {
	//max bytes for storing keys in chunks
	maxBytes int
	// flush intervals to writing keys to the chunks
	flushIntervalMillis int64
	//max batch size for writing in chunks. batch size 1 make turbo cache to sync cache
	maxWriteBatch int
	//size of the accumaling
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
