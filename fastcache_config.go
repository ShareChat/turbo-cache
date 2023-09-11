// Package fastcache implements fast in-memory cache.
//
// The package has been extracted from https://victoriametrics.com/
package turbocache

type Config struct {
	maxBytes                  int
	flushIntervalMillis       int64
	maxWriteBatch             int
	syncWrite                 bool
	dropWriteOnHighContention bool
}

func NewSyncWriteConfig(maxBytes int) *Config {
	return &Config{
		maxBytes:  maxBytes,
		syncWrite: true,
	}
}

func NewConfig(maxBytes int, flushInterval int64, maxWriteBatch int) *Config {
	return &Config{
		maxBytes:                  maxBytes,
		flushIntervalMillis:       flushInterval,
		maxWriteBatch:             maxWriteBatch,
		dropWriteOnHighContention: true,
	}
}

func NewConfigWithoutDropOnHighContention(maxBytes int, flushInterval int64, maxWriteBatch int) *Config {
	return &Config{
		maxBytes:                  maxBytes,
		flushIntervalMillis:       flushInterval,
		maxWriteBatch:             maxWriteBatch,
		dropWriteOnHighContention: false,
	}
}
