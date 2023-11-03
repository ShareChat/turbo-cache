package turbocache

import (
	"fmt"
	"github.com/ShareChat/turbo-cache/internal/benchmarking"
	"github.com/VictoriaMetrics/fastcache"
	"sync"
	"testing"
)

const defaultFlushInterval = 5000
const defaultBatchWriteSize = 50

func writeKeys(c *Cache, count int, k []byte, v []byte) {
	for i := 0; i < count; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		c.Set(k, v)
	}
}

func writeKeysFastCache(c *fastcache.Cache, count int, k []byte, v []byte) {
	for i := 0; i < count; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		c.Set(k, v)
	}
}

func BenchmarkCacheSet(b *testing.B) {
	const items = 1 << 20
	c := New(newCacheConfigBenchmarkParams(12 * items))
	defer c.Close()

	writeKeys(c, items, []byte("\x00\x00\x00\x00"), []byte("xyza"))
	b.ReportAllocs()
	b.SetBytes(items)
	b.ResetTimer()
	mutexMetricCollector := benchmarking.NewMutexMetricsCollector()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			writeKeys(c, items, []byte("\x00\x00\x00\x00"), []byte("xyza"))
		}
	})
	mutexMetricCollector.Report(b)
}

func BenchmarkFastCacheCacheSet(b *testing.B) {
	const items = 1 << 20
	c := fastcache.New(12 * items)
	defer c.Reset()
	writeKeysFastCache(c, items, []byte("\x00\x00\x00\x00"), []byte("xyza"))
	b.ReportAllocs()
	b.SetBytes(items)
	b.ResetTimer()
	mutexMetricCollector := benchmarking.NewMutexMetricsCollector()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			writeKeysFastCache(c, items, []byte("\x00\x00\x00\x00"), []byte("xyza"))
		}
	})
	mutexMetricCollector.Report(b)
}

func BenchmarkCacheGet(b *testing.B) {
	const items = 1 << 20
	c := New(newCacheConfigBenchmarkParams(12 * items))
	defer c.Close()
	k := []byte("\x00\x00\x00\x00")
	v := []byte("xyza")
	for i := 0; i < items; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		c.Set(k, v)
	}

	b.ReportAllocs()
	b.SetBytes(items)
	mutexMetricCollector := benchmarking.NewMutexMetricsCollector()
	mutexMetricCollector.Reset()
	b.RunParallel(func(pb *testing.PB) {
		var buf []byte
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				buf = c.Get(buf[:0], k)
			}
		}
	})
	mutexMetricCollector.Report(b)
}

func BenchmarkFastCachCacheGet(b *testing.B) {
	const items = 1 << 20
	c := fastcache.New(12 * items)
	defer c.Reset()
	k := []byte("\x00\x00\x00\x00")
	v := []byte("xyza")
	for i := 0; i < items; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		c.Set(k, v)
	}

	b.ReportAllocs()
	b.SetBytes(items)
	b.ResetTimer()
	mutexMetricCollector := benchmarking.NewMutexMetricsCollector()
	b.RunParallel(func(pb *testing.PB) {
		var buf []byte
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				buf = c.Get(buf[:0], k)
			}
		}
	})
	mutexMetricCollector.Report(b)
}

func BenchmarkCacheSetGet(b *testing.B) {
	const items = 1 << 16
	c := New(newCacheConfigBenchmarkParams(12 * items))
	defer c.Close()
	writeKeys(c, items, []byte("\x00\x00\x00\x00"), []byte("xyza"))
	b.ReportAllocs()
	b.SetBytes(2 * items)
	b.ResetTimer()
	mutexMetricCollector := benchmarking.NewMutexMetricsCollector()
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		var buf []byte
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				c.Set(k, v)
			}
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				buf = c.Get(buf[:0], k)
			}
		}
	})
	mutexMetricCollector.Report(b)
}

func BenchmarkFastCacheCacheSetGet(b *testing.B) {
	const items = 1 << 16
	c := fastcache.New(12 * items)
	defer c.Reset()
	writeKeysFastCache(c, items, []byte("\x00\x00\x00\x00"), []byte("xyza"))
	b.ReportAllocs()
	b.SetBytes(2 * items)
	b.ResetTimer()
	mutexMetricCollector := benchmarking.NewMutexMetricsCollector()
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		var buf []byte
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				c.Set(k, v)
			}
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				buf = c.Get(buf[:0], k)
			}
		}
	})
	mutexMetricCollector.Report(b)
}

func BenchmarkStdMapSet(b *testing.B) {
	const items = 1 << 16
	m := make(map[string][]byte)
	var mu sync.Mutex
	b.ReportAllocs()
	b.SetBytes(items)
	b.ResetTimer()
	mutexMetricCollector := benchmarking.NewMutexMetricsCollector()
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				mu.Lock()
				m[string(k)] = v
				mu.Unlock()
			}
		}
	})
	mutexMetricCollector.Report(b)
}

func BenchmarkStdMapGet(b *testing.B) {
	const items = 1 << 16
	m := make(map[string][]byte)
	k := []byte("\x00\x00\x00\x00")
	v := []byte("xyza")
	for i := 0; i < items; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		m[string(k)] = v
	}

	var mu sync.RWMutex
	b.ReportAllocs()
	b.SetBytes(items)
	b.ResetTimer()
	mutexMetricCollector := benchmarking.NewMutexMetricsCollector()
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				mu.RLock()
				vv := m[string(k)]
				mu.RUnlock()
				if string(vv) != string(v) {
					panic(fmt.Errorf("BUG: unexpected value; got %q; want %q", vv, v))
				}
			}
		}
	})
	mutexMetricCollector.Report(b)
}

func BenchmarkStdMapSetGet(b *testing.B) {
	const items = 1 << 16
	m := make(map[string][]byte)
	var mu sync.RWMutex
	b.ReportAllocs()
	b.SetBytes(2 * items)
	b.ResetTimer()
	mutexMetricCollector := benchmarking.NewMutexMetricsCollector()
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := []byte("xyza")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				mu.Lock()
				m[string(k)] = v
				mu.Unlock()
			}
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				mu.RLock()
				vv := m[string(k)]
				mu.RUnlock()
				if string(vv) != string(v) {
					panic(fmt.Errorf("BUG: unexpected value; got %q; want %q", vv, v))
				}
			}
		}
	})
	mutexMetricCollector.Report(b)
}

func BenchmarkSyncMapSet(b *testing.B) {
	const items = 1 << 16
	m := sync.Map{}
	b.ReportAllocs()
	b.SetBytes(items)
	b.ResetTimer()
	mutexMetricCollector := benchmarking.NewMutexMetricsCollector()
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := "xyza"
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				m.Store(string(k), v)
			}
		}
	})
	mutexMetricCollector.Report(b)
}

func BenchmarkSyncMapGet(b *testing.B) {
	const items = 1 << 16
	m := sync.Map{}
	k := []byte("\x00\x00\x00\x00")
	v := "xyza"
	for i := 0; i < items; i++ {
		k[0]++
		if k[0] == 0 {
			k[1]++
		}
		m.Store(string(k), v)
	}

	b.ReportAllocs()
	b.SetBytes(items)
	b.ResetTimer()
	mutexMetricCollector := benchmarking.NewMutexMetricsCollector()
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				vv, ok := m.Load(string(k))
				if !ok || vv.(string) != v {
					panic(fmt.Errorf("BUG: unexpected value; got %q; want %q", vv, v))
				}
			}
		}
	})
	mutexMetricCollector.Report(b)
}

func BenchmarkSyncMapSetGet(b *testing.B) {
	const items = 1 << 16
	m := sync.Map{}
	b.ReportAllocs()
	b.SetBytes(2 * items)
	b.ResetTimer()
	mutexMetricCollector := benchmarking.NewMutexMetricsCollector()
	b.RunParallel(func(pb *testing.PB) {
		k := []byte("\x00\x00\x00\x00")
		v := "xyza"
		for pb.Next() {
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				m.Store(string(k), v)
			}
			for i := 0; i < items; i++ {
				k[0]++
				if k[0] == 0 {
					k[1]++
				}
				vv, ok := m.Load(string(k))
				if !ok || vv.(string) != v {
					panic(fmt.Errorf("BUG: unexpected value; got %q; want %q", vv, v))
				}
			}
		}
	})
	mutexMetricCollector.Report(b)
}

func newCacheConfigBenchmarkParams(maxBytes int) *Config {
	return NewConfig(maxBytes, 1000, 100, 1)
}
