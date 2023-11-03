package turbocache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"golang.org/x/sync/semaphore"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const cacheDelay = 250

func TestCacheAsync(t *testing.T) {
	for _, batch := range []int{1, 3, 256, 1024} {
		t.Run(fmt.Sprintf("batch_%d", batch), func(t *testing.T) {
			c := New(NewConfig(bucketsCount*chunkSize*1.5, defaultFlushInterval, batch, 1))
			defer c.Close()

			calls := uint64(100000)
			missed := uint64(0)
			for i := uint64(0); i < calls; i++ {
				k := []byte(fmt.Sprintf("key %d", i))
				v := []byte(fmt.Sprintf("value %d", i))
				c.Set(k, v)
			}
			for i := uint64(0); i < calls; i++ {
				x := i
				k := []byte(fmt.Sprintf("key %d", x))
				v := []byte(fmt.Sprintf("value %d", x))
				vv, _ := c.getNotNilWithWait(nil, k, 5)
				if len(vv) == 0 {
					missed++
				}
				if len(vv) > 0 && string(v) != string(vv) {
					t.Fatalf("unexpected value; got %s; want  %s; key: %s", string(vv), string(v), k)
				}
			}

			if missed > calls/10 {
				t.Fatalf("unexpected missed getCalls; got %d; want > %d", missed, calls/10*2)
			}

			var s Stats
			c.UpdateStats(&s, true)
			if s.DropsInQueue > calls/10 {
				t.Fatalf("unexpected number of dropped writes; got %d; want > %d", s.DropsInQueue, calls/10)
			}
		})
	}
}

func TestCacheWrapAsync(t *testing.T) {
	c := New(newCacheConfigWithDefaultParams(bucketsCount * chunkSize * 1.5))
	notNullCount := 0
	defer c.Close()

	calls := uint64(5e6)
	for i := uint64(0); i < calls; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
	}

	for i := uint64(0); i < calls/10; i++ {
		x := i * 10
		k := []byte(fmt.Sprintf("key %d", x))
		v := []byte(fmt.Sprintf("value %d", x))
		vv := c.Get(nil, k)
		if len(vv) > 0 && string(v) != string(vv) {
			t.Fatalf("unexpected value for key %q; got %q; want %q", k, vv, v)
		}
		if len(vv) > 0 {
			notNullCount++
		}
	}

	var s Stats
	c.UpdateStats(&s, true)
	getCalls := calls / 10
	if s.GetCalls != getCalls {
		t.Fatalf("unexpected number of getCalls; got %d; want %d", s.GetCalls, getCalls)
	}
	if s.SetCalls < calls/10*9 {
		t.Fatalf("unexpected number of setCalls; got %d; want %d", s.SetCalls, calls)
	}
	if s.Misses == 0 || s.Misses >= calls/10 {
		t.Fatalf("unexpected number of misses; got %d; it should be between 0 and %d", s.Misses, calls/10)
	}
	if s.Collisions != 0 {
		t.Fatalf("unexpected number of collisions; got %d; want 0", s.Collisions)
	}
	if s.EntriesCount < calls/10 {
		t.Fatalf("unexpected number of items; got %d; cannot be smaller than %d", s.EntriesCount, calls/5)
	}
	if s.BytesSize < 1024 {
		t.Fatalf("unexpected BytesSize; got %d; cannot be smaller than %d", s.BytesSize, 1024)
	}
	if s.MaxBytesSize < 32*1024*1024 {
		t.Fatalf("unexpected MaxBytesSize; got %d; cannot be smaller than %d", s.MaxBytesSize, 32*1024*1024)
	}
}

func TestCacheDel(t *testing.T) {
	c := New(newCacheConfigWithDefaultParams(1024))
	defer c.Close()
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.setSync(k, v)

		vv := c.Get(nil, k)
		if string(vv) != string(v) {
			t.Fatalf("unexpected value for key %q; got %q; want %q", k, vv, v)
		}
		c.Del(k)
		vv = c.Get(nil, k)
		if len(vv) > 0 {
			t.Fatalf("unexpected non-empty value got for key %q: %q", k, vv)
		}
	}
}

func TestCacheBigKeyValue(t *testing.T) {
	c := New(newCacheConfigWithDefaultParams(1024))
	defer c.Close()

	// Both key and value exceed 64Kb
	k := make([]byte, 90*1024)
	v := make([]byte, 100*1024)
	c.Set(k, v)
	vv := c.getNotNilWithDefaultWait(nil, k)
	if len(vv) > 0 {
		t.Fatalf("unexpected non-empty value got for key %q: %q", k, vv)
	}

	// len(key) + len(value) > 64Kb
	k = make([]byte, 40*1024)
	v = make([]byte, 40*1024)
	c.Set(k, v)
	vv = c.getNotNilWithDefaultWait(nil, k)
	if len(vv) > 0 {
		t.Fatalf("unexpected non-empty value got for key %q: %q", k, vv)
	}
}

func TestCacheSetGetSerial(t *testing.T) {
	itemsCount := 10000
	c := New(newCacheConfigWithDefaultParams(30 * itemsCount))
	defer c.Close()
	if err := testCacheGetSet(c, itemsCount); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestCacheGetSetConcurrent(t *testing.T) {
	itemsCount := 1000
	const gorotines = 10
	c := New(newCacheConfigWithDefaultParams(30 * itemsCount * gorotines))
	defer c.Close()

	ch := make(chan error, gorotines)
	for i := 0; i < gorotines; i++ {
		go func() {
			ch <- testCacheGetSet(c, itemsCount)
		}()
	}
	for i := 0; i < gorotines; i++ {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		case <-time.After(300 * time.Second):
			t.Fatalf("timeout")
		}
	}
}

func testCacheGetSet(c *Cache, itemsCount int) error {
	for i := 0; i < itemsCount; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
	}
	for i := 0; i < itemsCount; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		vv := c.getNotNilWithDefaultWait(nil, k)
		if string(vv) != string(v) {
			return fmt.Errorf("unexpected value for key %q after insertion; got %q; want %q", k, vv, v)
		}
	}

	misses := 0
	for i := 0; i < itemsCount; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		vExpected := fmt.Sprintf("value %d", i)
		v := c.Get(nil, k)
		if string(v) != vExpected {
			if len(v) > 0 {
				return fmt.Errorf("unexpected value for key %q after all insertions; got %q; want %q", k, v, vExpected)
			}
			misses++
		}
	}
	if misses >= itemsCount/100 {
		return fmt.Errorf("too many cache misses; got %d; want less than %d", misses, itemsCount/100)
	}
	return nil
}

func TestAsyncInsertToCache(t *testing.T) {
	itemsCount := 64 * 1024
	for _, batch := range []int{1, 3, 131} {
		t.Run(fmt.Sprintf("batch_%d", batch), func(t *testing.T) {
			c := New(newCacheConfigWithDefaultParams(64 * itemsCount))
			c.stopFlushing()
			bucket := &c.buckets[0]
			notFoundCount := 0
			for i := 0; i < itemsCount; i++ {
				key := []byte(fmt.Sprintf("key %d", i))
				hash := xxhash.Sum64(key)
				expectedValue := []byte(fmt.Sprintf("value %d", i))
				bucket.logger.(*cacheLogger).onNewItem(key, expectedValue, hash, batch)

				actualValue, found, _ := bucket.Get(nil, key, hash, true)

				if !found {
					notFoundCount++
				}
				if found && string(expectedValue) != string(actualValue) {
					t.Fatalf("key %s, wanted %s got %s", string(key), string(expectedValue), string(actualValue))
				}
			}

			t.Logf("not found count: %d out %d", notFoundCount, itemsCount)
			expectedNotFoundThreshold := itemsCount / 3
			if notFoundCount > expectedNotFoundThreshold {
				t.Fatalf("too much not found. actual: %d, expected < %d", notFoundCount, expectedNotFoundThreshold)
			}
		})
	}
}

func TestAsyncInsertToCacheConcurrentRead(t *testing.T) {
	itemsCount := 4 * 1024 * 100
	for _, batch := range []int{11} {
		t.Run(fmt.Sprintf("batch_%d", batch), func(t *testing.T) {
			c := New(newCacheConfigWithDefaultParams(10 * chunkSize * bucketsCount))
			c.stopFlushing()

			ch := make(chan string, 128)
			var notFoundCount atomic.Int32
			bucket := &c.buckets[0]
			throttler := semaphore.NewWeighted(int64(runtime.NumCPU()))
			go func() {
				var wg sync.WaitGroup
				for i := 0; i < itemsCount; i++ {
					key := []byte(fmt.Sprintf("key %d", i))
					h := xxhash.Sum64(key)

					bucket.logger.(*cacheLogger).onNewItem(key, key, h, batch)
					wg.Add(1)
					_ = throttler.Acquire(context.Background(), 1)
					go func() {
						actualValue, found, l1cache := bucket.Get(nil, key, h, true)
						if !found {
							notFoundCount.Add(1)
							//it can be delayed and many items can be evicted
							if notFoundCount.Load() > int32(itemsCount)/2 {
								ch <- fmt.Sprintf("not found count expected less than %d, got %d", itemsCount/2, notFoundCount.Load())
							}
						}
						if found && string(key) != string(actualValue) {
							ch <- fmt.Sprintf("%s, wanted %s got %s, l1cache: %v", string(key), string(key), string(actualValue), l1cache)
						}
						wg.Done()
						throttler.Release(1)
					}()
				}
				wg.Wait()
				close(ch)
				fmt.Printf("stats not found %d out pf %d", notFoundCount.Load(), itemsCount)

			}()
			for err := range ch {
				t.Fatalf(err)
			}
		})
	}
}

func TestCacheResetUpdateStatsSetConcurrent(t *testing.T) {
	c := New(newCacheConfigWithDefaultParams(12334))
	defer c.Close()
	stopCh := make(chan struct{})

	// run workers for cache Reset
	var resettersWG sync.WaitGroup
	for i := 0; i < 10; i++ {
		resettersWG.Add(1)
		go func() {
			defer resettersWG.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					c.Reset()
					runtime.Gosched()
				}
			}
		}()
	}

	// run workers for update cache stats
	var statsWG sync.WaitGroup
	for i := 0; i < 10; i++ {
		statsWG.Add(1)
		go func() {
			defer statsWG.Done()
			var s Stats
			for {
				select {
				case <-stopCh:
					return
				default:
					c.UpdateStats(&s, true)
					runtime.Gosched()
				}
			}
		}()
	}

	// run workers for setting data to cache
	var settersWG sync.WaitGroup
	for i := 0; i < 10; i++ {
		settersWG.Add(1)
		go func() {
			defer settersWG.Done()
			for j := 0; j < 100; j++ {
				key := []byte(fmt.Sprintf("key_%d", j))
				value := []byte(fmt.Sprintf("value_%d", j))
				c.Set(key, value)
				runtime.Gosched()
			}
		}()
	}

	// wait for setters
	settersWG.Wait()
	close(stopCh)
	statsWG.Wait()
	resettersWG.Wait()
}

func (c *Cache) waitForExpectedCacheSize(delayInMillis int) error {
	t := time.Now()

	for time.Since(t).Milliseconds() < int64(delayInMillis) {
		for i := range c.buckets {
			if len(c.buckets[i].logger.(*cacheLogger).setBuf) > 0 && atomic.LoadUint64(&c.buckets[i].logger.(*cacheLogger).stats.writeBufferSize) > 0 {
				time.Sleep(time.Duration(delayInMillis/10) * time.Millisecond)
				continue
			}
		}
		return nil
	}
	return errors.New("timeout")
}

func (c *Cache) getNotNilWithDefaultWait(dst, k []byte) []byte {
	r, _ := c.getNotNilWithWait(dst, k, cacheDelay)
	return r
}

func (c *Cache) hasGetNotNilWithDefaultWait(dst, k []byte) ([]byte, bool) {
	t := time.Now()

	var result []byte
	var exists bool
	for time.Since(t).Milliseconds() < int64(cacheDelay) {
		if result, exists = c.HasGet(dst, k); !exists || result == nil {
			time.Sleep(cacheDelay / 10 * time.Millisecond)
			continue
		}
		return result, exists
	}
	return result, exists
}

func (c *Cache) getNotNilWithWait(dst, k []byte, delay int) ([]byte, error) {
	t := time.Now()

	for time.Since(t).Milliseconds() < int64(delay) {
		var result []byte
		if result = c.Get(dst, k); result == nil {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		return result, nil
	}
	return nil, errors.New("timeout")
}

func (c *Cache) getBigWithExpectedValue(dst, k []byte, expected []byte) []byte {
	t := time.Now()
	var result []byte
	for time.Since(t).Milliseconds() < int64(cacheDelay*100) {
		if result = c.GetBig(dst, k); !bytes.Equal(result, expected) {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		return result
	}
	return result
}

func newCacheConfigWithDefaultParams(maxBytes int) *Config {
	return NewConfig(maxBytes, defaultFlushInterval, defaultBatchWriteSize, 1)
}
