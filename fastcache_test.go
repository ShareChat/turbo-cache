package turbocache

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const cacheDelay = 100

func TestCacheSmall(t *testing.T) {
	c := New(NewSyncWriteConfig(10))
	defer c.Close()

	if v := c.Get(nil, []byte("aaa")); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}
	if v, exist := c.HasGet(nil, []byte("aaa")); exist || len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}

	c.Set([]byte("key"), []byte("value"))
	if v := c.getNotNilWithDefaultWait(nil, []byte("key")); string(v) != "value" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "value")
	}
	if v := c.getNotNilWithDefaultWait(nil, nil); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}
	if v, exist := c.HasGet(nil, nil); exist {
		t.Fatalf("unexpected nil-keyed value obtained in small cache: %q", v)
	}
	if v := c.getNotNilWithDefaultWait(nil, []byte("aaa")); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}

	c.Set([]byte("aaa"), []byte("bbb"))
	if v := c.getNotNilWithDefaultWait(nil, []byte("aaa")); string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}
	if v, exist := c.hasGetNotNilWithDefaultWait(nil, []byte("aaa")); !exist || string(v) != "bbb" {
		t.Fatalf("unexpected value obtained; got %q; want %q", v, "bbb")
	}

	c.Reset()
	if v := c.getNotNilWithDefaultWait(nil, []byte("aaa")); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from empty cache: %q", v)
	}
	if v, exist := c.hasGetNotNilWithDefaultWait(nil, []byte("aaa")); exist || len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from small cache: %q", v)
	}

	// Test empty value
	k := []byte("empty")
	c.Set(k, nil)

	if v := c.getNotNilWithDefaultWait(nil, k); len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from empty entry: %q", v)
	}
	if v, exist := c.hasGetNotNilWithDefaultWait(nil, k); !exist {
		t.Fatalf("cannot find empty entry for key %q", k)
	} else if len(v) != 0 {
		t.Fatalf("unexpected non-empty value obtained from empty entry: %q", v)
	}
	if !c.Has(k) {
		t.Fatalf("cannot find empty entry for key %q", k)
	}
	if c.Has([]byte("foobar")) {
		t.Fatalf("non-existing entry found in the cache")
	}
}

func TestCacheWrap(t *testing.T) {
	c := New(newCacheConfigWithDefaultParams(bucketsCount * chunkSize * 1.5))
	defer c.Close()

	calls := uint64(5e6)
	for i := uint64(0); i < calls; i++ {
		k := []byte(fmt.Sprintf("key %d", i))
		v := []byte(fmt.Sprintf("value %d", i))
		c.Set(k, v)
	}
	c.waitForExpectedCacheSize(cacheDelay)
	for i := uint64(0); i < calls/10; i++ {
		x := i * 10
		k := []byte(fmt.Sprintf("key %d", x))
		v := []byte(fmt.Sprintf("value %d", x))
		vv := c.Get(nil, k)
		if len(vv) > 0 && string(v) != string(vv) {
			t.Fatalf("unexpected value for key %q; got %q; want %q", k, vv, v)
		}
	}

	var s Stats
	c.UpdateStats(&s, true)
	getCalls := calls / 10
	if s.GetCalls != getCalls {
		t.Fatalf("unexpected number of getCalls; got %d; want %d", s.GetCalls, getCalls)
	}
	if s.SetCalls != calls {
		t.Fatalf("unexpected number of setCalls; got %d; want %d", s.SetCalls, calls)
	}
	if s.Misses == 0 || s.Misses >= calls/10 {
		t.Fatalf("unexpected number of misses; got %d; it should be between 0 and %d", s.Misses, calls/10)
	}
	if s.Collisions != 0 {
		t.Fatalf("unexpected number of collisions; got %d; want 0", s.Collisions)
	}
	if s.EntriesCount < calls/5 {
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

func TestShouldDropWritingOnBufferOverflow(t *testing.T) {
	itemsCount := 512 * setBufSize * 4
	const gorotines = 10
	c := New(NewConfigWithDroppingOnContention(30*itemsCount*gorotines, 5, 100, 1000))
	c.Close()

	for i := 0; i < itemsCount; i++ {
		c.Set([]byte(fmt.Sprintf("key %d", i)), []byte(fmt.Sprintf("value %d", i)))
	}
	var s Stats
	c.UpdateStats(&s, true)
	if s.DropsInQueue == 0 {
		t.Fatalf("drop writes should be presented")
	}
}

func TestShouldDropWritingOnLimitSetting(t *testing.T) {
	itemsCount := 16 * setBufSize
	const gorotines = 10
	c := New(NewConfigWithDroppingOnContention(30*itemsCount*gorotines, 5, 10, 10))

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		curId := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < itemsCount; i++ {
				c.Set([]byte(fmt.Sprintf("key %d, gorutine: %d", i, curId)), []byte(fmt.Sprintf("value %d", i)))
			}
		}()
	}
	wg.Wait()
	var s Stats
	c.UpdateStats(&s, true)
	if s.DroppedWrites == 0 {
		t.Fatalf("drop writes should be presented")
	}
}

func TestCacheResetUpdateStatsSetConcurrent(t *testing.T) {
	c := New(newCacheConfigWithDefaultParams(12334))

	stopCh := make(chan struct{})

	// run workers for cache reset
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
			if len(c.buckets[i].setBuf) > 0 && atomic.LoadUint64(&c.buckets[i].writeBufferSize) > 0 {
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
	return NewConfigWithDroppingOnContention(maxBytes, defaultFlushInterval, defaultBatchWriteSize, 100000)
}
