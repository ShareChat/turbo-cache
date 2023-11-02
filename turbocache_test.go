package turbocache

import "github.com/cespare/xxhash/v2"

func (c *Cache) setSync(k, v []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Set(k, v, h, true)
}
