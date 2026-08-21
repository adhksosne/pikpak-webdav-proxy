package server

import (
	"context"
	"sync"
	"time"
)

// segCache 按 path 缓存已下载的段，供 4xVR cancel 重连时复用。
//
// cancel 后已下载段不丢，重连命中区域秒返回（不走网络）：
//   - cancel 后已下载段不丢，重连命中区域秒返回（不走网络）
//   - 限 64MB，LRU 淘汰
//   - TTL 5 秒（cancel 后 5 秒内重连可命中）
type segCache struct {
	mu        sync.Mutex
	chunks    map[int64][]byte // 段起始 offset -> 数据
	order     []int64          // LRU 顺序（最近访问在尾）
	totalBytes int64
	maxBytes  int64
	lastUsed  time.Time

	// 后台预读控制：cancel 后继续预读，重连命中
	preadMu     sync.Mutex
	preadCancel context.CancelFunc // 当前预读的取消函数
}

func newSegCache(maxBytes int64) *segCache {
	return &segCache{
		chunks:    make(map[int64][]byte),
		order:     make([]int64, 0, 64),
		maxBytes:  maxBytes,
		lastUsed:  time.Now(),
	}
}

// get 读一段（命中返回 data, true）
func (c *segCache) get(off int64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.chunks[off]
	if ok {
		c.touch(off)
	}
	return data, ok
}

// put 存一段，超 maxBytes 时按 LRU 淘汰。
// 已有更长段时不覆盖；新段更长时替换（避免旧短段卡住长度校验导致永远 miss）。
func (c *segCache) put(off int64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if old, exists := c.chunks[off]; exists {
		if int64(len(old)) >= int64(len(data)) {
			return // 已有更长段
		}
		// 替换为更长段，修正计数（order 位置不变）
		c.totalBytes -= int64(len(old))
		c.chunks[off] = data
		c.totalBytes += int64(len(data))
		c.lastUsed = time.Now()
		return
	}
	// 淘汰直到能放下
	for c.totalBytes+int64(len(data)) > c.maxBytes && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		if old, ok := c.chunks[oldest]; ok {
			c.totalBytes -= int64(len(old))
			delete(c.chunks, oldest)
		}
	}
	c.chunks[off] = data
	c.totalBytes += int64(len(data))
	c.order = append(c.order, off)
	c.lastUsed = time.Now()
}

// touch 把 off 移到 LRU 尾部（最近使用）
func (c *segCache) touch(off int64) {
	for i, o := range c.order {
		if o == off {
			c.order = append(c.order[:i], c.order[i+1:]...)
			c.order = append(c.order, off)
			break
		}
	}
	c.lastUsed = time.Now()
}

func (c *segCache) isExpired(ttl time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Since(c.lastUsed) > ttl
}

func (c *segCache) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.chunks)
}

// cancelPread 取消当前后台预读（新预读或缓存过期时调用）
func (c *segCache) cancelPread() {
	c.preadMu.Lock()
	defer c.preadMu.Unlock()
	if c.preadCancel != nil {
		c.preadCancel()
		c.preadCancel = nil
	}
}

// setPread 记录新的预读取消函数，返回旧的（调用方先 cancel 旧的）
func (c *segCache) setPread(cancel context.CancelFunc) {
	c.preadMu.Lock()
	defer c.preadMu.Unlock()
	if c.preadCancel != nil {
		c.preadCancel()
	}
	c.preadCancel = cancel
}

// sessionStore 管理按 path 的段缓存，定期 GC 过期项。
type sessionStore struct {
	mu      sync.Mutex
	caches  map[string]*segCache
	ttl     time.Duration
	maxPer  int64 // 单 path 最大缓存字节
}

func newSessionStore(ttl time.Duration, maxPerPath int64) *sessionStore {
	s := &sessionStore{
		caches: make(map[string]*segCache),
		ttl:    ttl,
		maxPer: maxPerPath,
	}
	go s.gcLoop()
	return s
}

// get 获取或创建某 path 的段缓存
func (s *sessionStore) get(path string) *segCache {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.caches[path]
	if !ok {
		c = newSegCache(s.maxPer)
		s.caches[path] = c
	}
	return c
}

// gcLoop 后台清理过期缓存（避免内存泄漏）
func (s *sessionStore) gcLoop() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		for path, c := range s.caches {
			if c.isExpired(s.ttl) {
				c.cancelPread() // 过期前停掉预读
				delete(s.caches, path)
			}
		}
		s.mu.Unlock()
	}
}
