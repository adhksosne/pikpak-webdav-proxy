package davclient

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// NodeEvaluator 做节点质量评估。
// 从 dav 拿多个 302（间隔避开 503），对每个 dl- 节点测速 32 MB，挑最快的。
type NodeEvaluator struct {
	client *Client
	// 测速块大小
	BenchSize int64
	// 拿 302 的间隔（避免触发账号级 503）
	FetchInterval time.Duration
	// 每个 path 的最优签名 URL（签名 URL 每文件唯一，必须按 path 缓存；
	// 之前全局单例导致跨文件复用小文件 URL，大文件 Range 全部 416）
	bestMu   sync.Mutex
	best     map[string]*SignedURL
	pickedAt map[string]time.Time
	// 已评估过的 path（速度自适应评估只做一次）
	evaluated map[string]bool
	// 后台评估标志，避免多个请求并发触发评估
	evaluating atomic.Int32
}

func NewNodeEvaluator(c *Client) *NodeEvaluator {
	return &NodeEvaluator{
		client:        c,
		BenchSize:     4 * 1024 * 1024, // 4MB 足以比出节点优劣；32MB 在慢网络下抢主请求带宽太久
		FetchInterval: 2 * time.Second,
		best:          make(map[string]*SignedURL),
		pickedAt:      make(map[string]time.Time),
		evaluated:     make(map[string]bool),
	}
}

// PickBestNode 拿签名 URL（纯查询，不触发任何网络评估）。
// 实测教训：评估的测速下载进行时，PikPak 网关会对该账号的新签名请求
// 返回空 200（会话级熔断），导致主请求 502。所以主请求路径绝不评估——
// 评估只能由 EvaluateIfSlow 在主请求结束后按需触发。
func (n *NodeEvaluator) PickBestNode(path string, candidates int) (*SignedURL, error) {
	n.bestMu.Lock()
	if s, ok := n.best[path]; ok && time.Since(n.pickedAt[path]) < 60*time.Minute {
		if time.Now().Before(s.Expiry.Add(-10 * time.Minute)) {
			n.bestMu.Unlock()
			return s, nil
		}
	}
	n.bestMu.Unlock()

	// 首次：只拿一个签名 URL 立即返回，不阻塞下载
	s, err := n.client.GetSignedURL(path)
	if err != nil {
		return nil, err
	}
	n.bestMu.Lock()
	n.best[path] = s
	n.pickedAt[path] = time.Now()
	n.bestMu.Unlock()
	return s, nil
}

// EvaluateIfSlow 速度自适应评估：主请求完成后速度仍低于阈值时，
// 后台静默评估换更快的节点（每文件只做一次）。
// 延迟 15 秒 + 空闲检查（isIdle）：bench 下载绝不与正在播放的请求抢带宽，
// 也避开播放器开场密集探测期（防熔断签名请求）。
func (n *NodeEvaluator) EvaluateIfSlow(path string, speedMBps float64, threshold float64, isIdle func() bool) {
	if speedMBps >= threshold {
		return
	}
	n.bestMu.Lock()
	if n.evaluated[path] {
		n.bestMu.Unlock()
		return
	}
	n.evaluated[path] = true
	n.bestMu.Unlock()

	if !n.evaluating.CompareAndSwap(0, 1) {
		return
	}
	go func() {
		time.Sleep(15 * time.Second) // 冷却期，避开播放器开场密集请求
		if isIdle != nil && !isIdle() {
			n.evaluating.Store(0)
			return // 期间有新请求（还在播放），放弃本轮评估
		}
		n.evaluateInBackground(path, 2)
	}()
}

// evaluateInBackground 后台评估：拿 N 个候选 URL，测速淘汰，更新该 path 的最优。
// 注意：直接 fetchSignedOnce 拿新 302，不动 client.signed 缓存——
// 主请求的签名 URL 始终有效，评估对主路径零干扰。
func (n *NodeEvaluator) evaluateInBackground(path string, candidates int) {
	defer n.evaluating.Store(0)

	// 串行收集 N 个不同节点的签名 URL（评估私有，不碰缓存）
	urls := make([]*SignedURL, 0, candidates)
	for i := 0; i < candidates; i++ {
		s, err := n.client.fetchSignedOnce(path)
		if err != nil {
			continue
		}
		urls = append(urls, s)
		if i < candidates-1 {
			time.Sleep(n.FetchInterval) // 避开 503
		}
	}

	if len(urls) <= 1 {
		return
	}

	// 唯一节点去重
	seen := make(map[string]bool)
	uniq := make([]*SignedURL, 0, len(urls))
	for _, u := range urls {
		if !seen[u.Node] {
			seen[u.Node] = true
			uniq = append(uniq, u)
		}
	}
	if len(uniq) <= 1 {
		return
	}

	// 各做一次 4 MB 测速
	bestSpeed := -1.0
	bestIdx := 0
	for i, s := range uniq {
		start := int64(i) * n.BenchSize
		speed := n.benchOne(s, start, n.BenchSize)
		if speed > bestSpeed {
			bestSpeed = speed
			bestIdx = i
		}
	}

	n.bestMu.Lock()
	if bestSpeed > 0 {
		n.best[path] = uniq[bestIdx]
		n.pickedAt[path] = time.Now()
	}
	n.bestMu.Unlock()
}

// benchOne 对单个签名 URL 下一段 size 字节，返回 MB/s。
// 失败返回 -1。
func (n *NodeEvaluator) benchOne(s *SignedURL, start, size int64) float64 {
	resp, err := n.client.RangeGet(s, start, size)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	t0 := time.Now()
	n2, _ := io.Copy(io.Discard, resp.Body)
	elapsed := time.Since(t0).Seconds()
	if elapsed <= 0 {
		return -1
	}
	return float64(n2) / 1024 / 1024 / elapsed
}

