package davclient

import (
	"sync"
	"sync/atomic"
	"time"
)

// NodeEvaluator 节点优选 + 签名管理。
// 核心：DNS 域名枚举优选（参考 CF 优选 IP）——签名 query 只绑文件不绑节点，
// 枚举 dl-z01a-XXXX 兄弟主机名 → DNS 探活 → 小块测速 → 全局排名，
// 全程零网关请求（免疫 302 熔断）。详见 node_enum.go。
type NodeEvaluator struct {
	client *Client
	// 每个 path 的最优签名 URL（签名 URL 每文件唯一，必须按 path 缓存；
	// 之前全局单例导致跨文件复用小文件 URL，大文件 Range 全部 416）
	bestMu   sync.Mutex
	best     map[string]*SignedURL
	pickedAt map[string]time.Time
	// DNS 枚举优选状态（全局共享——节点质量只与用户位置相关，与文件无关）
	enumMu        sync.Mutex
	enumBest      string     // 当前最优节点主机名（空 = 未扫描）
	enumRanked    []nodeRank // 完整排名（诊断用）
	enumScannedAt time.Time
	enumScanning  atomic.Int32
}

func NewNodeEvaluator(c *Client) *NodeEvaluator {
	return &NodeEvaluator{
		client:    c,
		best:      make(map[string]*SignedURL),
		pickedAt:  make(map[string]time.Time),
	}
}

// PickBestNode 拿签名 URL（纯查询，不触发任何网络评估）。
// 实测教训：评估的测速下载进行时，PikPak 网关会对该账号的新签名请求
// 返回空 200（会话级熔断），导致主请求 502。所以主请求路径绝不评估——
// 枚举扫描只由 scanIfStale 在后台触发（且不碰网关）。
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
	s = n.rewriteBest(s)  // 换到最优节点（枚举完成后生效）
	n.scanIfStale(s, nil) // 后台枚举（防抖：15s 延迟 + 每小时刷新一次）
	n.bestMu.Lock()
	n.best[path] = s
	n.pickedAt[path] = time.Now()
	n.bestMu.Unlock()
	return s, nil
}

// EvaluateIfSlow 速度自适应评估：主请求完成后速度仍低于阈值时，
// 后台静默触发节点枚举扫描（空闲时才测速，不抢播放带宽）。
// 扫描零网关请求，与旧机制（多次 302 收集节点）不同，熔断期间也能跑。
func (n *NodeEvaluator) EvaluateIfSlow(path string, speedMBps float64, threshold float64, isIdle func() bool) {
	if speedMBps >= threshold {
		return
	}
	n.bestMu.Lock()
	sample := n.best[path]
	n.bestMu.Unlock()
	n.scanIfStale(sample, isIdle)
}
