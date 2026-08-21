package davclient

// 节点枚举优选 —— 参考 Cloudflare 优选 IP 的思路。
//
// 实测依据（2026-08-22 实验）：
//  1. 签名 URL 的 query 只绑定文件（fid），不绑定节点——把 dl-z01a-0063
//     的签名换到 dl-z01a-0050/0053 主机名上，全部 206 正常下载；
//  2. 节点编号空间是 DNS 白名单制：无 A 记录的编号 = 节点不存在；
//     有 A 记录的编号直接可用（个别节点 DNS 在但已死，测速阶段自然淘汰）；
//  3. 节点质量与文件无关，只与用户到 CDN 的链路相关——排名全局共享。
//
// 流程：拿到任一有效签名 → 提取主机名模板 dl-z01a-XXXX → 枚举编号
// 0000-0149 → DNS 探活（并行）→ 存活节点 512KB 测速（串行防带宽分摊失真）
// → 全局排名。全程零网关请求（不碰 302 签名交换），完全免疫账号级熔断。

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	enumMax        = 200              // 枚举编号 0-199（实测活跃簇 14-64，留余量）
	enumDNSWorker  = 16               // DNS 探活并发
	enumDNSTimeout = 3 * time.Second  // 单个 DNS 查询超时
	// 两阶段测速：初测全员 512KB（TTFB 敏感，关系拖动响应），
	// 前 3 名复测 4MB（稳态吞吐敏感）。总流量 ≈26MB，不触发账号级限速。
	enumBench1Size  = 512 * 1024      // 初测下载量
	enumBench1Start = 1 << 20         // 初测偏移（避开文件头热区）
	enumBench1TTL   = 6 * time.Second // 初测超时（慢节点直接淘汰）
	enumBench2Size  = 4 << 20         // 复测下载量
	enumBench2Start = 8 << 20         // 复测偏移（换段，排除初测缓存命中干扰）
	enumBench2TTL   = 10 * time.Second
	enumRescanGap   = time.Hour        // 重新扫描间隔
	enumKickDelay   = 15 * time.Second // 触发到开扫的延迟（避开播放器开场密集期）
)

// nodeHostRegex 拆解 dl-z01a-0063.mypikpak.net → 前缀 dl-z01a- / 编号 0063 / 域 .mypikpak.net
var nodeHostRegex = regexp.MustCompile(`^(.*-)(\d+)(\..+)$`)

// nodeRank 单节点测速结果
type nodeRank struct {
	host  string  // 完整主机名
	speed float64 // MB/s
}

// scanIfStale 后台触发枚举扫描（防抖）：从未扫描或距上次超过 1 小时才扫。
// isIdle 非空时开扫前等空闲（bench 不抢播放带宽）；nil 直接扫。
// 非阻塞，可随意调用（每次文件打开时都调也没问题）。
func (n *NodeEvaluator) scanIfStale(sample *SignedURL, isIdle func() bool) {
	if sample == nil || sample.Expiry.Before(time.Now()) {
		return
	}
	n.enumMu.Lock()
	stale := n.enumBest == "" || time.Since(n.enumScannedAt) > enumRescanGap
	n.enumMu.Unlock()
	if !stale || !n.enumScanning.CompareAndSwap(0, 1) {
		return
	}
	go func() {
		defer n.enumScanning.Store(0)
		time.Sleep(enumKickDelay)
		if isIdle != nil && !isIdle() {
			return // 期间有新请求（还在播放），放弃本轮
		}
		n.enumMu.Lock()
		if n.enumBest != "" && time.Since(n.enumScannedAt) <= enumRescanGap {
			n.enumMu.Unlock() // 等待期间别的扫描已完成
			return
		}
		n.enumMu.Unlock()
		n.ScanNodes(sample)
	}()
}

// ScanNodes 枚举 + 探活 + 测速 + 排名（同步执行，工具/测试可直调）。
// sample 必须是有效签名（提供 query），网关熔断期间也可用（不碰网关）。
func (n *NodeEvaluator) ScanNodes(sample *SignedURL) {
	tpl := nodeTemplate(sample)
	if tpl == "" {
		return
	}

	// 1. DNS 探活（白名单：有 A 记录才算节点存在）
	alive := make([]bool, enumMax)
	sem := make(chan struct{}, enumDNSWorker)
	var wg sync.WaitGroup
	for i := 0; i < enumMax; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			ctx, cancel := context.WithTimeout(context.Background(), enumDNSTimeout)
			defer cancel()
			if _, err := n.client.dns.lookup(ctx, fmt.Sprintf(tpl, i)); err == nil {
				alive[i] = true
			}
		}(i)
	}
	wg.Wait()

	hosts := make([]string, 0, 8)
	for i, ok := range alive {
		if ok {
			hosts = append(hosts, fmt.Sprintf(tpl, i))
		}
	}
	if Verbose {
		log.Printf("[节点优选] DNS 探活 %d 个编号，存活 %d 个: %s", enumMax, len(hosts), strings.Join(hosts, ", "))
	}
	if len(hosts) == 0 {
		return
	}

	// 2a. 初测：全部存活节点串行 512KB（TTFB 敏感）
	quick := make([]nodeRank, 0, len(hosts))
	for _, host := range hosts {
		ctx, cancel := context.WithTimeout(context.Background(), enumBench1TTL)
		speed := n.benchNode(ctx, host, sample, enumBench1Start, enumBench1Size)
		cancel()
		if speed > 0 {
			quick = append(quick, nodeRank{host: host, speed: speed})
		}
	}
	if len(quick) == 0 {
		return
	}
	sort.Slice(quick, func(i, j int) bool { return quick[i].speed > quick[j].speed })
	if Verbose {
		log.Printf("[节点优选] 初测排名（前 8）: %s", rankStr(quick, 8))
	}

	// 2b. 复测：前 3 名换偏移 4MB（稳态吞吐敏感，排除初测缓存命中）
	finalists := quick
	if len(quick) > 3 {
		finalists = quick[:3]
	}
	ranked := make([]nodeRank, 0, len(finalists))
	for _, q := range finalists {
		ctx, cancel := context.WithTimeout(context.Background(), enumBench2TTL)
		speed := n.benchNode(ctx, q.host, sample, enumBench2Start, enumBench2Size)
		cancel()
		if speed > 0 {
			ranked = append(ranked, nodeRank{host: q.host, speed: speed})
		}
	}
	if len(ranked) == 0 {
		ranked = quick // 复测全挂（签名临期等），退回初测结果
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].speed > ranked[j].speed })

	// 3. 记录全局最优
	n.enumMu.Lock()
	n.enumBest = ranked[0].host
	n.enumRanked = ranked
	n.enumScannedAt = time.Now()
	n.enumMu.Unlock()

	// 4. 已缓存的各文件签名也换到新最优（仍然有效的才换，慢文件立即受益）
	n.bestMu.Lock()
	for p, s := range n.best {
		if time.Now().Before(s.Expiry.Add(-10 * time.Minute)) {
			n.best[p] = swapHost(s, ranked[0].host)
		}
	}
	n.bestMu.Unlock()

	if Verbose {
		log.Printf("[节点优选] 复测排名: %s → 已锁定 %s", rankStr(ranked, len(ranked)), hostNode(ranked[0].host))
	}
}

// rankStr 把排名格式化成 "节点=速度MB/s, ..."（最多 topN 个）。
func rankStr(rs []nodeRank, topN int) string {
	if len(rs) < topN {
		topN = len(rs)
	}
	var sb strings.Builder
	for i := 0; i < topN; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "%s=%.1fMB/s", hostNode(rs[i].host), rs[i].speed)
	}
	return sb.String()
}

// BestNode 返回当前最优节点主机名（未扫描时为空）。
func (n *NodeEvaluator) BestNode() string {
	n.enumMu.Lock()
	defer n.enumMu.Unlock()
	return n.enumBest
}

// benchNode 用 sample 的签名换 host 后下载 size 字节测速，返回 MB/s（失败 -1）。
func (n *NodeEvaluator) benchNode(ctx context.Context, host string, sample *SignedURL, start, size int64) float64 {
	req, err := http.NewRequestWithContext(ctx, "GET", swapHost(sample, host).URL, nil)
	if err != nil {
		return -1
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+size-1))
	req.Header.Set("User-Agent", browserUA)
	resp, err := n.client.http.Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return -1
	}
	t0 := time.Now()
	n2, _ := io.Copy(io.Discard, resp.Body)
	elapsed := time.Since(t0).Seconds()
	if elapsed <= 0 || n2 <= 0 {
		return -1
	}
	return float64(n2) / 1024 / 1024 / elapsed
}

// swapHost 克隆签名并替换主机名（签名 query 与节点无关，实测可跨节点复用）。
func swapHost(s *SignedURL, host string) *SignedURL {
	parsed, err := url.Parse(s.URL)
	if err != nil || parsed.Hostname() == host {
		return s
	}
	c := *s
	c.URL = strings.Replace(s.URL, parsed.Hostname(), host, 1)
	c.Node = hostNode(host)
	return &c
}

// nodeTemplate 从签名 URL 提取编号模板，如 "dl-z01a-%04d.mypikpak.net"。
func nodeTemplate(sample *SignedURL) string {
	parsed, err := url.Parse(sample.URL)
	if err != nil {
		return ""
	}
	m := nodeHostRegex.FindStringSubmatch(parsed.Hostname())
	if m == nil {
		return ""
	}
	return m[1] + "%04d" + m[3]
}

// hostNode 取主机名第一段作为节点名（dl-z01a-0063.mypikpak.net → dl-z01a-0063）。
func hostNode(host string) string {
	if i := strings.IndexByte(host, '.'); i > 0 {
		return host[:i]
	}
	return host
}

// rewriteBest 把签名换成当前最优节点（未扫描或已在最优节点上则原样返回）。
func (n *NodeEvaluator) rewriteBest(s *SignedURL) *SignedURL {
	n.enumMu.Lock()
	best := n.enumBest
	n.enumMu.Unlock()
	if best == "" {
		return s
	}
	return swapHost(s, best)
}
