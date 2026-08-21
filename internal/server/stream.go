package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"pikpak-webdav-proxy/internal/davclient"
)

// ParallelStreamer 是核心模块。
//
// 调度参数（针对 4K 流媒体播放调优）：
//   - Concurrency=8        （8 并发对单文件足够）
//   - BigChunk=2MB          （吞吐主力段）
//   - MidChunk=1MB
//   - SmallChunk=512KB     （seek 后首块）
//   - SegCacheMaxPer=64MB  （内存段缓存，grace period 实质）
//   - SegCacheTTL=5s       （cancel 后 5s 内重连可命中）
type ParallelStreamer struct {
	dav       *davclient.Client
	evaluator *davclient.NodeEvaluator

	Concurrency int
	BigChunk    int64
	MidChunk    int64
	SmallChunk  int64
	FirstChunk  int64 // 首段大小：冷连接（TCP+TLS+慢启动）下小段 TTFB 快数倍
	speedLog    *SpeedLog
	sessions    *sessionStore
	disk        *DiskCache

	// 预热控制：真实请求到达时停掉预热，避免抢连接/带宽
	prewarmMu     sync.Mutex
	prewarmCancel context.CancelFunc

	// 每文件最近访问时间（节点评估只在空闲时跑，避免 bench 下载抢播放带宽）
	touchMu sync.Mutex
	touched map[string]time.Time
}

type SpeedLog struct {
	mu      sync.Mutex
	samples []float64
}

func NewSpeedLog() *SpeedLog {
	return &SpeedLog{samples: make([]float64, 0, 100)}
}

func (s *SpeedLog) Add(mbPerSec float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples = append(s.samples, mbPerSec)
	if len(s.samples) > 100 {
		s.samples = s.samples[1:]
	}
}

func NewStreamer(dav *davclient.Client, cacheDir string) *ParallelStreamer {
	return &ParallelStreamer{
		dav:         dav,
		evaluator:   davclient.NewNodeEvaluator(dav),
		Concurrency: 8,                   // 8 并发对单文件足够
		BigChunk:    2 * 1024 * 1024,     // 吞吐主力段
		MidChunk:    1 * 1024 * 1024,
		SmallChunk:  512 * 1024,          // seek 后首块
		FirstChunk:  64 * 1024,           // 首段 64KB：冷连接下 TTFB 关键（实测 512KB 冷连接 3s vs 64KB 亚秒）
		speedLog:    NewSpeedLog(),
		touched:     make(map[string]time.Time),
		// 内存按块缓存：拖回访问过的位置秒响应（用户实验验证的行为）
		// TTL 30 分钟覆盖整个拖动会话；上限 1GB 装多点块（几百 MB WorkingSet 级别）
		// 拖多个位置填满后溢出到磁盘缓存（持久，跨会话）
		sessions:    newSessionStore(30*time.Minute, 1024*1024*1024),
		disk:        NewDiskCache(cacheDir),
	}
}

// ServeFile 把 PikPak 文件流式输出给客户端。
//
// 三级缓存优先级：内存段缓存 > 磁盘缓存 > 网络
//   - 内存命中：秒回（最热数据）
//   - 磁盘命中：本地 pread（已下载段，跨会话持久）
//   - 网络：并发下载，边下边写盘+写内存
//   - 首次 GET 触发后台整文件下载
//   - 整文件下完后所有 Range 命中本地，随便拖不卡
func (s *ParallelStreamer) ServeFile(w http.ResponseWriter, r *http.Request, path string, fileSize int64) error {
	t0 := time.Now()
	s.stopPrewarm() // 真实请求优先，预热让路
	s.touch(path)
	start, end, hasRange := parseRange(r.Header.Get("Range"), fileSize)
	// 畸形 Range 防护：start > end 或 start 越界会导致负 length → makeslice panic
	if hasRange && (start > end || start >= fileSize || start < 0) {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", fileSize))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return nil
	}
	length := end - start + 1

	// 统一用 BigChunk(2MB) 做缓存边界，避免不同请求 chunk size 不同导致缓存段不兼容
	// （之前 1MB Range 存 1MB 段，整文件 2MB chunk 查到 1MB 段切片越界 → size=0 bug）
	chunk := s.BigChunk

	signed, err := s.evaluator.PickBestNode(path, 3)
	if err != nil {
		return fmt.Errorf("pick node: %w", err)
	}
	tSigned := time.Since(t0) // 签名（含可能的 302 往返）耗时
	memCache := s.sessions.get(path)
	// 新请求优先：停掉旧的后台预读，避免它抢占本请求的连接/带宽（拖动场景尤其关键）
	memCache.cancelPread()
	// 磁盘缓存：按 path 取（懒加载，命中已下载段）；未启用（cachedir 空）时为 nil
	var diskCache *cacheFile
	if s.disk != nil {
		fileName := pathBase(path)
		var derr error
		diskCache, derr = s.disk.get(path, fileName, fileSize, chunk)
		if derr != nil {
			log.Printf("disk cache init failed for %s: %v, continue without", path, derr)
		}
	}
	// 注：实测验证纯内存按块缓存即可（不主动整文件下载，退出播放才清）
	// 磁盘缓存只持久化"正常下载访问过的段"，跨会话命中，不主动预下载整文件

	w.Header().Set("Content-Type", "binary/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", length))
	if hasRange {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	vlog("use node %s, ms=%d, range=%d-%d (%d bytes), chunk=%d, disk=%d segs",
		signed.Node, signed.Ms, start, end, length, chunk, diskCacheSegs(diskCache))

	ctx := r.Context()
	return s.parallelCopy(ctx, path, signed, memCache, diskCache, start, length, chunk, fileSize, w, tSigned)
}

func diskCacheSegs(c *cacheFile) int {
	if c == nil {
		return -1
	}
	return c.downloadedCount()
}

// pathBase 取 path 最后一段作为缓存文件名基础
func pathBase(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == '\\' {
			return p[i+1:]
		}
	}
	return p
}

// Prewarm 目录浏览后预热：对目录里的视频文件预取签名 URL + 建立到节点的热连接。
// 播放器显示文件列表到用户点击之间有几秒窗口，利用它把"302 往返 + TCP/TLS 冷启动"
// 提前做掉——用户点击时首段直接跑热连接，TTFB 只剩传输时间。
// 串行 + 间隔，避免触发账号级 302 限流；最多预热 3 个文件。
// 真实请求到达时自动让路（stopPrewarm）。
func (s *ParallelStreamer) Prewarm(entries []davclient.DavEntry) {
	files := make([]string, 0, 4)
	for _, e := range entries {
		if e.IsDir || e.Size <= 0 {
			continue
		}
		if p, err := url.PathUnescape(e.Href); err == nil {
			files = append(files, p)
		}
		if len(files) >= 3 {
			break
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.prewarmMu.Lock()
	if s.prewarmCancel != nil {
		s.prewarmCancel() // 新一轮预热取代旧的
	}
	s.prewarmCancel = cancel
	s.prewarmMu.Unlock()

	for i, p := range files {
		if ctx.Err() != nil {
			return // 真实请求已到，让路
		}
		if i > 0 {
			select {
			case <-time.After(1 * time.Second): // 串行间隔，避开 302 限流
			case <-ctx.Done():
				return
			}
		}
		signed, err := s.evaluator.PickBestNode(p, 1)
		if err != nil {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		// 1 字节 Range 预热连接（连接回池，后续 GET 直接复用）
		resp, err := s.dav.RangeGet(signed, 0, 1)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		vlog("prewarmed %s (node %s)", p, signed.Node)
	}
}

// stopPrewarm 停掉后台预热（真实请求到达时调用，让路）
func (s *ParallelStreamer) stopPrewarm() {
	s.prewarmMu.Lock()
	if s.prewarmCancel != nil {
		s.prewarmCancel()
		s.prewarmCancel = nil
	}
	s.prewarmMu.Unlock()
}

// touch 记录文件最近访问时间（节点评估的空闲判定依据）
func (s *ParallelStreamer) touch(path string) {
	s.touchMu.Lock()
	s.touched[path] = time.Now()
	s.touchMu.Unlock()
}

// downloadAllToDisk 后台整文件下载到磁盘缓存。
// 低优先级，从文件头顺序下载，跳过已缓存段。下完后整文件本地，随便拖不卡。
func (s *ParallelStreamer) downloadAllToDisk(ctx context.Context, path string, signed *davclient.SignedURL,
	c *cacheFile, chunk, fileSize int64) {
	// 用独立 context，不受请求 cancel 影响（后台持续下载）
	bg, cancel := context.WithCancel(context.Background())
	defer cancel()
	const conc = 3 // 低并发，不抢主请求带宽
	var wg sync.WaitGroup
	sem := make(chan struct{}, conc)
	for off := int64(0); off < fileSize; off += chunk {
		if bg.Err() != nil {
			return
		}
		size := chunk
		if off+size > fileSize {
			size = fileSize - off
		}
		if c.has(off, size) {
			continue // 已缓存跳过
		}
		select {
		case sem <- struct{}{}:
		case <-bg.Done():
			return
		}
		wg.Add(1)
		go func(o, sz int64) {
			defer wg.Done()
			defer func() { <-sem }()
			buf, ok := s.downloadSegWithRetry(bg, path, signed, o, sz)
			if ok {
				c.write(o, buf)
			}
		}(off, size)
	}
	wg.Wait()
	log.Printf("background full-file download done for %s (%d/%d segs)",
		path, c.downloadedCount(), (fileSize+chunk-1)/chunk)
}

// parallelCopy 三级缓存命中优先混合流：
//   - 首段小段(512KB)同步下载独享带宽：最快出首帧
//   - 按段 idx 顺序 flush
//   - 命中段：内存→磁盘→秒取，立即写客户端
//   - 未命中段：worker 并发下载，完整段写完入内存+磁盘 + 写客户端
//   - 段结果带 done 标记：失败段占位跳过，flush 不会被 nil 卡死（修 size=0 bug）
func (s *ParallelStreamer) parallelCopy(ctx context.Context, path string, signed *davclient.SignedURL,
	memCache *segCache, disk *cacheFile, start, length, chunk, fileSize int64, w io.Writer, tSigned time.Duration) error {

	type seg struct {
		off, size int64
		idx       int
		hit       bool // 命中缓存（内存或磁盘）
		disk      bool // 命中磁盘（需从盘读）
		cacheable bool // 覆盖完整 chunk 段（可入缓存）
	}

	// 切段 + 标记命中（内存优先，磁盘次之）
	// 每段不跨越 chunk 边界；首段限制 SmallChunk 快速出首帧
	segs := make([]seg, 0, length/chunk+1)
	off := start
	rem := length
	idx := 0
	memHits, diskHits := 0, 0
	first := true
	for rem > 0 {
		segOff := off - (off % chunk)
		startInChunk := off - segOff
		segLen := chunk
		if segLen > fileSize-segOff {
			segLen = fileSize - segOff // 末段不足 chunk
		}
		sz := segLen - startInChunk // 默认切到段尾
		if first && sz > s.FirstChunk {
			sz = s.FirstChunk // 首段只下 64KB：TLS 完成后初始拥塞窗口内一趟传完，TTFB 最小
		}
		if rem < sz {
			sz = rem
		}
		// 命中检查带长度校验：旧缓存段（1MB/512KB 边界）长度不足时不算命中，
		// 走网络重下，避免切片越界产生 nil 段或零数据
		md, memHit := memCache.get(segOff)
		if memHit && int64(len(md)) < segLen {
			memHit = false
		}
		isDiskHit := false
		if !memHit && disk != nil {
			isDiskHit = disk.has(segOff, segLen)
		}
		cacheable := startInChunk == 0 && sz == segLen // 覆盖完整段才能入缓存
		segs = append(segs, seg{off: off, size: sz, idx: idx, hit: memHit || isDiskHit,
			disk: isDiskHit, cacheable: cacheable})
		if memHit {
			memHits++
		} else if isDiskHit {
			diskHits++
		}
		off += sz
		rem -= sz
		idx++
		first = false
	}
	if memHits+diskHits > 0 {
		vlog("cache hits: mem=%d disk=%d / %d segs", memHits, diskHits, len(segs))
	}

	// 段结果：done 区分"未完成"和"完成但失败（data=nil）"，失败段跳过不堵塞 flush
	type segResult struct {
		data []byte
		done bool
	}
	taskCh := make(chan seg, s.Concurrency*2)
	buffers := make(map[int]segResult)
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	nextToFlush := 0
	totalSegs := len(segs)
	totalBytes := int64(0)
	cacheBytes := int64(0)
	t0 := time.Now()

	// ctx 取消时唤醒所有 cond 等待者（滑动窗口的 main / flush），防死等
	go func() {
		<-ctx.Done()
		mu.Lock()
		cond.Broadcast()
		mu.Unlock()
	}()

	// flush goroutine：按 idx 顺序消费，失败段（data=nil）跳过
	flushDone := make(chan error, 1)
	go func() {
		defer close(flushDone)
		for nextToFlush < totalSegs {
			mu.Lock()
			for !buffers[nextToFlush].done {
				if ctx.Err() != nil {
					mu.Unlock()
					flushDone <- ctx.Err()
					return
				}
				cond.Wait()
			}
			res := buffers[nextToFlush]
			delete(buffers, nextToFlush)
			nextToFlush++
			cond.Broadcast() // 唤醒滑动窗口的 main（flush 前进，允许派发新任务）
			mu.Unlock()

			if len(res.data) == 0 {
				continue // 失败/跳过段
			}
			if _, err := w.Write(res.data); err != nil {
				flushDone <- err
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		flushDone <- nil
	}()

	// 命中段处理（内存优先，磁盘次之，直接填 buffers 秒响应）
	fillHit := func(sg seg) {
		segOff := sg.off - (sg.off % chunk)
		startInChunk := sg.off - segOff
		var data []byte
		if md, memHit := memCache.get(segOff); memHit && md != nil {
			data = md
		} else if sg.disk && disk != nil {
			d, err := disk.read(segOff, sg.size+startInChunk)
			if err == nil {
				data = d
			}
		}
		var buf []byte
		if data != nil && int64(startInChunk)+sg.size <= int64(len(data)) {
			buf = data[startInChunk : int64(startInChunk)+sg.size]
		} else {
			log.Printf("seg idx=%d off=%d hit-marked but data short, skip", sg.idx, sg.off)
		}
		mu.Lock()
		if buf != nil {
			cacheBytes += int64(len(buf))
			totalBytes += int64(len(buf))
		}
		buffers[sg.idx] = segResult{data: buf, done: true}
		cond.Signal()
		mu.Unlock()
	}

	// worker pool：只下载未命中段
	// 部分段（请求起点/终点落在 chunk 中间）下载整个对齐 chunk 入缓存、只交付切片——
	// 否则起点所在 chunk 永远不被缓存，播放器 +64KB 重连循环每次都走网络重下同一区域，
	// 慢网络下永远出不了画面（实测复现：重连 120s 无有效数据）
	var wg sync.WaitGroup
	for i := 0; i < s.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sg := range taskCh {
				if ctx.Err() != nil {
					return
				}
				if sg.hit {
					continue
				}
				var buf []byte
				var ok bool
				if sg.cacheable {
					buf, ok = s.downloadSegWithRetry(ctx, path, signed, sg.off, sg.size)
				} else {
					segOff := sg.off - (sg.off % chunk)
					segLen := chunk
					if segLen > fileSize-segOff {
						segLen = fileSize - segOff
					}
					full, ok2 := s.downloadSegWithRetry(ctx, path, signed, segOff, segLen)
					if ok2 {
						memCache.put(segOff, full)
						if disk != nil {
							disk.write(segOff, full)
						}
						startInChunk := sg.off - segOff
						if int64(startInChunk)+sg.size <= int64(len(full)) {
							buf = full[startInChunk : int64(startInChunk)+sg.size]
							ok = true
						}
						mu.Lock()
						cacheBytes += int64(segLen)
						mu.Unlock()
					}
				}
				if !ok {
					log.Printf("seg idx=%d off=%d failed after retries, skip", sg.idx, sg.off)
				}
				mu.Lock()
				buffers[sg.idx] = segResult{data: buf, done: true}
				if buf != nil {
					totalBytes += int64(len(buf))
				}
				cond.Signal()
				mu.Unlock()
				// cacheable 段入缓存（在 buffers 交付后，不阻塞 flush）
				if ok && sg.cacheable {
					memCache.put(sg.off, buf)
					if disk != nil {
						disk.write(sg.off, buf)
					}
					mu.Lock()
					cacheBytes += int64(len(buf))
					mu.Unlock()
				}
			}
		}()
	}

	// 主 goroutine：滑动窗口调度
	//   - 任务（命中填充/下载）最多领先 flush 位置 window 段，防止两种伤害：
	//     1) cancel-prone 的探测请求（播放器整文件 GET 秒取消）预下载远处数据浪费带宽
	//     2) 大量命中数据囤进 buffers 占内存
	//   - 带宽永远集中在客户端即将读取的位置（修拖动后掉帧：连接池不再被垃圾下载抢占）
	//   1. 首段（未命中时）同步下载独享带宽——最快出首帧
	//   2. 命中段直接填 buffers
	//   3. 未命中段送 taskCh 给 worker
	const window = 16 // 16×2MB = 32MB 读 ahead 上限
	for _, sg := range segs {
		if ctx.Err() != nil {
			break
		}
		mu.Lock()
		for sg.idx-nextToFlush >= window {
			if ctx.Err() != nil {
				mu.Unlock()
				goto drain
			}
			cond.Wait()
		}
		mu.Unlock()
		if ctx.Err() != nil {
			break
		}
		if sg.hit {
			fillHit(sg)
			continue
		}
		if sg.idx == 0 {
			// 首段同步下载：独享全部带宽，64KB 快速出首帧，
			// 之后再放开 8 worker 并发（避免首段被 8 路并发摊薄带宽）
			tf := time.Now()
			buf, ok := s.downloadSegWithRetry(ctx, path, signed, sg.off, sg.size)
			vlog("first-seg %d bytes in %dms (signed=%dms, hit=%v)", sg.size,
				time.Since(tf).Milliseconds(), tSigned.Milliseconds(), sg.hit)
			if !ok {
				log.Printf("first seg off=%d failed after retries, skip", sg.off)
			}
			mu.Lock()
			if ok && sg.cacheable {
				cacheBytes += int64(len(buf))
			}
			buffers[sg.idx] = segResult{data: buf, done: true}
			if buf != nil {
				totalBytes += int64(len(buf))
			}
			cond.Signal()
			mu.Unlock()
			if ok && sg.cacheable {
				segOff := sg.off - (sg.off % chunk)
				memCache.put(segOff, buf)
				if disk != nil {
					disk.write(segOff, buf)
				}
			}
			continue
		}
		select {
		case taskCh <- sg:
		case <-ctx.Done():
			goto drain
		}
	}
drain:
	close(taskCh)
	wg.Wait()

	// 唤醒可能还在等"永不 done 的段"的 flush（ctx cancel 时它检查 ctx.Err() 后退出）
	mu.Lock()
	cond.Broadcast()
	mu.Unlock()

	err := <-flushDone
	elapsed := time.Since(t0).Seconds()
	if elapsed > 0 && totalBytes > 0 {
		speed := float64(totalBytes) / 1024 / 1024 / elapsed
		s.speedLog.Add(speed)
		vlog("done: %d bytes (cache %d) / %.1fs = %.2f MB/s", totalBytes, cacheBytes, elapsed, speed)
		// 速度自适应换节点：延迟后仅当该文件已空闲（播放结束/暂停很久）才评估，
		// bench 下载（4MB×2）绝不与正在播放的请求抢带宽
		s.evaluator.EvaluateIfSlow(path, speed, 4.0, func() bool {
			s.touchMu.Lock()
			t := s.touched[path]
			s.touchMu.Unlock()
			return time.Since(t) > 30*time.Second
		})
	}
	// cancel 后触发后台预读：从已交付位置继续预读入缓存
	// cancel 后预读不停，重连命中数据已就绪
	if err != nil && isCancelErr(err) && totalBytes > 0 {
		from := start + totalBytes
		vlog("client canceled, start background pread from %d", from)
		s.startPread(memCache, disk, path, signed, from, chunk, fileSize)
	}
	return err
}

// isCancelErr 判断是否 cancel 类错误（context.Canceled / 连接被关）
func isCancelErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "canceled") || strings.Contains(s, "forcibly closed")
}

// startPread 启动后台预读 goroutine：从 from 顺序预读入段缓存。
// 新预读会取消同文件的旧预读（32MB 窗口）。
// 延迟 1 秒启动：cancel 后立刻重连的场景由新请求接管（ServeFile 会 cancelPread），
// 避免播放器 seek 到远处时预读旧位置浪费带宽、抢占新请求连接。
func (s *ParallelStreamer) startPread(memCache *segCache, disk *cacheFile, path string, signed *davclient.SignedURL,
	from, chunk, fileSize int64) {
	const window = 32 * 1024 * 1024 // 32MB 预读窗口
	const conc = 2                  // 低并发，不抢主请求带宽
	const delay = 1 * time.Second   // 延迟启动，躲过 cancel 重连高峰

	ctx, cancel := context.WithCancel(context.Background())
	memCache.setPread(cancel)

	go func() {
		defer cancel()
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return // 重连的新请求已接管，本预读取消
		}
		// 对齐到 chunk 边界：预读段必须与请求切段边界一致才能被命中
		from = from - (from % chunk)
		var wg sync.WaitGroup
		sem := make(chan struct{}, conc)
		end := from + window
		if end > fileSize {
			end = fileSize
		}
		for off := from; off < end; off += chunk {
			if ctx.Err() != nil {
				return
			}
			size := chunk
			if off+size > fileSize {
				size = fileSize - off
			}
			if _, hit := memCache.get(off); hit {
				continue
			}
			if disk != nil && disk.has(off, size) {
				continue // 磁盘已有跳过
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			wg.Add(1)
			go func(o, sz int64) {
				defer wg.Done()
				defer func() { <-sem }()
				buf, ok := s.downloadSegWithRetry(ctx, path, signed, o, sz)
				if ok {
					memCache.put(o, buf)
					if disk != nil {
						disk.write(o, buf)
					}
				}
			}(off, size)
		}
		wg.Wait()
		vlog("background pread done for %s from %d", path, from)
	}()
}

// downloadSegWithRetry 下载单个段，404 时刷新签名 URL 重试，最多 3 次。
// 404 时刷新签名 URL 重试（签名过期自愈）。
func (s *ParallelStreamer) downloadSegWithRetry(ctx context.Context, path string,
	signed *davclient.SignedURL, off, size int64) ([]byte, bool) {

	const maxRetry = 3
	cur := signed
	for attempt := 0; attempt < maxRetry; attempt++ {
		if ctx.Err() != nil {
			return nil, false
		}
		resp, err := s.dav.RangeGet(cur, off, size)
		if err != nil {
			vlog("range get err off=%d attempt=%d: %v", off, attempt, err)
			cur = s.refreshSigned(path, cur)
			continue
		}
		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			vlog("404 off=%d attempt=%d, refreshing signed url", off, attempt)
			cur = s.refreshSigned(path, cur)
			continue
		}
		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			resp.Body.Close()
			log.Printf("416 off=%d, abandon seg", off)
			return nil, false
		}
		if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			log.Printf("unexpected status %d off=%d, abandon seg", resp.StatusCode, off)
			return nil, false
		}
		buf := make([]byte, size)
		_, err = io.ReadFull(resp.Body, buf)
		resp.Body.Close()
		if err != nil {
			vlog("read err off=%d attempt=%d: %v", off, attempt, err)
			cur = s.refreshSigned(path, cur)
			continue
		}
		return buf, true
	}
	return nil, false
}

// refreshSigned 刷新签名 URL，失败则返回原签名。
func (s *ParallelStreamer) refreshSigned(path string, old *davclient.SignedURL) *davclient.SignedURL {
	s.dav.InvalidateSigned(path)
	newSigned, err := s.evaluator.PickBestNode(path, 1)
	if err != nil {
		log.Printf("refresh signed failed for %s: %v, reuse old", path, err)
		return old
	}
	return newSigned
}

// parseRange 解析 HTTP Range 头
func parseRange(rangeHdr string, fileSize int64) (start, end int64, hasRange bool) {
	if rangeHdr == "" {
		return 0, fileSize - 1, false
	}
	const prefix = "bytes="
	if len(rangeHdr) < len(prefix) || rangeHdr[:len(prefix)] != prefix {
		return 0, fileSize - 1, false
	}
	spec := rangeHdr[len(prefix):]
	dash := -1
	for i := 0; i < len(spec); i++ {
		if spec[i] == '-' {
			dash = i
			break
		}
	}
	if dash < 0 {
		return 0, fileSize - 1, false
	}
	left := spec[:dash]
	right := spec[dash+1:]
	if left == "" {
		var n int64
		fmt.Sscanf(right, "%d", &n)
		if n > fileSize {
			n = fileSize
		}
		return fileSize - n, fileSize - 1, true
	}
	fmt.Sscanf(left, "%d", &start)
	if right == "" {
		return start, fileSize - 1, true
	}
	fmt.Sscanf(right, "%d", &end)
	if end >= fileSize {
		end = fileSize - 1
	}
	return start, end, true
}
