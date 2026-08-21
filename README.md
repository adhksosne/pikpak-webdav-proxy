# PikPak WebDAV 加速代理

一个零依赖的轻量加速服务，解决中国大陆 PikPak 用户直连 WebDAV 的两大痛点：**下载慢**（单连接 2-5 MB/s）和**播放器拖动卡顿**（寻位后抽帧数秒甚至播放器退出）。

实测（4K WEB-DL 大文件，本地部署，与闭源 CloudDrive2 同账号同网络交叉对比）：

| 指标 | 本代理 | CloudDrive2 |
|------|--------|-------------|
| 顺序播放吞吐 | 10-30+ MB/s | 3-36 MB/s（受账号限速波动） |
| 拖动 TTFB | 400-850 ms | 480-3000 ms |
| 拖动后重连拿 32MB | 0.05 s（缓存命中） | ~2 s |
| 首开（预热命中） | ~1.3 s | ~2 s（冷连接） |
| 数据完整性 | SHA 与 CD2 全对齐 | 基线 |

## 原理

PikPak 的 API 和官方客户端对中国 IP 有严重限速（几百 KB/s），WebDAV 入口的速度好得多，但同样存在限制：**按连接限速**（单连接约 2-6 MB/s，直链签名里带 `ms=81600000` ≈ 10 MB/s 上限）、**账号级短时限速**（当天大流量后整体掉速，几小时恢复）、以及**高频 302 交换限流**（网关返回空 200 或 503）。

本代理的三层对策：

1. **单条 TCP 连接受拥塞控制（BDP 瓶颈）限制**，跨网延迟下通常只有 2-5 MB/s —— 多路并发把有效速率变成多条连接之和
2. **播放器拖动 = 高频"取消请求 + 重发请求"**，普通反向代理把客户端取消直接传播给上游，CDN 连接因响应体未读完而被拆毁，每次寻位都重付 DNS + TCP + TLS 握手（0.7~0.9s 起步），TTFB 飙到 2 秒级 —— 持久连接池 + 直链缓存 + 多级缓存规避
3. **限流与账号限速** —— 直链缓存 80 分钟从根源上不触发 302 限流；账号短时限速是服务端策略，所有方案（含 CloudDrive2）都会掉速，只能等待恢复

```
WebDAV 客户端 ──→ 代理 (:7777)
                    │  ① 根源探测 DNS（Termux/安卓友好）
                    │  ② 302 交换 → CDN 直链缓存（80 分钟）+ 节点择优
                    │  ③ 16 并发 × 2MB 段 + 64KB 首段 + 滑动窗口 → CDN
                    ▼
                1GB 内存块缓存 > 磁盘缓存（可选，sparse 稀疏文件）
```

非 Range 请求与 PROPFIND 等方法仍透明转发，WebDAV 目录浏览行为不变。

## 六项播放优化

### 1. 根源探测系统 DNS + 10 分钟缓存

**主动探测多个 resolv.conf 路径，读到哪个真机 DNS 就用哪个**，全程不向 8.8.8.8 之类固定公网 DNS 发查询。这是因为 Go 解析器在 linux 上只认固定路径 `/etc/resolv.conf`——Termux proot 容器里它实际在 `/usr/etc/resolv.conf`（即 `$PREFIX` 下），Go 找不到标准路径就退回连 `127.0.0.1:53`，而容器内没有本地 DNS 服务，导致 `connection refused`。

探测路径按优先级：`/etc/resolv.conf` → `/system/etc/resolv.conf`（安卓）→ `$PREFIX/etc/resolv.conf`（Termux）→ `/usr/etc/resolv.conf`。结果缓存 10 分钟，避免拖动频繁新建连接时重复解析。只有所有路径都找不到时才退回内置国内 DNS，并在启动横幅明确标注来源。

**TLS 根证书同理**：Go 的 x509 只认 `/etc/ssl/certs/` 等标准路径，安卓沙箱里没有 `/etc`——Termux 下 HTTPS 握手会报 `certificate signed by unknown authority`。处理策略与 DNS 一致：先读 Termux 自己的证书包（`$PREFIX/etc/tls/cert.pem`，`pkg install ca-certificates` 后存在），找不到就用**内置 Mozilla 根证书**（go:embed 编译进二进制，无运行时依赖）。启动横幅的「证书」行会标注实际来源。

### 2. CDN 直链缓存（80 分钟）+ DNS 节点枚举优选

PikPak WebDAV 对高频 302 交换有限流（网关会返回空 200 或 503）。本代理对每个文件只做一次 302 交换，拿到的 **CDN 直链（签名 URL）缓存 80 分钟反复使用**，并发 Range 请求全部复用同一条直链，从根源上不触发限流。

直链指向的 CDN 节点是随机分配的（"连接级抽奖"）。实测发现签名 URL 的 query 只绑定文件、不绑定节点——同一签名换到任意兄弟节点域名都有效。据此实现 **DNS 节点枚举优选**（参考 Cloudflare 优选 IP）：枚举 `dl-z01a-XXXX` 编号空间 → DNS 探活（节点存在才有 A 记录，实测存活约 26 个）→ 两阶段测速（512KB 初测筛 TTFB + 4MB 复测稳态吞吐）→ 全局锁定最快节点，所有文件的签名自动改写到该节点。每小时静默重扫；测速仅在空闲时进行，绝不与正在播放的请求抢带宽；全程零网关请求，302 熔断期间也能完成优选。

### 3. 64KB 首段快速出画 + 滑动窗口调度

请求首段只下 64KB：TLS 完成后初始拥塞窗口内一趟传完，TTFB 最小，播放器尽快解析 moov 出画面。首段独享全部带宽，之后才放开并发（`-c`，默认 16——CD2 交叉实测的甜点值：对比 8 有可见提升，32 无增益且偶发触发网关错误）。

**滑动窗口调度**是解决"拖动后抽帧卡顿"的关键：下载任务最多领先客户端读取位置 32MB，带宽永远集中在播放器即将读取的位置。播放器拖动时常见的"整文件探测请求"（发起即取消）不再触发远处的预下载浪费，实测探测请求开销归零、真实播放请求的连接不再被抢占。

### 4. 三级缓存（内存 1GB > 磁盘可选 > 网络）

- **内存按块缓存（1GB，TTL 30 分钟）**：重复拖同一位置直接内存命中，实测重连拿 32MB 仅 0.05s（约 640 MB/s 等效速度），LRU 淘汰
- **磁盘缓存（`-cachedir` 开启，默认关）**：已下载的段写入本地 sparse 稀疏文件（只占实际缓存的空间，8.7GB 电影看一半约占用几百 MB），跨会话持久化，重启进程后命中
- 请求起点落在块中间的部分段会下载整个对齐块入缓存——否则播放器"+64KB 重连循环"每次都走网络重下同一区域，慢网络下永远出不了画面（实测修复前重连 120s 无有效数据，修复后 0.05s）

### 5. Prewarm 预热（点击秒开）

播放器显示文件列表到用户点击视频之间有几秒窗口。代理利用这个窗口**后台预取直链 + 建立到 CDN 节点的热连接**（串行 + 间隔，避免触发限流；真实请求到达时自动让路）。用户点击视频时首段直接跑热连接，TTFB 只剩传输时间，实测首开从 ~2s 降到 ~1.3s。

### 6. HTTP/1.1 持久连接池

所有 CDN 请求走 `http.Transport` 持久连接池（`MaxIdleConnsPerHost` 按并发数配置），块请求总能完整读完响应体，连接温热归还连接池，供下一次拖动直接复用，不再重付握手成本。辅助优化：主流浏览器 UA（自标识 UA 可能触发 CDN 节点区别对待）；404/网络错误时刷新直链重试。

## 快速开始

### 下载预编译二进制

| 平台 | 文件 |
|------|------|
| Windows x64 | `pikpak-proxy-windows-amd64.exe` |
| Linux ARM64（Android 手机/Termux） | `pikpak-proxy-linux-arm64` |
| Linux x64（VPS/软路由） | `pikpak-proxy-linux-amd64` |
| Linux ARMv7（老设备） | `pikpak-proxy-linux-arm` |
| macOS ARM64（Apple Silicon） | `pikpak-proxy-darwin-arm64` |
| macOS x64（Intel） | `pikpak-proxy-darwin-amd64` |

### 运行

```bash
# Windows（匿名模式，适合本机或局域网内信任环境）
pikpak-proxy.exe -target https://dav.mypikpak.com -user 你的PikPak用户名 -pass 你的PikPak密码

# Windows（带认证，适合部署在 VPS 或公网可访问的环境）
pikpak-proxy.exe -target https://dav.mypikpak.com -user 你的PikPak用户名 -pass 你的PikPak密码 -auth-user proxy用户名 -auth-pass proxy密码

# 开启磁盘缓存（默认关闭；开启后已下载的段跨会话持久化）
pikpak-proxy.exe -target https://dav.mypikpak.com -user 你的PikPak用户名 -pass 你的PikPak密码 -cachedir ./cache

# Linux / Android (Termux)
chmod +x pikpak-proxy-linux-arm64
./pikpak-proxy-linux-arm64 -target https://dav.mypikpak.com -user 你的PikPak用户名 -pass 你的PikPak密码

# 若启动横幅显示"未找到 resolv.conf，已用内置国内 DNS"，说明所有候选路径都没读到，
# 可手动指定一个 DNS 服务器（如阿里 223.5.5.5）：
./pikpak-proxy-linux-arm64 -target https://dav.mypikpak.com -user 你的PikPak用户名 -pass 你的PikPak密码 -dns 223.5.5.5
```

### 获取 PikPak WebDAV 凭据

1. 打开 PikPak App
2. 进入 设置 → 实验室功能 → WebDAV设置
3. 开启 WebDAV，页面会显示用户名和密码

### 连接 WebDAV 客户端

| 代理启动模式 | 客户端用户名 | 客户端密码 |
|-------------|------------|-----------|
| 匿名模式（默认） | 无 | 无 |
| 认证模式（`-auth-user` / `-auth-pass`） | 填 `-auth-user` 的值 | 填 `-auth-pass` 的值 |

客户端地址填 `http://localhost:7777`（本机）或 `http://IP:7777`（局域网/VPS）。手机播放器连电脑代理时，确保两台设备在同一网段。

## 参数说明

```
Usage: pikpak-proxy [flags]

Flags:
  -listen string      本地监听地址（默认 :7777）
  -target string      PikPak WebDAV 地址（如 https://dav.mypikpak.com）
  -user string        PikPak WebDAV 用户名
  -pass string        PikPak WebDAV 密码
  -auth-user string   代理认证用户名，留空则匿名模式
  -auth-pass string   代理认证密码，留空则匿名模式
  -c int              单文件并行连接数（默认 16，CD2 交叉实测甜点值）
  -cachedir string    磁盘缓存目录，留空关闭（默认关，仅内存缓存）
  -dns string         兜底 DNS 服务器（逗号分隔，如 223.5.5.5），仅系统 DNS 不可用时使用
  -v                  打印详细请求日志
```

- **匿名模式**（默认）：不设 `-auth-user`，任何人连上代理即可使用。适合本机或可信局域网。
- **认证模式**：设置 `-auth-user` / `-auth-pass`，WebDAV 客户端必须提供这组凭据。适合 VPS 或公网部署。
- **详细日志**（`-v`）：默认只输出启动横幅和错误；`-v` 追加每个请求的节点调度/耗时/缓存命中信息，排查播放问题时使用。

## 从源码编译

需要 [Go 1.21+](https://go.dev/dl/)：

```bash
# 当前平台
go build -ldflags="-s -w" -o pikpak-proxy .

# 全平台交叉编译（build.sh）
bash build.sh

# 手动指定平台
CGO_ENABLED=0 GOOS=linux  GOARCH=amd64 go build -ldflags="-s -w" -o pikpak-proxy-linux-amd64 .
CGO_ENABLED=0 GOOS=linux  GOARCH=arm64 go build -ldflags="-s -w" -o pikpak-proxy-linux-arm64 .
CGO_ENABLED=0 GOOS=linux  GOARCH=arm   go build -ldflags="-s -w" -o pikpak-proxy-linux-arm .
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w" -o pikpak-proxy-darwin-arm64 .
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -o pikpak-proxy-darwin-amd64 .
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o pikpak-proxy-windows-amd64.exe .
```

## 故障排查

### 启动报 `bind: Only one usage of each socket address ... normally permitted`

7777 端口已被占用。通常是上一个代理实例还在运行，或者是其它程序占用了端口。

### 播放器拖动卡顿/抽帧

加 `-v` 看详细日志：`done: ... MB/s` 行是每个请求的实际吞吐；`first-seg ... in Nms` 是首段耗时。若速度持续低于 4K 码率（~3.3 MB/s），通常是 PikPak 账号被短时限速（当天大流量后触发，几小时恢复，所有方案包括 CD2 都会掉速）。

### 播放中途退出 / 503

PikPak WebDAV 对高频 302 交换有限流。当前版本已用直链缓存（80 分钟）规避；若仍出现，检查是否有多个代理实例同时运行，或同一账号在其他设备（如 CD2）频繁请求直链——网关对同账号活跃下载流会短暂熔断新直链请求。

### 磁盘缓存文件比预期大

缓存文件是 sparse 稀疏文件：资源管理器"大小"显示的是逻辑大小（等于电影大小），真实占用看属性里的"占用空间"（或资源管理器开启"占用空间"列），只有实际下载过的段占磁盘。

### 客户端 401

- 匿名模式：客户端不要填任何凭据
- 认证模式：客户端填 `-auth-user` / `-auth-pass` 的值（不是 PikPak 账号密码）

## 致谢

- 限速模型证据：[itzmx 论坛小樱对 PikPak 直链的实锤分析](https://bbs.itzmx.com/thread-98536-1-1.html)
- PikPak WebDAV 速度限制官方说明：[PikPak 官方帮助中心](https://mypikpak.com/zh-CN/help-center/play_and_download/webdav/speed_limit)

## License

[MIT](LICENSE)
