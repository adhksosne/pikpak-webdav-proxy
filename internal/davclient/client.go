package davclient

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Client 是 PikPak WebDAV 客户端，封装了 PROPFIND / GET / 302 处理 + 连接池。
type Client struct {
	BaseURL string // 形如 https://dav.mypikpak.com
	User    string
	Pass    string
	http    *http.Client

	// 302 拿到的签名 URL：相对路径 -> *SignedURL。
	// 单 goroutine 刷新避免账号级 503 限流。
	signedMu sync.Mutex
	signed   map[string]*SignedURL

	// 网关限流熔断：空 200 时标记账号级冷却截止时间（unix nano，atomic）。
	// 冷却期内所有新签名请求直接拒绝——否则每个请求独立重试 5 次，
	// 每次重试又是新 302 请求，反过来加重限流（重试风暴，实测踩过）。
	cooldown int64

	dns *smartDNS // DNS 来源说明（Note）
	certNote string // 根证书来源说明（启动横幅用）
}

// CertNote 返回根证书来源说明（启动横幅用）。
func (c *Client) CertNote() string {
	return c.certNote
}

// DNSNote 返回 DNS 来源说明（启动横幅用）。
func (c *Client) DNSNote() string {
	return c.dns.note
}

// CooldownRemaining 返回网关限流冷却剩余时长（0 = 未限流）。
// 预热等低优先级后台任务据此让路，限流期间不发任何 302 请求。
func (c *Client) CooldownRemaining() time.Duration {
	if until := atomic.LoadInt64(&c.cooldown); until > 0 {
		if d := time.Until(time.Unix(0, until)); d > 0 {
			return d
		}
	}
	return 0
}

// SignedURL 是从 dav 拿到的 302 Location（指向 dl-*.mypikpak.net 的签名下载 URL）。
type SignedURL struct {
	URL     string    // 完整签名 URL
	Node    string    // dl-z01a-0054 之类的节点名
	Ms      int64     // 服务端给的限速参数（bits/s）
	Th      int64     // 同上
	Expiry  time.Time // URL 失效时间（按 expire 参数 + 余量）
	Obtained time.Time
}

// browserUA 用主流浏览器 UA，避免自标识 UA 触发 CDN 节点区别对待（降速）。
// Chrome 系 UA 实测吞吐更稳。
const browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"

// Verbose 详细日志开关（-v 参数开启，由 main 设置）
var Verbose bool

// New 创建一个 WebDAV 客户端，使用持久连接池。
// poolSize = 同一节点最多维持多少条持久连接（实测 6-8 最优）。
// dnsServers 非空时用指定 DNS 服务器（Termux 等非标准环境友好，详见 smartDNS）。
func New(baseURL, user, pass string, poolSize int, dnsServers []string) *Client {
	dns := newSmartDNS(dnsServers)
	tlsCfg, certNote := loadRootCAs() // Termux/安卓下系统池为空，须内置根证书兜底
	t := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: poolSize,
		MaxConnsPerHost:     poolSize,
		IdleConnTimeout:     90 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		DisableCompression:  true, // 视频已压缩，关 gzip
		ForceAttemptHTTP2:   false, // PikPak 不支持 HTTP/2
		DialContext:         dns.dialContext(), // Termux/proot 下 Go 读不到 resolv.conf，自定义解析接管
		TLSClientConfig:     tlsCfg,             // Termux/proot 下 Go 读不到 /etc/ssl，根证书须自备
	}
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		User:    user,
		Pass:    pass,
		http: &http.Client{
			Transport: t,
			// 不自动跟随重定向：我们要拦下 302 看签名 URL。
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Timeout: 0, // 流式下载不设全局超时
		},
		signed:   make(map[string]*SignedURL),
		dns:      dns,
		certNote: certNote,
	}
}

// Propfind 发 PROPFIND Depth:1 列目录（或者 Depth:0 查单文件）。
// 返回 PROPFIND XML 解析后的 entries。
func (c *Client) Propfind(path string, depth int) ([]DavEntry, error) {
	u := c.BaseURL + path
	req, err := http.NewRequest("PROPFIND", u, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.User, c.Pass)
	req.Header.Set("Depth", fmt.Sprintf("%d", depth))
	req.Header.Set("User-Agent", browserUA)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 207 {
		return nil, fmt.Errorf("PROPFIND %s: %d %s", path, resp.StatusCode, string(body[:min(len(body), 200)]))
	}
	return parseMultistatus(body)
}

// GetSignedURL 拿一次 302，解析出签名 URL 和参数。
// 如果缓存里还有有效的签名 URL 就直接返回缓存。
// 网关偶发对 Range 请求直接回 200（不走 302），间隔重试即可拿到 302。
func (c *Client) GetSignedURL(path string) (*SignedURL, error) {
	c.signedMu.Lock()
	defer c.signedMu.Unlock()

	// 命中缓存（留 10 分钟余量）
	if s, ok := c.signed[path]; ok && time.Now().Before(s.Expiry.Add(-10*time.Minute)) {
		return s, nil
	}

	var lastErr error
	// 退避 0/1/2/4/8s：网关对该账号有活跃下载流时会熔断新签名请求（空 200），
	// 典型场景是同账号的另一个客户端正在预读。总窗口 15 秒扛过常规预读。
	backoff := []time.Duration{0, 1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		// 全局熔断冷却检查：账号已被网关限流（空 200）时停止一切新 302 请求，
		// 重试只会延长限流窗口。首个撞限流的请求已标记 cooldown。
		if until := atomic.LoadInt64(&c.cooldown); time.Now().UnixNano() < until {
			return nil, fmt.Errorf("gateway rate-limited, cooling down until %s",
				time.Unix(0, until).Format("15:04:05"))
		}
		if backoff[attempt] > 0 {
			c.signedMu.Unlock()
			time.Sleep(backoff[attempt])
			c.signedMu.Lock()
			// 重试期间可能别的 goroutine 已拿到
			if s, ok := c.signed[path]; ok && time.Now().Before(s.Expiry.Add(-10*time.Minute)) {
				return s, nil
			}
		}

		s, err := c.fetchSignedOnce(path)
		if err != nil {
			lastErr = err
			continue
		}
		c.signed[path] = s
		return s, nil
	}
	return nil, lastErr
}

// fetchSignedOnce 发一次 Range GET 拦截 302，解析 Location。
func (c *Client) fetchSignedOnce(path string) (*SignedURL, error) {
	u := c.BaseURL + path
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.User, c.Pass)
	req.Header.Set("Range", "bytes=0-0") // 只拿 1 字节，触发 302 但不下数据
	req.Header.Set("User-Agent", browserUA)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 302 && resp.StatusCode != 301 {
		// 空 200 = 网关账号级限流信号：标记全局冷却 60s，
		// 期间所有签名请求秒拒绝（GetSignedURL 开头检查），避免重试风暴
		if resp.StatusCode == http.StatusOK && resp.ContentLength == 0 {
			atomic.StoreInt64(&c.cooldown, time.Now().Add(60*time.Second).UnixNano())
			log.Printf("gateway rate-limited (empty 200), account cooldown 60s")
		}
		// 取证：打印 200 降级响应的详情（headers + body 前缀），仅 -v 模式
		if Verbose {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
			log.Printf("fetchSignedOnce got %d for %s: content-type=%q content-range=%q content-length=%q body-prefix=%q",
				resp.StatusCode, path, resp.Header.Get("Content-Type"), resp.Header.Get("Content-Range"),
				resp.Header.Get("Content-Length"), string(body[:min(len(body), 120)]))
		}
		io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("expected 302, got %d for %s", resp.StatusCode, path)
	}
	io.Copy(io.Discard, resp.Body)

	loc := resp.Header.Get("Location")
	if loc == "" {
		return nil, fmt.Errorf("302 without Location for %s", path)
	}

	s := &SignedURL{
		URL:      loc,
		Obtained: time.Now(),
	}
	// 解析节点名（dl-z01a-0054.mypikpak.net -> dl-z01a-0054）
	parsed, err := url.Parse(loc)
	if err == nil {
		parts := strings.Split(parsed.Hostname(), ".")
		if len(parts) > 0 {
			s.Node = parts[0]
		}
	}
	// 解析 query 参数 ms / th / expire
	q := parsed.Query()
	s.Ms = parseInt64(q.Get("ms"))
	s.Th = parseInt64(q.Get("th"))
	expire := parseInt64(q.Get("expire"))
	if expire > 0 {
		// expire 是 unix 秒时间戳；保守留 70 分钟
		s.Expiry = time.Unix(expire, 0)
	} else {
		s.Expiry = time.Now().Add(70 * time.Minute)
	}
	return s, nil
}

// RangeGet 对签名 URL 发起 Range GET，返回响应体（流式）。
// 调用方负责关闭 body。复用 http.Client 的连接池。
// 注意：这个 client 不带 CheckRedirect，因为我们直接对 CDN 节点请求，不应该再被重定向。
func (c *Client) RangeGet(signed *SignedURL, start, length int64) (*http.Response, error) {
	req, err := http.NewRequest("GET", signed.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+length-1))
	req.Header.Set("User-Agent", browserUA)
	// 不带 Basic Auth，签名 URL 已包含认证
	return c.http.Do(req)
}

// InvalidateSigned 清除某 path 的缓存签名 URL。
// 用于 404 重试：节点失效后强制重新取 302 拿新签名。
func (c *Client) InvalidateSigned(path string) {
	c.signedMu.Lock()
	delete(c.signed, path)
	c.signedMu.Unlock()
}

// DavEntry 是 PROPFIND 单条 response。
type DavEntry struct {
	Href       string
	IsDir      bool
	Name       string
	Size       int64
	Modified   string
	ETag       string
	IsRoot     bool
}

// multistatus 是 PROPFIND XML 解析结构。
type multistatus struct {
	Responses []response `xml:"response"`
}

type response struct {
	Href     string  `xml:"href"`
	Propstat propstat `xml:"propstat"`
}

type propstat struct {
	Prop prop `xml:"prop"`
}

type prop struct {
	Displayname      string `xml:"displayname"`
	Getcontentlength string `xml:"getcontentlength"`
	Getlastmodified  string `xml:"getlastmodified"`
	Getetag          string `xml:"getetag"`
	Iscollection     string `xml:"iscollection"`
	Resourcetype     struct {
		Collection string `xml:"collection"` // 用 string 能区分"标签存在但空" vs "标签不存在"
	} `xml:"resourcetype"`
}

func parseMultistatus(body []byte) ([]DavEntry, error) {
	var m multistatus
	if err := xml.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	entries := make([]DavEntry, 0, len(m.Responses))
	for _, r := range m.Responses {
		isDir := r.Propstat.Prop.Iscollection == "true" || r.Propstat.Prop.Resourcetype.Collection != ""
		e := DavEntry{
			Href:     r.Href,
			Name:     r.Propstat.Prop.Displayname,
			Modified: r.Propstat.Prop.Getlastmodified,
			ETag:     r.Propstat.Prop.Getetag,
			Size:     parseInt64(r.Propstat.Prop.Getcontentlength),
			IsDir:    isDir,
		}
		entries = append(entries, e)
	}
	return entries, nil
}

func parseInt64(s string) int64 {
	var n int64
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			break
		}
		n = n*10 + int64(s[i]-'0')
	}
	return n
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
