package server

import (
	"crypto/subtle"
	"encoding/xml"
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

// Server 对外暴露 WebDAV Server 给 4xVR / PotPlayer / Kodi 等播放器消费。
// 内部把请求路由到 davclient → 签名 URL → dl- 节点并行下载。
type Server struct {
	dav     *davclient.Client
	stream  *ParallelStreamer

	// 对外鉴权（authUser 空 = 匿名模式，任意账号密码可连，适合本机/局域网）
	authUser string
	authPass string

	// 目录缓存：path -> entries + 失效时间
	dirMu  sync.RWMutex
	dirCache map[string]*dirCacheEntry
	DirTTL  time.Duration

	// 文件元数据缓存：path -> size + 失效时间
	fileMu   sync.RWMutex
	fileCache map[string]*fileCacheEntry
}

// Verbose 详细日志开关（-v 参数开启）。
// 默认只输出启动横幅和错误；开启后追加请求行/节点调度/耗时统计等信息。
var Verbose bool

// vlog 信息级日志（仅 -v 开启时输出）
func vlog(format string, args ...any) {
	if Verbose {
		log.Printf(format, args...)
	}
}

type dirCacheEntry struct {
	entries []davclient.DavEntry
	at      time.Time
}

type fileCacheEntry struct {
	size int64
	at   time.Time
}

func New(dav *davclient.Client, cacheDir string, authUser, authPass string, concurrency int) *Server {
	s := &Server{
		dav:       dav,
		stream:    NewStreamer(dav, cacheDir, concurrency),
		authUser:  authUser,
		authPass:  authPass,
		dirCache:  make(map[string]*dirCacheEntry),
		fileCache: make(map[string]*fileCacheEntry),
		DirTTL:    5 * time.Minute,
	}
	return s
}

// Handler 返回 http.Handler 处理所有 WebDAV 请求。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handle)
	return s.withAuth(mux)
}

// withAuth 对外鉴权中间件。
// authUser 为空 = 匿名模式（任意请求放行，适合本机/局域网信任环境）；
// 设置了 authUser 则要求 Basic Auth 匹配（constant-time 比较防时序侧信道）。
func (s *Server) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.authUser != "" {
			u, p, ok := r.BasicAuth()
			uOK := subtle.ConstantTimeCompare([]byte(u), []byte(s.authUser)) == 1
			pOK := subtle.ConstantTimeCompare([]byte(p), []byte(s.authPass)) == 1
			if !ok || !uOK || !pOK {
				log.Printf("401 rejected: %s %s (no/bad credentials)", r.Method, r.URL.Path)
				w.Header().Set("WWW-Authenticate", `Basic realm="pikpak-proxy"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	// WebDAV 路径 = URL path，需要 URL 解码
	rawPath := r.URL.Path
	decodedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		http.Error(w, "bad path", 400)
		return
	}
	// 注意：dav.mypikpak.com 的 href 里 + 是字面字符（HDR10+）
	// url.PathUnescape 不会把 %2B 当 + 处理，但 + 字符不会被改动
	vlog("%s %s", r.Method, decodedPath)

	switch r.Method {
	case "OPTIONS":
		s.handleOptions(w, r)
	case "PROPFIND":
		s.handlePropfind(w, r, decodedPath)
	case "GET", "HEAD":
		s.handleGet(w, r, decodedPath)
	default:
		// PUT/MKCOL/DELETE 等暂不支持（PikPak WebDAV 也只读）
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleOptions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("DAV", "1, 2")
	w.Header().Set("Allow", "OPTIONS, PROPFIND, GET, HEAD")
	w.Header().Set("MS-Author-Via", "DAV")
	w.WriteHeader(http.StatusOK)
}

// PROPFIND 转发到 dav client，缓存结果
func (s *Server) handlePropfind(w http.ResponseWriter, r *http.Request, path string) {
	depth := 1
	if d := r.Header.Get("Depth"); d == "0" {
		depth = 0
	}

	// 查缓存
	if depth == 1 {
		s.dirMu.RLock()
		if c, ok := s.dirCache[path]; ok && time.Since(c.at) < s.DirTTL {
			s.dirMu.RUnlock()
			s.writeMultistatus(w, path, c.entries)
			return
		}
		s.dirMu.RUnlock()
	} else {
		// Depth:0 单文件查询：PikPak 不支持（404），先查本地元数据缓存
		if e, ok := s.lookupFileMeta(path); ok {
			s.writeMultistatus(w, path, []davclient.DavEntry{e})
			return
		}
		// 目录的 Depth:0 也可能查父目录缓存命中
		s.dirMu.RLock()
		if c, ok := s.dirCache[path]; ok && time.Since(c.at) < s.DirTTL && len(c.entries) > 0 && c.entries[0].IsDir {
			s.dirMu.RUnlock()
			s.writeMultistatus(w, path, c.entries[:1])
			return
		}
		s.dirMu.RUnlock()
	}

	entries, err := s.dav.Propfind(path, depth)
	if err != nil {
		log.Printf("PROPFIND err: %v", err)
		http.Error(w, "upstream error: "+err.Error(), 502)
		return
	}

	// 缓存
	if depth == 1 {
		s.dirMu.Lock()
		s.dirCache[path] = &dirCacheEntry{entries: entries, at: time.Now()}
		s.dirMu.Unlock()
		// 文件大小入元数据缓存：后续 GET/HEAD 免 302（首开 TTFB 关键）
		s.storeFileMeta(entries)
		// 后台预热：签名 URL + 节点热连接（用户浏览列表的窗口期做完，
		// 点击视频时首段直接跑热连接）
		go s.stream.Prewarm(entries)
	}

	s.writeMultistatus(w, path, entries)
}

// storeFileMeta 把 PROPFIND 结果里的文件大小写入 fileCache
func (s *Server) storeFileMeta(entries []davclient.DavEntry) {
	s.fileMu.Lock()
	defer s.fileMu.Unlock()
	for _, e := range entries {
		if e.IsDir || e.Size <= 0 {
			continue
		}
		// Href 是 URL 编码的路径，统一解码后作 key（与 GET 的 decodedPath 对齐）
		if p, err := url.PathUnescape(e.Href); err == nil {
			s.fileCache[p] = &fileCacheEntry{size: e.Size, at: time.Now()}
		}
	}
}

// lookupFileMeta 查文件元数据缓存（先查 fileCache，再扫目录缓存）
func (s *Server) lookupFileMeta(path string) (davclient.DavEntry, bool) {
	now := time.Now()
	s.fileMu.RLock()
	if c, ok := s.fileCache[path]; ok && now.Sub(c.at) < s.DirTTL {
		s.fileMu.RUnlock()
		name := pathBase(path)
		return davclient.DavEntry{Href: path, Name: name, Size: c.size, IsDir: false}, true
	}
	s.fileMu.RUnlock()

	// fileCache 没有就扫父目录缓存（key 可能带/不带尾斜杠，两种都试）
	dir := parentDir(path)
	s.dirMu.RLock()
	defer s.dirMu.RUnlock()
	var c *dirCacheEntry
	if x, ok := s.dirCache[dir]; ok {
		c = x
	} else if x, ok := s.dirCache[dir+"/"]; ok {
		c = x
	}
	if c == nil || now.Sub(c.at) >= s.DirTTL {
		return davclient.DavEntry{}, false
	}
	for _, e := range c.entries {
		if e.IsDir {
			continue
		}
		if p, err := url.PathUnescape(e.Href); err == nil && p == path {
			return davclient.DavEntry{Href: e.Href, Name: e.Name, Size: e.Size, IsDir: false}, true
		}
	}
	return davclient.DavEntry{}, false
}

// parentDir 取父目录路径（不带尾斜杠，与 PROPFIND 请求路径形式对齐）
func parentDir(p string) string {
	i := lastSlash(p)
	if i <= 0 {
		return "/"
	}
	return p[:i]
}

func lastSlash(p string) int {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == '\\' {
			return i
		}
	}
	return -1
}

// writeMultistatus 把 entries 序列化成 WebDAV multistatus XML 返回
func (s *Server) writeMultistatus(w http.ResponseWriter, basePath string, entries []davclient.DavEntry) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("DAV", "1, 2")
	w.WriteHeader(http.StatusMultiStatus)

	io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>`)
	io.WriteString(w, `<D:multistatus xmlns:D="DAV:">`)
	for _, e := range entries {
		io.WriteString(w, `<D:response>`)
		io.WriteString(w, `<D:href>`+xmlEscape(e.Href)+`</D:href>`)
		io.WriteString(w, `<D:propstat><D:prop>`)
		if e.IsDir {
			io.WriteString(w, `<D:resourcetype><D:collection/></D:resourcetype>`)
		} else {
			io.WriteString(w, `<D:resourcetype/>`)
		}
		io.WriteString(w, `<D:displayname>`+xmlEscape(e.Name)+`</D:displayname>`)
		io.WriteString(w, `<D:getcontentlength>`+fmt.Sprintf("%d", e.Size)+`</D:getcontentlength>`)
		if e.Modified != "" {
			io.WriteString(w, `<D:getlastmodified>`+e.Modified+`</D:getlastmodified>`)
		}
		if e.ETag != "" {
			io.WriteString(w, `<D:getetag>`+e.ETag+`</D:getetag>`)
		}
		io.WriteString(w, `</D:prop>`)
		io.WriteString(w, `<D:status>HTTP/1.1 200 OK</D:status>`)
		io.WriteString(w, `</D:propstat></D:response>`)
	}
	io.WriteString(w, `</D:multistatus>`)
}

func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}

// GET：拿签名 URL + 节点评估 + 并行流式下载
// HEAD：只返回元数据头（Content-Length/Accept-Ranges），绝不触发下载——
// 之前 HEAD 走完整 ServeFile，parallelCopy 会真的下载整个文件（Go 丢弃 body
// 但下载照跑），8 worker 抢光带宽，紧接着的 GET 被堵死 → 播放器首开卡死。
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, path string) {
	size, err := s.getFileSize(path)
	if err != nil {
		log.Printf("get file size err: %v", err)
		s.writeUpstreamErr(w, err)
		return
	}

	// 调用 streamer 并行下载
	if err := s.stream.ServeFile(w, r, path, size); err != nil {
		log.Printf("stream err: %v", err)
	}
}

// writeUpstreamErr 上游错误分类响应：网关熔断返回 503 + Retry-After
// （播放器语义化稍后重试），其余错误维持 502。
func (s *Server) writeUpstreamErr(w http.ResponseWriter, err error) {
	if strings.Contains(err.Error(), "rate-limited") {
		retry := 30
		if d := s.dav.CooldownRemaining(); d > 0 {
			retry = int(d.Seconds()) + 1
		}
		w.Header().Set("Retry-After", fmt.Sprintf("%d", retry))
		http.Error(w, "gateway rate-limited, retry later", http.StatusServiceUnavailable)
		return
	}
	http.Error(w, "size lookup: "+err.Error(), http.StatusBadGateway)
}

// getFileSize 拿文件大小：优先元数据缓存（PROPFIND 时已存，免 302 网络往返），
// miss 才走签名 URL 的 f= 参数。
func (s *Server) getFileSize(path string) (int64, error) {
	if e, ok := s.lookupFileMeta(path); ok {
		return e.Size, nil
	}
	return s.getFileSizeFromSignedURL(path)
}

// getFileSizeFromSignedURL 从 dav 拿签名 URL（顺带触发 302），解析 Location 的 f= 参数。
// 这个签名 URL 同时被 streamer 复用（缓存命中时直接复用，不会触发新 302）。
func (s *Server) getFileSizeFromSignedURL(path string) (int64, error) {
	signed, err := s.dav.GetSignedURL(path)
	if err != nil {
		return 0, err
	}
	// 签名 URL 里 f= 参数是文件大小
	u, err := url.Parse(signed.URL)
	if err != nil {
		return 0, err
	}
	f := u.Query().Get("f")
	if f == "" {
		return 0, fmt.Errorf("no f= in signed URL: %s", signed.URL[:60])
	}
	var size int64
	fmt.Sscanf(f, "%d", &size)
	if size <= 0 {
		return 0, fmt.Errorf("invalid size: %s", f)
	}
	return size, nil
}

// ListenAndServe 启动 HTTP/WebDAV Server
func (s *Server) ListenAndServe(addr string) error {
	log.Printf("pikpak-proxy listening on %s", addr)
	return http.ListenAndServe(addr, s.Handler())
}

// xmlEscape 不直接用 xml.EscapeString 是为了避免它的换行问题，手写一个最小实现
var _ = xml.Header
