package davclient

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// smartDNS 以系统 DNS 为主的自定义解析器，结果缓存 10 分钟。
//
// 背景：Go 纯 Go 解析器在 linux 上只认 /etc/resolv.conf。Termux/proot 环境
// 里 resolv.conf 实际位于 /usr/etc/resolv.conf（$PREFIX 下）或其他非标准
// 路径，Go 找不到就退回连 127.0.0.1:53（容器内无本地 DNS，必然 refused）。
//
// 策略（对齐老项目 pikpak-proxy 验证过的方案）：
//  1. 按优先级探测多个 resolv.conf 候选路径，收集真实 nameserver
//  2. 用 net.Resolver{PreferGo: true} + 自定义 Dial 直接查这些 nameserver
//     ——纯 Go 实现，无需 CGO，交叉编译友好
//  3. 标准 /etc/resolv.conf 存在时优先走 Go 默认系统解析（cgo 缓存等收益）
//  4. 一个都找不到时退回内置国内 DNS（223.5.5.5 / 119.29.29.29）并标注来源
type smartDNS struct {
	mu     sync.Mutex
	cache  map[string]dnsEntry
	ttl    time.Duration
	note   string        // 启动横幅来源说明
	dial   func(ctx context.Context, network, addr string) (net.Conn, error)
	custom *net.Resolver // 非 nil 时所有解析都走它（termux/内置场景）
}

type dnsEntry struct {
	ips []net.IP
	exp time.Time
}

// resolvConfCandidates 按优先级列出可能存放 resolv.conf 的路径：
// 标准 Linux / 安卓系统 / Termux($PREFIX)。
var resolvConfCandidates = []string{
	"/etc/resolv.conf",
	"/system/etc/resolv.conf",
	"/data/data/com.termux/files/usr/etc/resolv.conf", // $PREFIX/etc
	"/usr/etc/resolv.conf",
}

// builtinDNS 找不到任何 resolv.conf 时的兜底（国内公共 DNS，避免污染严重的 8.8.8.8）
var builtinDNS = []string{"223.5.5.5", "119.29.29.29"}

// readResolvConfNameservers 探测候选路径，返回去重的 nameserver 列表
// 和实际读到的源路径（空表示一个都没找到）。
func readResolvConfNameservers() ([]string, string) {
	var out []string
	seen := make(map[string]bool)
	src := ""
	for _, path := range resolvConfCandidates {
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if src == "" {
			src = path
		}
		for _, line := range strings.Split(string(b), "\n") {
			if i := strings.IndexByte(line, '#'); i >= 0 {
				line = line[:i]
			}
			f := strings.Fields(line)
			if len(f) >= 2 && f[0] == "nameserver" {
				ip := strings.TrimSpace(f[1])
				if net.ParseIP(ip) != nil && !seen[ip] {
					seen[ip] = true
					out = append(out, ip)
				}
			}
		}
	}
	return out, src
}

// newSmartDNS explicit 非空时用用户指定的 DNS 服务器（-dns 参数）。
func newSmartDNS(explicit []string) *smartDNS {
	d := &smartDNS{cache: make(map[string]dnsEntry), ttl: 10 * time.Minute}
	ns, src := readResolvConfNameservers()

	var servers []string
	switch {
	case len(explicit) > 0:
		servers = explicit
		d.note = "系统解析优先，否则用指定服务器"
	case len(ns) > 0:
		servers = ns
		d.note = "resolv.conf(" + src + ")"
	default:
		servers = builtinDNS
		d.note = "未找到 resolv.conf，已用内置国内 DNS"
	}

	if src == "/etc/resolv.conf" && len(explicit) == 0 {
		// 标准路径存在：Go 默认解析器自己就能读到，走系统解析即可
		d.note = "仅系统解析"
		return d
	}

	// 非标准环境（Termux/proot/内置兜底）：所有解析走自定义 resolver
	srv := append([]string(nil), servers...)
	d.custom = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			var dd net.Dialer
			var lastErr error
			for _, s := range srv {
				if !strings.Contains(s, ":") {
					s += ":53"
				}
				conn, err := dd.DialContext(ctx, "udp", s)
				if err == nil {
					return conn, nil
				}
				lastErr = err
			}
			return nil, lastErr
		},
	}
	d.dial = d.customDial
	return d
}

// lookup 解析 host，带 TTL 缓存。
func (d *smartDNS) lookup(ctx context.Context, host string) ([]net.IP, error) {
	d.mu.Lock()
	if e, ok := d.cache[host]; ok && time.Now().Before(e.exp) {
		d.mu.Unlock()
		return e.ips, nil
	}
	d.mu.Unlock()

	var addrs []net.IPAddr
	var err error
	if d.custom != nil {
		addrs, err = d.custom.LookupIPAddr(ctx, host)
	} else {
		// 标准环境：先系统解析，失败退自定义服务器（若指定了 -dns）
		addrs, err = net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil && d.dial != nil {
			addrs, err = d.custom.LookupIPAddr(ctx, host)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("DNS 解析 %s 失败(%v)，可用 -dns 指定服务器", host, err)
	}
	ips := make([]net.IP, 0, len(addrs))
	for _, a := range addrs {
		ips = append(ips, a.IP)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no addresses for %s", host)
	}
	d.mu.Lock()
	d.cache[host] = dnsEntry{ips: ips, exp: time.Now().Add(d.ttl)}
	d.mu.Unlock()
	return ips, nil
}

// customDial 拨号回调：把域名替换为已解析的 IP（逐个尝试），
// 挂到 http.Transport 的 DialContext 上，接管全部出站连接的 DNS。
func (d *smartDNS) customDial(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if net.ParseIP(host) != nil {
		var dd net.Dialer
		return dd.DialContext(ctx, network, addr)
	}
	ips, err := d.lookup(ctx, host)
	if err != nil {
		return nil, err
	}
	var dd net.Dialer
	var lastErr error
	for _, ip := range ips {
		conn, err := dd.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// dialContext 返回挂到 http.Transport 的拨号函数。
// 标准环境（Go 自己能读到 /etc/resolv.conf）返回 nil——不接管，走默认。
func (d *smartDNS) dialContext() func(ctx context.Context, network, addr string) (net.Conn, error) {
	return d.dial
}

// Note 返回 DNS 来源说明（启动横幅用）。
func (d *smartDNS) Note() string {
	return d.note
}
