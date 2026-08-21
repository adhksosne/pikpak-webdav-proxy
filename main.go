// pikpak-webdav-proxy — PikPak WebDAV 加速代理
//
// 解决中国大陆 PikPak 用户直连 WebDAV 下载慢、播放器拖动卡顿的痛点：
// 1. CDN 直链缓存（一次 302，~80 分钟反复用）+ 节点择优（低速时后台换节点）
// 2. 64KB 首段快速出画 + 滑动窗口调度（带宽锚定播放位置）
// 3. HTTP/1.1 持久连接池（TTFB≈0）
// 4. 三级缓存（内存按块 / 磁盘可选 / 网络兜底）+ Prewarm 预热
//
// 用法（匿名模式，本机/局域网信任环境）：
//   pikpak-proxy -listen :7777 -target https://dav.mypikpak.com -user X -pass Y
//
// 认证模式（VPS/公网部署）：
//   pikpak-proxy -target https://dav.mypikpak.com -user X -pass Y -auth-user proxy用户 -auth-pass proxy密码
//
// 播放器连接 http://localhost:7777 即可（匿名模式任意账号密码可连）。
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"pikpak-webdav-proxy/internal/davclient"
	"pikpak-webdav-proxy/internal/server"
)

// version 版本号（发版时更新）
const version = "1.0"

func main() {
	listen := flag.String("listen", ":7777", "本地监听地址")
	target := flag.String("target", "https://dav.mypikpak.com", "PikPak WebDAV 地址")
	user := flag.String("user", "", "PikPak WebDAV 用户名")
	pass := flag.String("pass", "", "PikPak WebDAV 密码")
	authUser := flag.String("auth-user", "", "代理认证用户名，留空则匿名模式")
	authPass := flag.String("auth-pass", "", "代理认证密码，留空则匿名模式")
	c := flag.Int("c", 8, "单文件并行连接数")
	cacheDir := flag.String("cachedir", "", "磁盘缓存目录，留空关闭（默认关，仅内存缓存）")
	dns := flag.String("dns", "", "兜底 DNS 服务器（逗号分隔，如 223.5.5.5），仅系统 DNS 不可用时使用")
	verbose := flag.Bool("v", false, "打印详细请求日志")
	flag.Parse()

	if *user == "" || *pass == "" {
		log.Fatal("usage: pikpak-proxy -user <PikPak用户名> -pass <PikPak密码> [-listen :7777] [-auth-user U -auth-pass P] [-cachedir ./cache] [-dns 223.5.5.5] [-v]")
	}
	if (*authUser == "") != (*authPass == "") {
		log.Fatal("-auth-user 和 -auth-pass 必须同时设置")
	}

	var dnsServers []string
	if *dns != "" {
		for _, s := range strings.Split(*dns, ",") {
			if s = strings.TrimSpace(s); s != "" {
				dnsServers = append(dnsServers, s)
			}
		}
	}

	authMode := "匿名模式（任意账号密码可连）"
	if *authUser != "" {
		authMode = "认证模式（账号 " + *authUser + "）"
	}
	cacheMode := "关闭（仅内存缓存）"
	if *cacheDir != "" {
		cacheMode = "开启（" + *cacheDir + "，sparse 稀疏文件）"
	}
	dnsMode := "自动探测"
	if len(dnsServers) > 0 {
		dnsMode = strings.Join(dnsServers, ",")
	}

	dav := davclient.New(*target, *user, *pass, *c, dnsServers)
	srv := server.New(dav, *cacheDir, *authUser, *authPass)
	davclient.Verbose = *verbose
	server.Verbose = *verbose

	log.Printf("pikpak-proxy v%s 启动", version)
	log.Printf("  监听     : %s（播放器连 http://<本机IP>%s）", *listen, *listen)
	log.Printf("  WebDAV   : %s（账号 %s）", *target, *user)
	log.Printf("  并发     : %d 连接/文件", *c)
	log.Printf("  认证     : %s", authMode)
	log.Printf("  磁盘缓存 : %s", cacheMode)
	log.Printf("  DNS      : %s [%s]", dnsMode, dav.DNSNote())
	if *verbose {
		log.Printf("  详细日志 : 开启（-v）")
	}

	// 信号处理：Ctrl+C 优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down...")
		os.Exit(0)
	}()

	log.Fatal(srv.ListenAndServe(*listen))
}
