// enumtest: 节点枚举优选验证工具——用一条有效签名 URL 直调 ScanNodes，
// 观察 DNS 探活 + 测速排名 + 最优节点锁定。全程不碰网关（熔断期可用）。
//
// 用法: enumtest.exe "https://dl-z01a-XXXX.mypikpak.net/download/?fid=..."
package main

import (
	"fmt"
	"os"
	"time"

	"pikpak-webdav-proxy/internal/davclient"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: enumtest <signed-url>")
		os.Exit(1)
	}
	davclient.Verbose = true
	c := davclient.New("https://dav.mypikpak.com", "x", "y", 8, nil) // 凭证任意：扫描不碰网关
	ev := davclient.NewNodeEvaluator(c)
	sample := &davclient.SignedURL{
		URL:    os.Args[1],
		Expiry: time.Now().Add(time.Hour),
	}
	t0 := time.Now()
	ev.ScanNodes(sample)
	fmt.Printf("\n最优节点: %q （扫描耗时 %s）\n", ev.BestNode(), time.Since(t0).Round(time.Millisecond))
}
