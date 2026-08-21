package davclient

// 根证书加载 —— 与 smartDNS 同一套哲学：先读 Termux 环境的证书包，
// 找不到就用内置 Mozilla 根证书兜底（go:embed，无运行时依赖）。
//
// 背景：Go 的 crypto/x509 在 Unix 上只认标准路径（/etc/ssl/certs/ca-certificates.crt
// 等）。Android 应用沙箱里没有 /etc —— Termux 下跑 Go 程序时 SystemCertPool
// 返回空池，所有 HTTPS 握手报 "certificate signed by unknown authority"。
// Termux 自己的 ca-certificates 包装在 $PREFIX/etc/tls/cert.pem，
// 标准探测路径读不到它。

import (
	"crypto/tls"
	"crypto/x509"
	_ "embed"
	"os"
)

//go:embed cacert.pem
var embeddedCA []byte

// certFileCandidates 非标准环境的 CA bundle 路径（Go 原生读不到的），
// 按优先级：Termux($PREFIX) → proot/容器常见位置。
var certFileCandidates = []string{
	"/data/data/com.termux/files/usr/etc/tls/cert.pem",                // termux ca-certificates 包
	"/data/data/com.termux/files/usr/etc/ssl/certs/ca-certificates.crt", // termux 老版路径
	"/usr/etc/ssl/certs/ca-certificates.crt",                           // proot 容器
}

// loadRootCAs 构建根证书池：
//  1. 探测 Termux/proot 证书文件，读到就以其为主（对齐 DNS 的"先 Termux"策略）
//  2. 否则用系统池 + 内置 Mozilla 根证书（系统池在安卓原生为空，内置兜底生效；
//     在 Windows/Linux 上是超集，无副作用）
//
// 返回 tls.Config 和来源说明（启动横幅用）。
func loadRootCAs() (*tls.Config, string) {
	for _, f := range certFileCandidates {
		if data, err := os.ReadFile(f); err == nil {
			pool := x509.NewCertPool()
			if pool.AppendCertsFromPEM(data) {
				return &tls.Config{RootCAs: pool}, "termux(" + f + ")"
			}
		}
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(embeddedCA) {
		// 理论不可达（embed 的静态文件）；防御性兜底
		return nil, "系统（内置根证书解析失败，走系统默认）"
	}
	return &tls.Config{RootCAs: pool}, "系统 + 内置 Mozilla 根证书"
}
