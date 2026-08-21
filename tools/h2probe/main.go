// h2probe: 判别 dav.mypikpak.com 网关对 HTTP/2 vs HTTP/1.1 GET 的行为差异
package main

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	url  = "https://dav.mypikpak.com/My%20Pack/PikPak%20Tutorial.mp4"
	user = "bbse"
	pass = "fxssfgjz"
	ua   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
)

func probe(name string, client *http.Client) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Println(name, "req err:", err)
		return
	}
	req.SetBasicAuth(user, pass)
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("User-Agent", ua)

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("%s: err %v\n", name, err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
	fmt.Printf("%s: proto=%s code=%d ttfb=%s location=%q via=%q body=%q\n",
		name, resp.Proto, resp.StatusCode, time.Since(start).Round(time.Millisecond),
		resp.Header.Get("Location"), resp.Header.Get("Via"), string(body))
}

func main() {
	// HTTP/2（默认 Transport 自动 ALPN 协商 h2）
	h2c := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	probe("h2-auto", h2c)

	// 强制 HTTP/1.1（等价当前 proxy 的行为）
	h1t := &http.Transport{ForceAttemptHTTP2: false}
	h1c := &http.Client{Transport: h1t, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	probe("h1-only", h1c)
}
