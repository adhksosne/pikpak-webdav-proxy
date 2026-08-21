// memscan: 扫描指定进程内存，提取 PikPak 签名直链（验证 CD2 签名缓存假设）
// 用法: memscan.exe <pid>
// 原理: 签名 URL 以 Rust String 形式驻留 CD2 堆内存，按 "mypikpak" 特征扫描
package main

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"unsafe"
)

var (
	kernel32        = syscall.NewLazyDLL("kernel32.dll")
	procOpenProcess = kernel32.NewProc("OpenProcess")
	procVirtualQueryEx = kernel32.NewProc("VirtualQueryEx")
	procReadProcessMem = kernel32.NewProc("ReadProcessMemory")
	procCloseHandle = kernel32.NewProc("CloseHandle")
)

const (
	procQueryInfo = 0x0400
	procVMRead    = 0x0010
	memCommit     = 0x1000
	pageNoaccess  = 0x01
	pageGuard     = 0x100
)

type mbi struct {
	base       uintptr
	allocBase  uintptr
	allocProt  uint32
	_          uint32 // padding
	size       uintptr
	state      uint32
	protect    uint32
	regionType uint32
	_          uint32 // padding
}

// urlish: URL 中合法可见字符（用于边界扩展）
func urlish(b byte) bool {
	if b < 0x21 || b > 0x7e {
		return false
	}
	switch b {
	case '"', '\'', '<', '>', '{', '}', '|', '\\', '^', '`':
		return false
	}
	return true
}

func extract(data []byte, found map[string]int) {
	needle := []byte("mypikpak")
	pos := 0
	for {
		i := bytes.Index(data[pos:], needle)
		if i < 0 {
			return
		}
		i += pos
		// 向前回溯 URL 边界（最多 2048B）
		start := i
		limit := i - 2048
		if limit < 0 {
			limit = 0
		}
		for start > limit && urlish(data[start-1]) {
			start--
		}
		// 必须以 http 开头且包含 ://
		if !bytes.HasPrefix(data[start:], []byte("http")) || !bytes.Contains(data[start:i], []byte("://")) {
			pos = i + 8
			continue
		}
		// 向后扩展 URL 边界
		end := i
		for end < len(data) && urlish(data[end]) {
			end++
		}
		u := string(data[start:end])
		if len(u) > 16 && len(u) < 2048 {
			found[u]++
		}
		pos = i + 8
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: memscan <pid>")
		os.Exit(1)
	}
	pid, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad pid:", err)
		os.Exit(1)
	}
	h, _, e := procOpenProcess.Call(procQueryInfo|procVMRead, 0, uintptr(pid))
	if h == 0 {
		fmt.Fprintln(os.Stderr, "OpenProcess failed:", e)
		os.Exit(1)
	}
	defer procCloseHandle.Call(h)

	found := map[string]int{}
	var addr uintptr
	var scanned uint64
	for addr < 0x7FFFFFFFFFFF {
		var m mbi
		r, _, _ := procVirtualQueryEx.Call(h, addr, uintptr(unsafe.Pointer(&m)), unsafe.Sizeof(m))
		if r == 0 {
			break
		}
		if m.size == 0 {
			break
		}
		if m.state == memCommit && m.protect&pageGuard == 0 && m.protect&0xFF != pageNoaccess && m.size <= 0x8000000 {
			data := make([]byte, m.size)
			var n uintptr
			ok, _, _ := procReadProcessMem.Call(h, m.base, uintptr(unsafe.Pointer(&data[0])), m.size, uintptr(unsafe.Pointer(&n)))
			if ok != 0 && n > 0 {
				scanned += uint64(n)
				extract(data[:n], found)
			}
		}
		addr = m.base + m.size
	}
	fmt.Fprintf(os.Stderr, "scanned %d MB, %d unique mypikpak URLs\n", scanned>>20, len(found))
	for u, c := range found {
		fmt.Printf("%d\t%s\n", c, u)
	}
}
