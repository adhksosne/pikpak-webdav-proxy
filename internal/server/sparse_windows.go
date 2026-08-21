//go:build windows

package server

import (
	"os"
	"syscall"
)

// fsctlSetSparse = FSCTL_SET_SPARSE：把文件标记为 NTFS 稀疏文件。
// Windows 上 Truncate 扩容不产生 sparse（与 Linux ftruncate 不同），
// 会把整个 fileSize 直接分配到磁盘（9GB 电影 = 9GB 实占）。标记 sparse 后
// 只有真正写入的段占空间。
const fsctlSetSparse = 0x000900c4

func markSparse(f *os.File) error {
	var bytesReturned uint32
	return syscall.DeviceIoControl(syscall.Handle(f.Fd()), fsctlSetSparse,
		nil, 0, nil, 0, &bytesReturned, nil)
}
