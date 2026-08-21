//go:build !windows

package server

import "os"

// markSparse：POSIX 上 ftruncate 扩容天然产生 sparse file，无需处理。
func markSparse(f *os.File) error {
	return nil
}
