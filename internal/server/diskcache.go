package server

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
)

// DiskCache 管理磁盘文件缓存。
//
// 行为设计：
//   - 每个云文件对应一个本地 .cache 文件
//   - 按 chunk 边界 WriteAt/ReadAt，多 worker 并发写不同段安全
//   - 段索引持久化到 .index 文件，重启后复用已下载段
//   - 后台整文件下载，下完后所有 Range 命中本地（随便拖不卡）
type DiskCache struct {
	dir string
	mu  sync.Mutex
	// path -> *cacheFile（懒加载）
	files map[string]*cacheFile
}

// cacheFile 单个云文件的磁盘缓存。
type cacheFile struct {
	mu        sync.Mutex
	localPath string    // 本地 .cache 文件路径
	idxPath   string    // .index 文件路径
	f         *os.File
	fileSize  int64
	chunkSize int64
	chunks    map[int64]int64 // 已下载段的起始 offset -> 段长度
	dirty     int             // 自上次 saveIndex 后新增的段数（定期持久化，防重启丢缓存）
}

func NewDiskCache(dir string) *DiskCache {
	if dir == "" {
		return nil // 磁盘缓存关闭：仅用内存缓存（内存缓存已够日常播放）
	}
	os.MkdirAll(dir, 0755)
	return &DiskCache{dir: dir, files: make(map[string]*cacheFile)}
}

// get 获取或创建某 path 的磁盘缓存。
// fileName 用作缓存文件名基础（已转义）。
func (d *DiskCache) get(path, fileName string, fileSize, chunkSize int64) (*cacheFile, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if cf, ok := d.files[path]; ok {
		return cf, nil
	}
	// 用 fileName 的 hash 避免路径/特殊字符问题
	safe := safeName(fileName)
	localPath := filepath.Join(d.dir, safe+".cache")
	idxPath := filepath.Join(d.dir, safe+".index")
	cf := &cacheFile{
		localPath: localPath,
		idxPath:   idxPath,
		fileSize:  fileSize,
		chunkSize: chunkSize,
		chunks:    make(map[int64]int64),
	}
	if err := cf.open(); err != nil {
		return nil, err
	}
	d.files[path] = cf
	return cf, nil
}

func (c *cacheFile) open() error {
	// 不存在则创建，预分配到 fileSize（sparse：只占实际写入的空间）
	f, err := os.OpenFile(c.localPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	c.f = f
	// Windows/NTFS 需显式设置 sparse 标记，否则 Truncate 会把整个
	// fileSize 直接分配到磁盘（8.7GB 电影 = 8.7GB 实占）；对已存在的
	// 非 sparse 缓存文件重复打开也会把未写过的尾部释放掉
	if err := markSparse(f); err != nil {
		// 置不了 sparse 也继续（功能不受影响，只是占磁盘）
		_ = err
	}
	if fi, err := f.Stat(); err == nil && fi.Size() != c.fileSize {
		if err := f.Truncate(c.fileSize); err != nil {
			f.Close()
			return err
		}
	}
	c.loadIndex()
	return nil
}

// has 段是否已缓存且长度覆盖 needLen。
// 长度校验必须做：旧版本 .index 存的是 1MB/512KB 边界段，与当前 chunk 边界不一致，
// 只查 offset 命中会把 sparse hole 的零数据当缓存发给播放器（静默数据损坏）。
func (c *cacheFile) has(off, needLen int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.chunks[off] >= needLen
}

// segLen 计算某 offset 所在段的完整长度（末段可能不足 chunk）
func (c *cacheFile) segLen(off int64) int64 {
	l := c.chunkSize
	if off+l > c.fileSize {
		l = c.fileSize - off
	}
	return l
}

// read 从本地文件读一段（调用方确保已缓存）
func (c *cacheFile) read(off, size int64) ([]byte, error) {
	buf := make([]byte, size)
	c.mu.Lock()
	f := c.f
	c.mu.Unlock()
	if f == nil {
		return nil, os.ErrClosed
	}
	_, err := f.ReadAt(buf, off)
	return buf, err
}

// write 写一段到本地文件 + 标记已缓存（记录长度）+ 定期持久化索引
func (c *cacheFile) write(off int64, data []byte) {
	c.mu.Lock()
	if c.chunks[off] >= int64(len(data)) {
		c.mu.Unlock()
		return // 已有更长段
	}
	f := c.f
	c.mu.Unlock()
	if f == nil {
		return
	}
	if _, err := f.WriteAt(data, off); err != nil {
		return
	}
	c.mu.Lock()
	c.chunks[off] = int64(len(data))
	c.dirty++
	needSave := c.dirty >= 32 // 每 32 段落盘一次，防重启丢已下载缓存
	c.dirty = 0
	c.mu.Unlock()
	if needSave {
		c.saveIndex()
	}
}

// complete 是否整文件已缓存
func (c *cacheFile) complete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := (c.fileSize + c.chunkSize - 1) / c.chunkSize
	return int64(len(c.chunks)) >= total
}

// downloadedCount 已缓存段数
func (c *cacheFile) downloadedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.chunks)
}

// indexMagic .index 文件头魔数。
// v2 格式带段长度；v1（仅 offset 列表）在 chunk 边界改动后不可信，直接作废。
var indexMagic = []byte("CDC2IDX2")

// loadIndex 从 .index 文件加载已下载段 (offset, length) 列表。
// 格式：8 字节 magic + 8 字节 count + count*(8B offset + 8B length)。
// 旧格式（无 magic）不兼容，忽略——旧缓存段边界混杂，重新下载更安全。
func (c *cacheFile) loadIndex() {
	data, err := os.ReadFile(c.idxPath)
	if err != nil || len(data) < 16 {
		return
	}
	if string(data[:8]) != string(indexMagic) {
		return // 旧 v1 格式，作废
	}
	n := binary.LittleEndian.Uint64(data[8:16])
	for i := uint64(0); i < n && len(data) >= int(16+16*(i+1)); i++ {
		off := int64(binary.LittleEndian.Uint64(data[16+16*i:]))
		l := int64(binary.LittleEndian.Uint64(data[16+16*i+8:]))
		if l > 0 && off >= 0 && off+l <= c.fileSize {
			c.chunks[off] = l
		}
	}
}

// saveIndex 持久化段索引
func (c *cacheFile) saveIndex() {
	c.mu.Lock()
	offs := make([]int64, 0, len(c.chunks))
	for off := range c.chunks {
		offs = append(offs, off)
	}
	buf := make([]byte, 16+16*len(offs))
	copy(buf[:8], indexMagic)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(len(offs)))
	for i, off := range offs {
		binary.LittleEndian.PutUint64(buf[16+16*i:], uint64(off))
		binary.LittleEndian.PutUint64(buf[16+16*i+8:], uint64(c.chunks[off]))
	}
	c.mu.Unlock()
	os.WriteFile(c.idxPath, buf, 0644)
}

// safeName 把文件名转成安全的缓存文件名。
// 用 SHA1 hash 避免中文/特殊字符导致的文件名乱码（之前逐字节处理中文 UTF-8 出 bug）。
func safeName(s string) string {
	h := sha1.Sum([]byte(s))
	return hex.EncodeToString(h[:16]) // 32 字符 hex，足够唯一且文件名安全
}
