// Package format 负责集群/纠删码布局的持久化与一致性校验。
//
// 每个磁盘根目录下保存一份 format.json，记录：
//   - 部署 ID（所有节点/所有盘一致）
//   - 本盘 UUID（This）
//   - 全局盘布局（Sets：按「集合 -> 槽位」排列的盘 UUID）
//
// 有了它，所有节点可以从同一份持久化布局推导出完全一致的盘顺序与盘身份，
// 从而避免「成员配置非对称 -> 分片映射错位 -> 静默数据损坏」。
package format

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/kxj/gos3/internal/disk"
)

const (
	// FormatFile 是布局文件在单块磁盘上的相对路径。
	FormatFile = ".gos3.sys/format.json"

	formatBackend    = "xl"
	formatVersion    = "1"
	xlFormatVersion  = "3"
	distributionAlgo = "SIPMOD"
)

var (
	// ErrUnformatted 表示该磁盘尚未写入 format.json。
	ErrUnformatted = errors.New("format: unformatted disk")
	// ErrInconsistent 表示磁盘布局与其他盘/集群期望不一致，属于不可重试错误。
	ErrInconsistent = errors.New("format: inconsistent disk layout")
)

// Format 是单个磁盘上的布局文件内容。
type Format struct {
	Version string   `json:"version"`
	Format  string   `json:"format"`
	ID      string   `json:"id"`
	XL      XLFormat `json:"xl"`
}

// XLFormat 是纠删码后端专有布局。
type XLFormat struct {
	Version          string     `json:"version"`
	This             string     `json:"this"`
	Sets             [][]string `json:"sets"`
	DistributionAlgo string     `json:"distributionAlgo"`
}

// New 生成一份全新布局：部署 ID 随机，并为每个槽位分配一个盘 UUID。
// 当前 gos3 只使用单个集合，numSets 传 1 即可。
func New(numSets, setLen int) (*Format, error) {
	if numSets < 1 || setLen < 1 {
		return nil, fmt.Errorf("format: invalid layout %d sets x %d drives", numSets, setLen)
	}
	id, err := newUUID()
	if err != nil {
		return nil, err
	}
	sets := make([][]string, numSets)
	for s := range sets {
		sets[s] = make([]string, setLen)
		for d := range sets[s] {
			u, err := newUUID()
			if err != nil {
				return nil, err
			}
			sets[s][d] = u
		}
	}
	return &Format{
		Version: formatVersion,
		Format:  formatBackend,
		ID:      id,
		XL: XLFormat{
			Version:          xlFormatVersion,
			Sets:             sets,
			DistributionAlgo: distributionAlgo,
		},
	}, nil
}

// Load 从单块磁盘读取 format.json；文件不存在返回 ErrUnformatted。
func Load(ctx context.Context, d disk.Disk) (*Format, error) {
	data, err := d.ReadFile(ctx, FormatFile)
	if errors.Is(err, disk.ErrNotExist) {
		return nil, ErrUnformatted
	}
	if err != nil {
		return nil, err
	}
	var f Format
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("format: decode %s: %w", d.ID(), err)
	}
	if f.Version != formatVersion || f.Format != formatBackend || f.XL.Version != xlFormatVersion {
		return nil, fmt.Errorf("%w: unsupported version on %s", ErrInconsistent, d.ID())
	}
	return &f, nil
}

// Save 把布局写入单块磁盘（带 This 的形态）。
func Save(ctx context.Context, d disk.Disk, f *Format) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return d.WriteFile(ctx, FormatFile, data)
}

// Drives 返回布局中的盘总数。
func (f *Format) Drives() int {
	n := 0
	for _, s := range f.XL.Sets {
		n += len(s)
	}
	return n
}

// LayoutHash 是 Sets 的稳定摘要，用于跨节点快速比对布局是否一致。
func (f *Format) LayoutHash() string {
	var b strings.Builder
	for _, s := range f.XL.Sets {
		for _, u := range s {
			b.WriteString(u)
			b.WriteByte('/')
		}
		b.WriteByte('|')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// DriveUUID 按全局槽位下标返回该槽位应持有的盘 UUID。
func (f *Format) DriveUUID(slot int) string {
	uuids := f.SlotUUIDs()
	if slot < 0 || slot >= len(uuids) {
		return ""
	}
	return uuids[slot]
}

// SlotUUIDs 按全局槽位下标顺序展开布局中的全部盘 UUID。
func (f *Format) SlotUUIDs() []string {
	out := make([]string, 0, f.Drives())
	for _, s := range f.XL.Sets {
		out = append(out, s...)
	}
	return out
}

// FindDiskIndex 由盘 UUID 反查其所属槽位下标；布局中不存在该 UUID 时返回 -1。
// 用于诊断「盘顺序与布局不一致」（例如 -data-dirs 顺序被改动）的启动失败。
func (f *Format) FindDiskIndex(this string) int {
	for i, u := range f.SlotUUIDs() {
		if u == this {
			return i
		}
	}
	return -1
}

// ForSlot 返回一份拷贝，其 This 指向指定槽位的盘 UUID。
func (f *Format) ForSlot(slot int) *Format {
	c := *f
	c.XL.This = f.DriveUUID(slot)
	return &c
}

// Bootstrap 确保所有磁盘拥有一致的布局：
//   - 全部未格式化时，由 initializer 节点生成并写入所有盘；写入未达多数派则回滚，回到全未格式化；
//   - 否则加载并用多数派布局作为参考，校验每块盘，并补齐未格式化的盘。
//
// 非 initializer 节点在布局尚未出现时会按 retry 周期等待，直到 ctx 超时。
// 布局不一致（ErrInconsistent）直接返回，不重试。
func Bootstrap(ctx context.Context, disks []disk.Disk, initializer bool, log *slog.Logger, retry time.Duration) (*Format, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ref, err := tryBootstrap(ctx, disks, initializer, log)
		if err == nil {
			return ref, nil
		}
		if errors.Is(err, ErrInconsistent) {
			return nil, err
		}
		log.Info("[gos3: waiting-for-format]", "error", err.Error())
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retry):
		}
	}
}

func tryBootstrap(ctx context.Context, disks []disk.Disk, initializer bool, log *slog.Logger) (*Format, error) {
	formats := make([]*Format, len(disks))
	unformatted := 0
	for i, d := range disks {
		f, err := Load(ctx, d)
		if err != nil {
			if errors.Is(err, ErrUnformatted) {
				unformatted++
				continue
			}
			if errors.Is(err, ErrInconsistent) {
				return nil, err
			}
			return nil, err
		}
		formats[i] = f
	}

	// 全新集群：首个节点负责初始化。
	if unformatted == len(disks) {
		if !initializer {
			return nil, errors.New("format: waiting for initializer node to format")
		}
		return initAll(ctx, disks, log)
	}

	// 已存在布局：取多数派作为参考。
	ref := pickQuorum(formats, len(disks))
	if ref == nil {
		return nil, errors.New("format: layout read quorum not reached")
	}
	if ref.Drives() != len(disks) {
		return nil, fmt.Errorf("%w: layout has %d drives, cluster expects %d", ErrInconsistent, ref.Drives(), len(disks))
	}

	// 逐盘校验，并补齐未格式化的盘。
	for i, d := range disks {
		want := ref.ForSlot(i)
		if formats[i] == nil {
			if err := Save(ctx, d, want); err != nil {
				log.Warn("[gos3: format-repair-failed]", "disk", d.ID(), "error", err.Error())
			}
			continue
		}
		if formats[i].ID != ref.ID || formats[i].LayoutHash() != ref.LayoutHash() {
			return nil, fmt.Errorf("%w: drive %s layout hash mismatch at slot %d", ErrInconsistent, d.ID(), i)
		}
		if formats[i].XL.This != want.XL.This {
			// 盘内容合法但不属于当前槽位：通常是成员顺序或 -data-dirs 顺序变了。
			return nil, fmt.Errorf("%w: drive %s holds uuid %s (slot %d), but is used at slot %d",
				ErrInconsistent, d.ID(), formats[i].XL.This, ref.FindDiskIndex(formats[i].XL.This), i)
		}
	}
	log.Info("[gos3: format-loaded]", "deployment", ref.ID, "drives", len(disks), "layout", ref.LayoutHash())
	return ref, nil
}

// initAll 在全新集群上生成布局并写入所有盘。
// 若写入未达多数派则回滚已写入的布局，避免留下「部分盘已格式化」的中间状态，
// 否则下一轮既无法重新初始化（不再是全未格式化），也凑不出多数派布局。
func initAll(ctx context.Context, disks []disk.Disk, log *slog.Logger) (*Format, error) {
	nf, err := New(1, len(disks))
	if err != nil {
		return nil, err
	}
	written := make([]disk.Disk, 0, len(disks))
	for i, d := range disks {
		if err := Save(ctx, d, nf.ForSlot(i)); err != nil {
			log.Warn("[gos3: format-write-failed]", "disk", d.ID(), "error", err.Error())
			continue
		}
		written = append(written, d)
	}
	if len(written) < majority(len(disks)) {
		for _, d := range written {
			if err := d.DeleteFile(ctx, FormatFile); err != nil {
				log.Error("[gos3: format-rollback-failed]", "disk", d.ID(), "error", err.Error())
			}
		}
		return nil, fmt.Errorf("format: init write quorum not reached (%d/%d)", len(written), len(disks))
	}
	log.Info("[gos3: format-init]", "deployment", nf.ID, "drives", len(disks), "layout", nf.LayoutHash())
	return nf, nil
}

// pickQuorum 从已读取的布局中选出达到多数派的一致布局作为参考。
func pickQuorum(formats []*Format, total int) *Format {
	type group struct {
		ref   *Format
		count int
	}
	groups := map[string]*group{}
	for _, f := range formats {
		if f == nil {
			continue
		}
		h := f.LayoutHash()
		g := groups[h]
		if g == nil {
			g = &group{ref: f}
			groups[h] = g
		}
		g.count++
	}
	var best *group
	for _, g := range groups {
		if g.count >= majority(total) && (best == nil || g.count > best.count) {
			best = g
		}
	}
	if best == nil {
		return nil
	}
	return best.ref
}

func majority(n int) int {
	return n/2 + 1
}

// newUUID 生成一个随机 UUID v4 字符串。
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
