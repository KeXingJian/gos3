package catalog

import (
	"bytes"
	"math/rand"
)

// maxLevel 是 skip list 的最大层数。层级越高概率越低（p=1/2），
// 因此 maxLevel=32 足够支撑 2^32 量级的节点数。
const maxLevel = 32

// skipNode 是 skip list 的节点：key 有序，next 是各层前向指针。
type skipNode struct {
	key  []byte
	doc  *Doc
	next []*skipNode
}

// omap 是基于 skip list 的有序 map（[]byte -> *Doc）。
//
// 注意：omap 自身不做并发安全，写路径由分片/单锁保证串行，读路径通过
// atomic.Pointer 换代后无锁读。不要在多个 goroutine 里无锁并发读写同一个实例。
type omap struct {
	head  *skipNode // 哨兵节点，next 长度固定为 maxLevel
	level int       // 当前实际使用的最高层（1..maxLevel）
	n     int       // 节点数
	rnd   *rand.Rand
}

func newOmap() *omap {
	return &omap{
		head:  &skipNode{next: make([]*skipNode, maxLevel)},
		level: 1,
		rnd:   rand.New(rand.NewSource(1)), // 固定种子，便于测试复现
	}
}

// randomLevel 按 p=1/2 的几何分布随机层高，最低 1 层。
func (m *omap) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && m.rnd.Intn(2) == 0 {
		lvl++
	}
	return lvl
}

// find 从最高层向下，返回每一层上 key 应插入位置的前驱节点。
func (m *omap) find(key []byte) [maxLevel]*skipNode {
	var update [maxLevel]*skipNode
	x := m.head
	for i := m.level - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
		update[i] = x
	}
	return update
}

// Get 返回 key 对应的 Doc；不存在时返回 false。
func (m *omap) Get(key []byte) (*Doc, bool) {
	x := m.head
	for i := m.level - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
	}
	x = x.next[0]
	if x != nil && bytes.Equal(x.key, key) {
		return x.doc, true
	}
	return nil, false
}

// Upsert 写入 key -> doc，返回旧值（不存在时返回 nil,false）。
func (m *omap) Upsert(key []byte, doc *Doc) (*Doc, bool) {
	update := m.find(key)
	x := update[0].next[0]
	if x != nil && bytes.Equal(x.key, key) {
		old := x.doc
		x.doc = doc
		return old, true
	}
	lvl := m.randomLevel()
	if lvl > m.level {
		for i := m.level; i < lvl; i++ {
			update[i] = m.head
		}
		m.level = lvl
	}
	x = &skipNode{
		key:  append([]byte(nil), key...),
		doc:  doc,
		next: make([]*skipNode, lvl),
	}
	for i := 0; i < lvl; i++ {
		x.next[i] = update[i].next[i]
		update[i].next[i] = x
	}
	m.n++
	return nil, false
}

// Delete 删除 key，返回被删的 Doc；不存在时返回 nil,false。
func (m *omap) Delete(key []byte) (*Doc, bool) {
	update := m.find(key)
	x := update[0].next[0]
	if x == nil || !bytes.Equal(x.key, key) {
		return nil, false
	}
	for i := 0; i < m.level; i++ {
		if update[i].next[i] == x {
			update[i].next[i] = x.next[i]
		}
	}
	for m.level > 1 && m.head.next[m.level-1] == nil {
		m.level--
	}
	m.n--
	return x.doc, true
}

// Len 返回节点数。
func (m *omap) Len() int { return m.n }

// iter 是按键升序遍历的迭代器。
type iter struct {
	x *skipNode
}

// Valid 报告迭代器是否还有元素。
func (it *iter) Valid() bool { return it.x != nil }

// Next 前进到下一个元素。
func (it *iter) Next() { it.x = it.x.next[0] }

// Key 返回当前节点的键（只读，勿修改）。
func (it *iter) Key() []byte { return it.x.key }

// Doc 返回当前节点的值。
func (it *iter) Doc() *Doc { return it.x.doc }

// Seek 定位到第一个 >= key 的节点，返回迭代器。
// 典型用法：for it := m.Seek(prefix); it.Valid() && bytes.HasPrefix(it.Key(), prefix); it.Next() { ... }
func (m *omap) Seek(key []byte) *iter {
	x := m.head
	for i := m.level - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
	}
	return &iter{x: x.next[0]}
}
