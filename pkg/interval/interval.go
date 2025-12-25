package interval

import (
	"sort"
	"sync"
)

// Range 表示一个闭区间 [Start, End]
type Range struct {
	Start int64
	End   int64
}

// Manager 用于管理已下载的区间，支持并发安全的操作
type Manager struct {
	mu        sync.RWMutex
	intervals []Range
}

// NewManager 创建一个新的区间管理器
func NewManager() *Manager {
	return &Manager{
		intervals: make([]Range, 0),
	}
}

// Add 添加一个新的已下载区间 [start, end] (闭区间)
// start: 起始字节偏移量
// end: 结束字节偏移量 (包含)
func (m *Manager) Add(start, end int64) {
	if start > end {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 简单的插入并合并策略
	// 1. 找到插入位置或直接追加
	// 为了保持有序，我们先将新区间加入，然后排序，再进行一次合并扫描
	// 在区间数量不大时（Docker layer 分块通常在几百个以内），这种简单策略性能足够且最稳健

	// 优化：如果是完全有序追加（大部分情况），可以直接检查最后一个
	if len(m.intervals) == 0 {
		m.intervals = append(m.intervals, Range{Start: start, End: end})
		return
	}

	// 检查是否可以快速合并到最后一个区间
	last := &m.intervals[len(m.intervals)-1]
	if start == last.End+1 {
		last.End = end
		return
	}
	if start > last.End+1 {
		m.intervals = append(m.intervals, Range{Start: start, End: end})
		return
	}

	// 如果不能快速追加，回退到通用合并逻辑
	// 插入新区间
	m.intervals = append(m.intervals, Range{Start: start, End: end})

	// 排序
	sort.Slice(m.intervals, func(i, j int) bool {
		return m.intervals[i].Start < m.intervals[j].Start
	})

	// 合并
	merged := make([]Range, 0, len(m.intervals))
	current := m.intervals[0]

	for i := 1; i < len(m.intervals); i++ {
		next := m.intervals[i]

		// 如果当前区间的结束位置 + 1 >= 下一个区间的开始位置，说明重叠或连续
		if current.End+1 >= next.Start {
			if next.End > current.End {
				current.End = next.End
			}
		} else {
			merged = append(merged, current)
			current = next
		}
	}
	merged = append(merged, current)
	m.intervals = merged
}

// GetMaxSafeOffset 返回从 0 开始连续覆盖的最大偏移量
// 也就是说，[0, offset) 的数据都已经准备好了
// 返回值如果是 100，表示字节 0-99 已存在，下一个需要的字节是 100
func (m *Manager) GetMaxSafeOffset() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.intervals) == 0 {
		return 0
	}

	first := m.intervals[0]
	if first.Start == 0 {
		return first.End + 1
	}

	return 0
}

// IsComplete 检查是否覆盖了 [0, totalSize-1]
func (m *Manager) IsComplete(totalSize int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.intervals) == 0 {
		return false
	}

	// 只需要检查第一个区间是否从 0 开始且覆盖了 totalSize
	// 注意：区间可能会超过 totalSize (如果逻辑有 bug 或者下载器多下了)，但这里主要检查是否覆盖
	first := m.intervals[0]
	return first.Start == 0 && first.End >= totalSize-1
}

// Dump 导出当前的区间列表副本 (用于序列化)
func (m *Manager) Dump() []Range {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dump := make([]Range, len(m.intervals))
	copy(dump, m.intervals)
	return dump
}

// Load 导入区间列表 (用于反序列化)
// 注意：这会覆盖当前的所有区间
func (m *Manager) Load(ranges []Range) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 深度拷贝以防止外部修改影响内部状态
	m.intervals = make([]Range, len(ranges))
	copy(m.intervals, ranges)
}

// Includes 检查区间 [start, end] 是否被某个已下载区间完全包含
func (m *Manager) Includes(start, end int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 因为 m.intervals 是有序的，我们可以用二分查找优化查找
	// 但考虑到区间主要用于稀疏文件，遍历也很快。
	// 简单起见，这里先遍历。如果性能有问题（区间非常多），可以使用 sort.Search。

	// 优化：从后往前查？或者二分。
	// 实现二分查找找到第一个 Start <= start 的区间
	idx := sort.Search(len(m.intervals), func(i int) bool {
		return m.intervals[i].Start > start
	})

	// sort.Search 返回的是第一个 > start 的索引，我们需要前一个
	if idx > 0 {
		candidate := m.intervals[idx-1]
		// 检查 candidate 是否覆盖 [start, end]
		// candidate.Start <= start 已经满足
		if candidate.End >= end {
			return true
		}
	}

	return false
}
