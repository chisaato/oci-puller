package interval

import (
	"testing"
)

func TestManager_Basic(t *testing.T) {
	m := NewManager()

	// 1. Initially 0
	if off := m.GetMaxSafeOffset(); off != 0 {
		t.Errorf("Expected 0, got %d", off)
	}

	// 2. Add [0, 9] (10 bytes)
	m.Add(0, 9)
	if off := m.GetMaxSafeOffset(); off != 10 {
		t.Errorf("Expected 10, got %d", off)
	}

	// 3. Add [20, 29] (Gap 10-19 missing)
	m.Add(20, 29)
	if off := m.GetMaxSafeOffset(); off != 10 {
		t.Errorf("Expected 10 (stuck at gap), got %d", off)
	}

	// 4. Fill Gap partially [10, 14]
	m.Add(10, 14)
	if off := m.GetMaxSafeOffset(); off != 15 {
		t.Errorf("Expected 15, got %d", off)
	}

	// 5. Fill remaining gap [15, 19] -> Complete [0, 29]
	m.Add(15, 19)
	if off := m.GetMaxSafeOffset(); off != 30 {
		t.Errorf("Expected 30, got %d", off)
	}

	// Check intervals count, should be 1 merged interval
	if len(m.intervals) != 1 {
		t.Errorf("Expected 1 interval, got %d: %v", len(m.intervals), m.intervals)
	}
}

func TestManager_Overlaps(t *testing.T) {
	m := NewManager()

	// [0, 10]
	m.Add(0, 10)

	// Overlap [5, 15] -> [0, 15]
	m.Add(5, 15)

	if off := m.GetMaxSafeOffset(); off != 16 {
		t.Errorf("Expected 16, got %d", off)
	}
}

func TestManager_Unordered(t *testing.T) {
	m := NewManager()

	// Add [100, 200]
	m.Add(100, 200)
	if off := m.GetMaxSafeOffset(); off != 0 {
		t.Errorf("Expected 0, got %d", off)
	}

	// Add [0, 99]
	m.Add(0, 99)
	// Should merge to [0, 200]
	if off := m.GetMaxSafeOffset(); off != 201 {
		t.Errorf("Expected 201, got %d", off)
	}
}
