package read

import (
	"testing"

	"github.com/justapithecus/lode/lode"
)

// -----------------------------------------------------------------------------
// SegmentFileIterator tests
// -----------------------------------------------------------------------------

func TestSegmentFileIterator_Basic(t *testing.T) {
	files := []lode.FileRef{
		{Path: "data/file1.json", SizeBytes: 100},
		{Path: "data/file2.json", SizeBytes: 200},
		{Path: "data/file3.json", SizeBytes: 300},
	}

	it := NewSegmentFileIterator("mydata", SegmentRef{ID: "snap-1"}, files)
	defer it.Close()

	var refs []ObjectRef
	for it.Next() {
		refs = append(refs, it.Ref())
	}

	if it.Err() != nil {
		t.Errorf("unexpected error: %v", it.Err())
	}

	if len(refs) != 3 {
		t.Errorf("expected 3 refs, got %d", len(refs))
	}
}

func TestSegmentFileIterator_Empty(t *testing.T) {
	it := NewSegmentFileIterator("mydata", SegmentRef{ID: "snap-1"}, []lode.FileRef{})
	defer it.Close()

	if it.Next() {
		t.Error("Next() should return false for empty iterator")
	}

	if it.Err() != nil {
		t.Errorf("unexpected error: %v", it.Err())
	}
}

func TestSegmentFileIterator_CloseIdempotent(t *testing.T) {
	files := []lode.FileRef{{Path: "data/file.json", SizeBytes: 100}}
	it := NewSegmentFileIterator("mydata", SegmentRef{ID: "snap-1"}, files)

	// Close multiple times - should not panic or error
	for i := 0; i < 3; i++ {
		err := it.Close()
		if err != nil {
			t.Errorf("Close() iteration %d returned error: %v", i, err)
		}
	}
}

func TestSegmentFileIterator_NextAfterClose(t *testing.T) {
	files := []lode.FileRef{
		{Path: "data/file1.json", SizeBytes: 100},
		{Path: "data/file2.json", SizeBytes: 200},
	}
	it := NewSegmentFileIterator("mydata", SegmentRef{ID: "snap-1"}, files)

	// Advance once
	if !it.Next() {
		t.Fatal("expected Next() to return true")
	}

	// Close
	_ = it.Close()

	// Next() must return false after Close()
	if it.Next() {
		t.Error("Next() must return false after Close()")
	}
}

func TestSegmentFileIterator_NextAfterExhaustion(t *testing.T) {
	files := []lode.FileRef{{Path: "data/file.json", SizeBytes: 100}}
	it := NewSegmentFileIterator("mydata", SegmentRef{ID: "snap-1"}, files)
	defer it.Close()

	// Exhaust the iterator
	for it.Next() {
	}

	// Additional Next() calls must return false
	for i := 0; i < 3; i++ {
		if it.Next() {
			t.Errorf("Next() iteration %d should return false after exhaustion", i)
		}
	}
}

func TestSegmentFileIterator_ErrAfterExhaustion(t *testing.T) {
	files := []lode.FileRef{{Path: "data/file.json", SizeBytes: 100}}
	it := NewSegmentFileIterator("mydata", SegmentRef{ID: "snap-1"}, files)
	defer it.Close()

	// Exhaust
	for it.Next() {
	}

	// Err() must be callable after exhaustion
	if it.Err() != nil {
		t.Errorf("unexpected error after exhaustion: %v", it.Err())
	}
}

func TestSegmentFileIterator_ErrAfterClose(t *testing.T) {
	files := []lode.FileRef{{Path: "data/file.json", SizeBytes: 100}}
	it := NewSegmentFileIterator("mydata", SegmentRef{ID: "snap-1"}, files)

	_ = it.Close()

	// Err() must be callable after Close()
	if it.Err() != nil {
		t.Errorf("unexpected error after close: %v", it.Err())
	}
}

func TestSegmentFileIterator_RefContents(t *testing.T) {
	files := []lode.FileRef{
		{Path: "data/part=a/file.json", SizeBytes: 100},
	}
	it := NewSegmentFileIterator("mydata", SegmentRef{ID: "snap-1"}, files)
	defer it.Close()

	if !it.Next() {
		t.Fatal("expected Next() to return true")
	}

	ref := it.Ref()
	if ref.Dataset != "mydata" {
		t.Errorf("Dataset = %q, want %q", ref.Dataset, "mydata")
	}
	if ref.Segment.ID != "snap-1" {
		t.Errorf("Segment.ID = %q, want %q", ref.Segment.ID, "snap-1")
	}
	if ref.Path != "data/part=a/file.json" {
		t.Errorf("Path = %q, want %q", ref.Path, "data/part=a/file.json")
	}
}

// -----------------------------------------------------------------------------
// ListingIterator tests
// -----------------------------------------------------------------------------

func TestListingIterator_Basic(t *testing.T) {
	keys := []ObjectKey{"obj1", "obj2", "obj3"}
	it := NewListingIterator("mydata", SegmentRef{ID: "snap-1"}, keys)
	defer it.Close()

	count := 0
	for it.Next() {
		count++
	}

	if it.Err() != nil {
		t.Errorf("unexpected error: %v", it.Err())
	}

	if count != 3 {
		t.Errorf("expected 3 iterations, got %d", count)
	}
}

func TestListingIterator_CloseIdempotent(t *testing.T) {
	keys := []ObjectKey{"obj1"}
	it := NewListingIterator("mydata", SegmentRef{ID: "snap-1"}, keys)

	for i := 0; i < 3; i++ {
		err := it.Close()
		if err != nil {
			t.Errorf("Close() iteration %d returned error: %v", i, err)
		}
	}
}

func TestListingIterator_NextAfterClose(t *testing.T) {
	keys := []ObjectKey{"obj1", "obj2"}
	it := NewListingIterator("mydata", SegmentRef{ID: "snap-1"}, keys)

	if !it.Next() {
		t.Fatal("expected Next() to return true")
	}

	_ = it.Close()

	if it.Next() {
		t.Error("Next() must return false after Close()")
	}
}

func TestListingIterator_NextAfterExhaustion(t *testing.T) {
	keys := []ObjectKey{"obj1"}
	it := NewListingIterator("mydata", SegmentRef{ID: "snap-1"}, keys)
	defer it.Close()

	for it.Next() {
	}

	for i := 0; i < 3; i++ {
		if it.Next() {
			t.Errorf("Next() iteration %d should return false after exhaustion", i)
		}
	}
}

func TestListingIterator_ErrAfterExhaustion(t *testing.T) {
	keys := []ObjectKey{"obj1"}
	it := NewListingIterator("mydata", SegmentRef{ID: "snap-1"}, keys)
	defer it.Close()

	for it.Next() {
	}

	if it.Err() != nil {
		t.Errorf("unexpected error: %v", it.Err())
	}
}

func TestListingIterator_ErrAfterClose(t *testing.T) {
	keys := []ObjectKey{"obj1"}
	it := NewListingIterator("mydata", SegmentRef{ID: "snap-1"}, keys)

	_ = it.Close()

	if it.Err() != nil {
		t.Errorf("unexpected error: %v", it.Err())
	}
}

// -----------------------------------------------------------------------------
// EmptyIterator tests
// -----------------------------------------------------------------------------

func TestEmptyIterator_Basic(t *testing.T) {
	it := NewEmptyIterator()
	defer it.Close()

	if it.Next() {
		t.Error("EmptyIterator.Next() should always return false")
	}

	if it.Err() != nil {
		t.Errorf("unexpected error: %v", it.Err())
	}
}

func TestEmptyIterator_CloseIdempotent(t *testing.T) {
	it := NewEmptyIterator()

	for i := 0; i < 3; i++ {
		err := it.Close()
		if err != nil {
			t.Errorf("Close() iteration %d returned error: %v", i, err)
		}
	}
}

func TestEmptyIterator_NextAfterClose(t *testing.T) {
	it := NewEmptyIterator()

	_ = it.Close()

	if it.Next() {
		t.Error("Next() must return false after Close()")
	}
}

func TestEmptyIterator_ErrAfterClose(t *testing.T) {
	it := NewEmptyIterator()

	_ = it.Close()

	if it.Err() != nil {
		t.Errorf("unexpected error: %v", it.Err())
	}
}

// -----------------------------------------------------------------------------
// Contract compliance: No ordering guarantee tests
// These tests verify that tests don't accidentally depend on ordering.
// -----------------------------------------------------------------------------

func TestSegmentFileIterator_NoOrderingGuarantee(t *testing.T) {
	// This test documents that we make no ordering guarantees.
	// The implementation happens to iterate in slice order,
	// but consumers should not rely on this.
	files := []lode.FileRef{
		{Path: "c.json", SizeBytes: 100},
		{Path: "a.json", SizeBytes: 100},
		{Path: "b.json", SizeBytes: 100},
	}

	it := NewSegmentFileIterator("mydata", SegmentRef{ID: "snap-1"}, files)
	defer it.Close()

	var paths []string
	for it.Next() {
		paths = append(paths, it.Ref().Path)
	}

	// We verify we got all 3 paths, but NOT their order
	if len(paths) != 3 {
		t.Errorf("expected 3 paths, got %d", len(paths))
	}

	// Use set comparison, not ordered comparison
	pathSet := make(map[string]bool)
	for _, p := range paths {
		pathSet[p] = true
	}

	for _, f := range files {
		if !pathSet[f.Path] {
			t.Errorf("missing path: %s", f.Path)
		}
	}
}
