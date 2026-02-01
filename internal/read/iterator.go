package read

import (
	"github.com/justapithecus/lode/lode"
)

// ObjectIterator provides sequential access to objects.
//
// Per CONTRACT_ITERATION.md:
//   - Ordering is unspecified
//   - Pagination is unspecified
//   - Next() MUST return false after exhaustion or after Close() is called
//   - Close() MUST be idempotent
//   - Err() MAY be called after exhaustion or close
//   - Implementations MUST release resources on Close() or exhaustion
//
// Prohibited behaviors:
//   - Implicit ordering guarantees
//   - Hidden buffering that changes visibility semantics
type ObjectIterator interface {
	// Next advances to the next object.
	// Returns true if there is a next object, false if exhausted or closed.
	// Must return false after Close() is called.
	Next() bool

	// Ref returns the current object reference.
	// Only valid after Next() returns true.
	Ref() ObjectRef

	// Err returns any error encountered during iteration.
	// May be called after exhaustion or close.
	Err() error

	// Close releases resources held by the iterator.
	// Must be idempotent (safe to call multiple times).
	Close() error
}

// SegmentFileIterator iterates over files in a segment's manifest.
//
// This iterator does not guarantee any ordering of files.
// It does not perform hidden buffering that changes visibility.
type SegmentFileIterator struct {
	dataset lode.DatasetID
	segment SegmentRef
	files   []lode.FileRef
	index   int
	current ObjectRef
	err     error
	closed  bool
}

// NewSegmentFileIterator creates an iterator over the files in a manifest.
// The iterator takes ownership of the files slice and will not modify it.
func NewSegmentFileIterator(dataset lode.DatasetID, segment SegmentRef, files []lode.FileRef) *SegmentFileIterator {
	return &SegmentFileIterator{
		dataset: dataset,
		segment: segment,
		files:   files,
		index:   -1, // Start before first element
	}
}

// Next advances to the next file.
// Returns false if exhausted or if Close() has been called.
func (it *SegmentFileIterator) Next() bool {
	// Per contract: Next() must return false after Close()
	if it.closed {
		return false
	}

	it.index++

	// Check for exhaustion
	if it.index >= len(it.files) {
		return false
	}

	// Build current ObjectRef
	it.current = ObjectRef{
		Dataset: it.dataset,
		Segment: it.segment,
		Path:    it.files[it.index].Path,
	}

	return true
}

// Ref returns the current object reference.
// Only valid after Next() returns true.
func (it *SegmentFileIterator) Ref() ObjectRef {
	return it.current
}

// Err returns any error encountered during iteration.
// For this simple iterator, errors are not expected during iteration,
// but the method is provided per the contract.
func (it *SegmentFileIterator) Err() error {
	return it.err
}

// Close releases resources and marks the iterator as closed.
// Idempotent: safe to call multiple times.
func (it *SegmentFileIterator) Close() error {
	it.closed = true
	// Release reference to files slice to allow GC
	it.files = nil
	return nil
}

// Ensure SegmentFileIterator implements ObjectIterator
var _ ObjectIterator = (*SegmentFileIterator)(nil)

// ListingIterator iterates over objects from a storage listing.
//
// This iterator wraps a slice of ObjectKeys from a List operation.
// It does not guarantee any ordering.
type ListingIterator struct {
	dataset lode.DatasetID
	segment SegmentRef
	keys    []ObjectKey
	index   int
	current ObjectRef
	err     error
	closed  bool
}

// NewListingIterator creates an iterator over listing results.
func NewListingIterator(dataset lode.DatasetID, segment SegmentRef, keys []ObjectKey) *ListingIterator {
	return &ListingIterator{
		dataset: dataset,
		segment: segment,
		keys:    keys,
		index:   -1,
	}
}

// Next advances to the next object.
func (it *ListingIterator) Next() bool {
	if it.closed {
		return false
	}

	it.index++

	if it.index >= len(it.keys) {
		return false
	}

	it.current = ObjectRef{
		Dataset: it.dataset,
		Segment: it.segment,
		Path:    string(it.keys[it.index]),
	}

	return true
}

// Ref returns the current object reference.
func (it *ListingIterator) Ref() ObjectRef {
	return it.current
}

// Err returns any error encountered during iteration.
func (it *ListingIterator) Err() error {
	return it.err
}

// Close releases resources and marks the iterator as closed.
// Idempotent.
func (it *ListingIterator) Close() error {
	it.closed = true
	it.keys = nil
	return nil
}

// Ensure ListingIterator implements ObjectIterator
var _ ObjectIterator = (*ListingIterator)(nil)

// EmptyIterator is an iterator that yields no objects.
// Useful for empty results without allocating state.
type EmptyIterator struct {
	closed bool
}

// NewEmptyIterator creates an iterator that yields no objects.
func NewEmptyIterator() *EmptyIterator {
	return &EmptyIterator{}
}

// Next always returns false.
func (it *EmptyIterator) Next() bool {
	return false
}

// Ref returns a zero ObjectRef.
func (it *EmptyIterator) Ref() ObjectRef {
	return ObjectRef{}
}

// Err returns nil.
func (it *EmptyIterator) Err() error {
	return nil
}

// Close marks the iterator as closed. Idempotent.
func (it *EmptyIterator) Close() error {
	it.closed = true
	return nil
}

// Ensure EmptyIterator implements ObjectIterator
var _ ObjectIterator = (*EmptyIterator)(nil)
