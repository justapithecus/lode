package read

import (
	"context"
	"io"

	"github.com/justapithecus/lode/internal/storage"
	"github.com/justapithecus/lode/lode"
)

// RangeReadStore is an optional interface that storage adapters can implement
// to support efficient range reads. Per CONTRACT_READ_API.md, range reads
// must be true range reads, not simulated full downloads.
//
// Any lode.Store implementation that also implements this interface will
// automatically have range read capabilities enabled.
type RangeReadStore interface {
	lode.Store

	// Stat returns the size of the object at the given path.
	Stat(ctx context.Context, path string) (int64, error)

	// ReadRange reads a byte range from the object at the given path.
	// Must be a true range read (e.g., seek+read, HTTP Range header).
	ReadRange(ctx context.Context, path string, offset int64, length int64) ([]byte, error)

	// ReaderAt returns a random-access reader for the object.
	// The returned value must implement io.ReaderAt, io.Closer, and have a Size() method.
	ReaderAt(ctx context.Context, path string) (storage.SizedReaderAt, error)
}

// SizedReaderAt is an alias for storage.SizedReaderAt for convenience.
type SizedReaderAt = storage.SizedReaderAt

// StoreAdapter adapts a lode.Store to the ReadStorage interface.
// If the underlying store implements RangeReadStore, those methods will be used.
// Otherwise, range operations will return ErrRangeReadNotSupported.
type StoreAdapter struct {
	store          lode.Store
	rangeReadStore RangeReadStore // may be nil
}

// NewStoreAdapter creates a ReadStorage adapter for the given store.
// It automatically detects if the store supports range reads via the
// RangeReadStore interface. This is backend-agnostic - any store that
// implements the interface will have range reads enabled.
func NewStoreAdapter(store lode.Store) *StoreAdapter {
	adapter := &StoreAdapter{store: store}

	// Check if store supports range reads via interface assertion
	// This is backend-agnostic - works for any store implementing RangeReadStore
	if rrs, ok := store.(RangeReadStore); ok {
		adapter.rangeReadStore = rrs
	}

	return adapter
}

// Stat returns metadata about an object.
func (a *StoreAdapter) Stat(ctx context.Context, key ObjectKey) (ObjectInfo, error) {
	if a.rangeReadStore != nil {
		size, err := a.rangeReadStore.Stat(ctx, string(key))
		if err != nil {
			return ObjectInfo{}, err
		}
		return ObjectInfo{Key: key, SizeBytes: size}, nil
	}

	// Fallback: open and check size (less efficient)
	rc, err := a.store.Get(ctx, string(key))
	if err != nil {
		return ObjectInfo{}, err
	}
	defer func() { _ = rc.Close() }()

	// Read to get size - not ideal but works as fallback
	data, err := io.ReadAll(rc)
	if err != nil {
		return ObjectInfo{}, err
	}

	return ObjectInfo{Key: key, SizeBytes: int64(len(data))}, nil
}

// Open returns a reader for the entire object.
func (a *StoreAdapter) Open(ctx context.Context, key ObjectKey) (io.ReadCloser, error) {
	return a.store.Get(ctx, string(key))
}

// ReadRange reads a byte range from an object.
func (a *StoreAdapter) ReadRange(ctx context.Context, key ObjectKey, offset int64, length int64) ([]byte, error) {
	if a.rangeReadStore == nil {
		return nil, ErrRangeReadNotSupported
	}
	return a.rangeReadStore.ReadRange(ctx, string(key), offset, length)
}

// ReaderAt returns a random-access reader for an object.
func (a *StoreAdapter) ReaderAt(ctx context.Context, key ObjectKey) (ReaderAt, error) {
	if a.rangeReadStore == nil {
		return nil, ErrRangeReadNotSupported
	}
	return a.rangeReadStore.ReaderAt(ctx, string(key))
}

// List returns objects matching the given prefix.
func (a *StoreAdapter) List(ctx context.Context, prefix ObjectKey, opts ListOptions) (ListPage, error) {
	paths, err := a.store.List(ctx, string(prefix))
	if err != nil {
		return ListPage{}, err
	}

	keys := make([]ObjectKey, len(paths))
	for i, p := range paths {
		keys[i] = ObjectKey(p)
	}

	// Apply limit if specified
	hasMore := false
	if opts.Limit > 0 && len(keys) > opts.Limit {
		keys = keys[:opts.Limit]
		hasMore = true
	}

	return ListPage{Keys: keys, HasMore: hasMore}, nil
}

// SupportsRangeReads returns true if this adapter supports range reads.
func (a *StoreAdapter) SupportsRangeReads() bool {
	return a.rangeReadStore != nil
}

// Ensure StoreAdapter implements ReadStorage
var _ ReadStorage = (*StoreAdapter)(nil)
