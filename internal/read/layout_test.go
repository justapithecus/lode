package read

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/justapithecus/lode/lode"
)

func TestDefaultLayout_DatasetsPrefix(t *testing.T) {
	layout := DefaultLayout{}
	got := layout.DatasetsPrefix()
	want := "datasets/"
	if got != want {
		t.Errorf("DatasetsPrefix() = %q, want %q", got, want)
	}
}

func TestDefaultLayout_SegmentsPrefix(t *testing.T) {
	layout := DefaultLayout{}
	got := layout.SegmentsPrefix("my-dataset")
	want := "datasets/my-dataset/snapshots/"
	if got != want {
		t.Errorf("SegmentsPrefix() = %q, want %q", got, want)
	}
}

func TestDefaultLayout_ManifestPath(t *testing.T) {
	layout := DefaultLayout{}
	got := layout.ManifestPath("my-dataset", "snap-1")
	want := "datasets/my-dataset/snapshots/snap-1/manifest.json"
	if got != want {
		t.Errorf("ManifestPath() = %q, want %q", got, want)
	}
}

func TestDefaultLayout_IsManifest(t *testing.T) {
	layout := DefaultLayout{}
	tests := []struct {
		path string
		want bool
	}{
		// Valid canonical paths
		{"datasets/foo/snapshots/bar/manifest.json", true},
		{"datasets/my-dataset/snapshots/snap-1/manifest.json", true},
		// Invalid: wrong structure (stray manifests)
		{"manifest.json", false},
		{"some/path/manifest.json", false},
		{"datasets/foo/misc/manifest.json", false},
		{"datasets/foo/snapshots/manifest.json", false}, // missing segment
		{"datasets/foo/snapshots/bar/data/manifest.json", false}, // too deep
		// Invalid: wrong filename
		{"datasets/foo/snapshots/bar/data.json", false},
		{"datasets/foo/snapshots/bar/manifest.txt", false},
		// Invalid: empty
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.IsManifest(tt.path)
			if got != tt.want {
				t.Errorf("IsManifest(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestDefaultLayout_ParseDatasetID(t *testing.T) {
	layout := DefaultLayout{}
	tests := []struct {
		path string
		want lode.DatasetID
	}{
		// Valid canonical paths
		{"datasets/my-dataset/snapshots/snap-1/manifest.json", "my-dataset"},
		{"datasets/foo/snapshots/bar/manifest.json", "foo"},
		// Invalid: wrong structure
		{"snapshots/bar/manifest.json", ""},
		{"datasets/manifest.json", ""},
		{"datasets/foo/misc/manifest.json", ""},         // missing /snapshots/
		{"datasets/foo/snapshots/manifest.json", ""},    // missing segment
		{"datasets/foo/snapshots/bar/data/manifest.json", ""}, // too deep
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.ParseDatasetID(tt.path)
			if got != tt.want {
				t.Errorf("ParseDatasetID(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestDefaultLayout_ParseSegmentID(t *testing.T) {
	layout := DefaultLayout{}
	tests := []struct {
		path string
		want lode.SnapshotID
	}{
		// Valid canonical paths
		{"datasets/my-dataset/snapshots/snap-1/manifest.json", "snap-1"},
		{"datasets/foo/snapshots/bar/manifest.json", "bar"},
		// Invalid: wrong structure
		{"some/path/seg-id/manifest.json", ""},
		{"datasets/foo/misc/manifest.json", ""},
		{"datasets/foo/snapshots/manifest.json", ""},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.ParseSegmentID(tt.path)
			if got != tt.want {
				t.Errorf("ParseSegmentID(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestDefaultLayout_ExtractPartitionPath(t *testing.T) {
	layout := DefaultLayout{}
	tests := []struct {
		path string
		want string
	}{
		// With partition
		{"datasets/ds/snapshots/snap/data/day=2024-01-01/file.json", "day=2024-01-01"},
		{"datasets/ds/snapshots/snap/data/day=2024-01-01/hour=12/file.json", "day=2024-01-01/hour=12"},
		// Without partition
		{"datasets/ds/snapshots/snap/data/file.json", ""},
		// No data directory
		{"datasets/ds/snapshots/snap/file.json", ""},
		// Empty
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := layout.ExtractPartitionPath(tt.path)
			if got != tt.want {
				t.Errorf("ExtractPartitionPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestNewReaderWithLayout_NilLayout(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewReaderWithLayout(store, nil) did not panic")
		}
	}()

	store := &mockStore{}
	NewReaderWithLayout(store, nil)
}

// mockStore implements lode.Store for testing
type mockStore struct{}

func (m *mockStore) Put(_ context.Context, _ string, _ io.Reader) error     { return nil }
func (m *mockStore) Get(_ context.Context, _ string) (io.ReadCloser, error) { return nil, nil }
func (m *mockStore) Exists(_ context.Context, _ string) (bool, error)       { return false, nil }
func (m *mockStore) List(_ context.Context, _ string) ([]string, error)     { return nil, nil }
func (m *mockStore) Delete(_ context.Context, _ string) error               { return nil }

// -----------------------------------------------------------------------------
// Custom Layout Tests - Prove Reader honors custom layouts
// -----------------------------------------------------------------------------

// customLayout implements Layout with a different path structure:
// custom/<dataset>/segs/<segment>/meta.json
type customLayout struct {
	datasetsPrefix string
	listCalls      []string
	getManifestReq []string
}

func (c *customLayout) DatasetsPrefix() string {
	return c.datasetsPrefix
}

func (c *customLayout) SegmentsPrefix(dataset lode.DatasetID) string {
	prefix := "custom/" + string(dataset) + "/segs/"
	c.listCalls = append(c.listCalls, prefix)
	return prefix
}

func (c *customLayout) ManifestPath(dataset lode.DatasetID, segment lode.SnapshotID) string {
	path := "custom/" + string(dataset) + "/segs/" + string(segment) + "/meta.json"
	c.getManifestReq = append(c.getManifestReq, path)
	return path
}

func (c *customLayout) IsManifest(p string) bool {
	// Custom layout uses meta.json instead of manifest.json
	parts := splitPath(p)
	if len(parts) != 5 {
		return false
	}
	return parts[0] == "custom" && parts[2] == "segs" && parts[4] == "meta.json"
}

func (c *customLayout) ParseDatasetID(manifestPath string) lode.DatasetID {
	parts := splitPath(manifestPath)
	if len(parts) != 5 || parts[0] != "custom" || parts[2] != "segs" || parts[4] != "meta.json" {
		return ""
	}
	return lode.DatasetID(parts[1])
}

func (c *customLayout) ParseSegmentID(manifestPath string) lode.SnapshotID {
	parts := splitPath(manifestPath)
	if len(parts) != 5 || parts[0] != "custom" || parts[2] != "segs" || parts[4] != "meta.json" {
		return ""
	}
	return lode.SnapshotID(parts[3])
}

func (c *customLayout) ExtractPartitionPath(_ string) string {
	return "" // Custom layout doesn't support partitions
}

func splitPath(p string) []string {
	if p == "" {
		return nil
	}
	var parts []string
	for _, part := range strings.Split(p, "/") {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

// trackingStore records calls for verification
type trackingStore struct {
	listPrefix string
	listResult []string
	getCalls   []string
}

func (t *trackingStore) Put(_ context.Context, _ string, _ io.Reader) error { return nil }
func (t *trackingStore) Get(_ context.Context, path string) (io.ReadCloser, error) {
	t.getCalls = append(t.getCalls, path)
	return nil, lode.ErrNotFound
}
func (t *trackingStore) Exists(_ context.Context, _ string) (bool, error) { return false, nil }
func (t *trackingStore) List(_ context.Context, prefix string) ([]string, error) {
	t.listPrefix = prefix
	return t.listResult, nil
}
func (t *trackingStore) Delete(_ context.Context, _ string) error { return nil }

func TestReader_UsesCustomLayout_ListDatasets(t *testing.T) {
	store := &trackingStore{
		listResult: []string{
			"custom/ds1/segs/seg1/meta.json",
			"custom/ds2/segs/seg2/meta.json",
		},
	}
	layout := &customLayout{datasetsPrefix: "custom/"}

	reader := NewReaderWithLayout(store, layout)
	datasets, err := reader.ListDatasets(context.Background(), DatasetListOptions{})
	if err != nil {
		t.Fatalf("ListDatasets failed: %v", err)
	}

	// Verify layout's DatasetsPrefix was used
	if store.listPrefix != "custom/" {
		t.Errorf("Expected list prefix %q, got %q", "custom/", store.listPrefix)
	}

	// Verify layout's IsManifest and ParseDatasetID were used
	if len(datasets) != 2 {
		t.Errorf("Expected 2 datasets, got %d", len(datasets))
	}
}

func TestReader_UsesCustomLayout_ListSegments(t *testing.T) {
	store := &trackingStore{
		listResult: []string{
			"custom/mydata/segs/seg-a/meta.json",
			"custom/mydata/segs/seg-b/meta.json",
		},
	}
	layout := &customLayout{datasetsPrefix: "custom/"}

	reader := NewReaderWithLayout(store, layout)
	segments, err := reader.ListSegments(context.Background(), "mydata", "", SegmentListOptions{})
	if err != nil {
		t.Fatalf("ListSegments failed: %v", err)
	}

	// Verify layout's SegmentsPrefix was called with correct dataset
	expectedPrefix := "custom/mydata/segs/"
	if store.listPrefix != expectedPrefix {
		t.Errorf("Expected list prefix %q, got %q", expectedPrefix, store.listPrefix)
	}

	// Verify layout's IsManifest and ParseSegmentID were used
	if len(segments) != 2 {
		t.Errorf("Expected 2 segments, got %d", len(segments))
	}
	if segments[0].ID != "seg-a" {
		t.Errorf("Expected segment ID %q, got %q", "seg-a", segments[0].ID)
	}
}

func TestReader_UsesCustomLayout_GetManifest(t *testing.T) {
	store := &trackingStore{}
	layout := &customLayout{datasetsPrefix: "custom/"}

	reader := NewReaderWithLayout(store, layout)
	// This will fail with ErrNotFound, but we just want to verify the path
	_, _ = reader.GetManifest(context.Background(), "mydata", SegmentRef{ID: "seg-1"})

	// Verify layout's ManifestPath was called
	if len(store.getCalls) != 1 {
		t.Fatalf("Expected 1 Get call, got %d", len(store.getCalls))
	}

	expectedPath := "custom/mydata/segs/seg-1/meta.json"
	if store.getCalls[0] != expectedPath {
		t.Errorf("Expected Get path %q, got %q", expectedPath, store.getCalls[0])
	}
}

func TestReader_UsesCustomLayout_RejectsStrayManifests(t *testing.T) {
	// Store returns paths that don't match custom layout
	store := &trackingStore{
		listResult: []string{
			"custom/ds1/segs/seg1/meta.json",          // valid
			"custom/ds1/misc/meta.json",               // invalid - wrong structure
			"datasets/ds2/snapshots/s1/manifest.json", // invalid - wrong layout
		},
	}
	layout := &customLayout{datasetsPrefix: "custom/"}

	reader := NewReaderWithLayout(store, layout)
	datasets, err := reader.ListDatasets(context.Background(), DatasetListOptions{})
	if err != nil {
		t.Fatalf("ListDatasets failed: %v", err)
	}

	// Only ds1 should be found (the valid one)
	if len(datasets) != 1 {
		t.Errorf("Expected 1 dataset (stray manifests rejected), got %d", len(datasets))
	}
	if len(datasets) > 0 && datasets[0] != "ds1" {
		t.Errorf("Expected dataset %q, got %q", "ds1", datasets[0])
	}
}

// TestDefaultLayout_RejectsStrayManifests verifies that stray manifest.json files
// outside the canonical /snapshots/ structure don't create false dataset existence.
// This is the regression test for the "layout parsing too permissive" bug.
func TestDefaultLayout_RejectsStrayManifests(t *testing.T) {
	store := &trackingStore{
		listResult: []string{
			"datasets/ds1/snapshots/seg1/manifest.json", // valid
			"datasets/ds2/misc/manifest.json",           // invalid - missing /snapshots/
			"datasets/ds3/snapshots/manifest.json",      // invalid - missing segment
			"datasets/ds4/snapshots/seg/sub/manifest.json", // invalid - too deep
		},
	}

	reader := NewReader(store)
	datasets, err := reader.ListDatasets(context.Background(), DatasetListOptions{})
	if err != nil {
		t.Fatalf("ListDatasets failed: %v", err)
	}

	// Only ds1 should be found
	if len(datasets) != 1 {
		t.Errorf("Expected 1 dataset (stray manifests rejected), got %d: %v", len(datasets), datasets)
	}
	if len(datasets) > 0 && datasets[0] != "ds1" {
		t.Errorf("Expected dataset %q, got %q", "ds1", datasets[0])
	}
}
