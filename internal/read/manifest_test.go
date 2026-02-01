package read

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/justapithecus/lode/internal/storage"
	"github.com/justapithecus/lode/lode"
)

func TestValidateManifest_Valid(t *testing.T) {
	m := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-dataset",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files:         []lode.FileRef{},
		RowCount:      0,
		Codec:         "jsonl",
		Compressor:    "noop",
		Partitioner:   "noop",
	}

	if err := ValidateManifest(m); err != nil {
		t.Errorf("expected valid manifest, got error: %v", err)
	}
}

func TestValidateManifest_ValidWithFiles(t *testing.T) {
	m := &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-dataset",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{"key": "value"},
		Files: []lode.FileRef{
			{Path: "data/file1.json", SizeBytes: 100},
			{Path: "data/file2.json", SizeBytes: 200, Checksum: "sha256:abc123"},
		},
		RowCount:    50,
		Codec:       "jsonl",
		Compressor:  "gzip",
		Partitioner: "hive-dt",
	}

	if err := ValidateManifest(m); err != nil {
		t.Errorf("expected valid manifest, got error: %v", err)
	}
}

func TestValidateManifest_Nil(t *testing.T) {
	err := ValidateManifest(nil)
	if err == nil {
		t.Error("expected error for nil manifest")
	}
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got %v", err)
	}
}

func TestValidateManifest_MissingSchemaName(t *testing.T) {
	m := validManifest()
	m.SchemaName = ""

	err := ValidateManifest(m)
	assertValidationError(t, err, "schema_name")
}

func TestValidateManifest_MissingFormatVersion(t *testing.T) {
	m := validManifest()
	m.FormatVersion = ""

	err := ValidateManifest(m)
	assertValidationError(t, err, "format_version")
}

func TestValidateManifest_MissingDatasetID(t *testing.T) {
	m := validManifest()
	m.DatasetID = ""

	err := ValidateManifest(m)
	assertValidationError(t, err, "dataset_id")
}

func TestValidateManifest_MissingSnapshotID(t *testing.T) {
	m := validManifest()
	m.SnapshotID = ""

	err := ValidateManifest(m)
	assertValidationError(t, err, "snapshot_id")
}

func TestValidateManifest_ZeroCreatedAt(t *testing.T) {
	m := validManifest()
	m.CreatedAt = time.Time{}

	err := ValidateManifest(m)
	assertValidationError(t, err, "created_at")
}

func TestValidateManifest_NilMetadata(t *testing.T) {
	m := validManifest()
	m.Metadata = nil

	err := ValidateManifest(m)
	assertValidationError(t, err, "metadata")
}

func TestValidateManifest_NilFiles(t *testing.T) {
	m := validManifest()
	m.Files = nil

	err := ValidateManifest(m)
	assertValidationError(t, err, "files")
}

func TestValidateManifest_NegativeRowCount(t *testing.T) {
	m := validManifest()
	m.RowCount = -1

	err := ValidateManifest(m)
	assertValidationError(t, err, "row_count")
}

func TestValidateManifest_MissingCodec(t *testing.T) {
	m := validManifest()
	m.Codec = ""

	err := ValidateManifest(m)
	assertValidationError(t, err, "codec")
}

func TestValidateManifest_MissingCompressor(t *testing.T) {
	m := validManifest()
	m.Compressor = ""

	err := ValidateManifest(m)
	assertValidationError(t, err, "compressor")
}

func TestValidateManifest_MissingPartitioner(t *testing.T) {
	m := validManifest()
	m.Partitioner = ""

	err := ValidateManifest(m)
	assertValidationError(t, err, "partitioner")
}

func TestValidateManifest_FileWithEmptyPath(t *testing.T) {
	m := validManifest()
	m.Files = []lode.FileRef{
		{Path: "", SizeBytes: 100},
	}

	err := ValidateManifest(m)
	assertValidationError(t, err, "files[0].path")
}

func TestValidateManifest_FileWithNegativeSize(t *testing.T) {
	m := validManifest()
	m.Files = []lode.FileRef{
		{Path: "data/file.json", SizeBytes: -1},
	}

	err := ValidateManifest(m)
	assertValidationError(t, err, "files[0].size_bytes")
}

func TestValidateManifest_SecondFileInvalid(t *testing.T) {
	m := validManifest()
	m.Files = []lode.FileRef{
		{Path: "data/file1.json", SizeBytes: 100},
		{Path: "", SizeBytes: 200}, // invalid
	}

	err := ValidateManifest(m)
	assertValidationError(t, err, "files[1].path")
}

// -----------------------------------------------------------------------------
// Integration tests: GetManifest with validation
// -----------------------------------------------------------------------------

func TestGetManifest_ValidationError_MissingSchemaName(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write manifest missing schema_name
	manifest := map[string]any{
		"format_version": "1.0.0",
		"dataset_id":     "mydata",
		"snapshot_id":    "snap-1",
		"created_at":     time.Now().UTC(),
		"metadata":       map[string]any{},
		"files":          []any{},
		"row_count":      0,
		"codec":          "jsonl",
		"compressor":     "noop",
		"partitioner":    "noop",
	}

	data, _ := json.Marshal(manifest)
	err := store.Put(ctx, "datasets/mydata/snapshots/snap-1/manifest.json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	reader := NewReader(store)
	_, err = reader.GetManifest(ctx, "mydata", SegmentRef{ID: "snap-1"})
	if err == nil {
		t.Error("expected validation error")
	}
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got %v", err)
	}
}

func TestGetManifest_ValidationError_NilMetadata(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write manifest with null metadata (nil when decoded)
	manifest := map[string]any{
		"schema_name":    "lode-manifest",
		"format_version": "1.0.0",
		"dataset_id":     "mydata",
		"snapshot_id":    "snap-1",
		"created_at":     time.Now().UTC(),
		"metadata":       nil, // explicitly null
		"files":          []any{},
		"row_count":      0,
		"codec":          "jsonl",
		"compressor":     "noop",
		"partitioner":    "noop",
	}

	data, _ := json.Marshal(manifest)
	err := store.Put(ctx, "datasets/mydata/snapshots/snap-1/manifest.json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	reader := NewReader(store)
	_, err = reader.GetManifest(ctx, "mydata", SegmentRef{ID: "snap-1"})
	if err == nil {
		t.Error("expected validation error for nil metadata")
	}
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got %v", err)
	}
}

func TestGetManifest_ValidationError_MissingCodec(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write manifest missing codec
	manifest := map[string]any{
		"schema_name":    "lode-manifest",
		"format_version": "1.0.0",
		"dataset_id":     "mydata",
		"snapshot_id":    "snap-1",
		"created_at":     time.Now().UTC(),
		"metadata":       map[string]any{},
		"files":          []any{},
		"row_count":      0,
		// codec missing
		"compressor":  "noop",
		"partitioner": "noop",
	}

	data, _ := json.Marshal(manifest)
	err := store.Put(ctx, "datasets/mydata/snapshots/snap-1/manifest.json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to write manifest: %v", err)
	}

	reader := NewReader(store)
	_, err = reader.GetManifest(ctx, "mydata", SegmentRef{ID: "snap-1"})
	if err == nil {
		t.Error("expected validation error for missing codec")
	}
	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got %v", err)
	}
}

func TestGetManifest_DecodeError_InvalidJSON(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemory()

	// Write invalid JSON
	err := store.Put(ctx, "datasets/bad/snapshots/snap-1/manifest.json", bytes.NewReader([]byte("not json")))
	if err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}

	reader := NewReader(store)
	_, err = reader.GetManifest(ctx, "bad", SegmentRef{ID: "snap-1"})
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
	// Should not be ErrManifestInvalid - it's a decode error
	if errors.Is(err, ErrManifestInvalid) {
		t.Error("decode error should not be ErrManifestInvalid")
	}
}

// -----------------------------------------------------------------------------
// Optional fields tests
// -----------------------------------------------------------------------------

func TestValidateManifest_OptionalParentSnapshotID(t *testing.T) {
	m := validManifest()
	m.ParentSnapshotID = "" // optional, empty is valid

	if err := ValidateManifest(m); err != nil {
		t.Errorf("expected valid manifest (empty parent is optional), got error: %v", err)
	}

	m.ParentSnapshotID = "snap-0" // also valid when set
	if err := ValidateManifest(m); err != nil {
		t.Errorf("expected valid manifest (parent set), got error: %v", err)
	}
}

func TestValidateManifest_OptionalTimestamps(t *testing.T) {
	m := validManifest()
	m.MinTimestamp = nil
	m.MaxTimestamp = nil

	if err := ValidateManifest(m); err != nil {
		t.Errorf("expected valid manifest (nil timestamps are optional), got error: %v", err)
	}

	now := time.Now().UTC()
	m.MinTimestamp = &now
	m.MaxTimestamp = &now

	if err := ValidateManifest(m); err != nil {
		t.Errorf("expected valid manifest (timestamps set), got error: %v", err)
	}
}

func TestValidateManifest_OptionalChecksum(t *testing.T) {
	m := validManifest()
	m.Files = []lode.FileRef{
		{Path: "data/file.json", SizeBytes: 100, Checksum: ""}, // optional
	}

	if err := ValidateManifest(m); err != nil {
		t.Errorf("expected valid manifest (checksum optional), got error: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Helper functions
// -----------------------------------------------------------------------------

func validManifest() *lode.Manifest {
	return &lode.Manifest{
		SchemaName:    "lode-manifest",
		FormatVersion: "1.0.0",
		DatasetID:     "test-dataset",
		SnapshotID:    "snap-1",
		CreatedAt:     time.Now().UTC(),
		Metadata:      lode.Metadata{},
		Files:         []lode.FileRef{},
		RowCount:      0,
		Codec:         "jsonl",
		Compressor:    "noop",
		Partitioner:   "noop",
	}
}

func assertValidationError(t *testing.T, err error, expectedField string) {
	t.Helper()

	if err == nil {
		t.Errorf("expected validation error for field %q, got nil", expectedField)
		return
	}

	if !errors.Is(err, ErrManifestInvalid) {
		t.Errorf("expected ErrManifestInvalid, got %v", err)
		return
	}

	var valErr *ManifestValidationError
	if !errors.As(err, &valErr) {
		t.Errorf("expected ManifestValidationError, got %T", err)
		return
	}

	if valErr.Field != expectedField {
		t.Errorf("expected field %q, got %q", expectedField, valErr.Field)
	}
}
