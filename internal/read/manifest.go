package read

import (
	"errors"
	"fmt"

	"github.com/justapithecus/lode/lode"
)

// Manifest validation errors.
var (
	// ErrManifestInvalid indicates a manifest failed validation.
	ErrManifestInvalid = errors.New("invalid manifest")
)

// ManifestValidationError provides details about manifest validation failures.
type ManifestValidationError struct {
	Field   string
	Message string
}

func (e *ManifestValidationError) Error() string {
	return fmt.Sprintf("invalid manifest: %s: %s", e.Field, e.Message)
}

func (e *ManifestValidationError) Unwrap() error {
	return ErrManifestInvalid
}

// ValidateManifest checks that a manifest contains all required fields
// per CONTRACT_CORE.md and CONTRACT_READ_API.md.
//
// Required fields:
//   - SchemaName: identifies the manifest schema
//   - FormatVersion: identifies the schema version
//   - DatasetID: identifies the dataset
//   - SnapshotID: identifies the snapshot
//   - CreatedAt: when the snapshot was committed (must not be zero)
//   - Metadata: user-provided key-value pairs (must not be nil)
//   - Files: list of data files (must not be nil, may be empty)
//   - RowCount: total records (must be >= 0)
//   - Codec: serialization format
//   - Compressor: compression format
//   - Partitioner: partitioning strategy
//
// Optional fields (not validated as required):
//   - ParentSnapshotID: previous snapshot reference
//   - MinTimestamp/MaxTimestamp: applicable only for timestamped records
//   - File checksums: optional per CONTRACT_CORE.md
func ValidateManifest(m *lode.Manifest) error {
	if m == nil {
		return &ManifestValidationError{Field: "manifest", Message: "is nil"}
	}

	// Schema identification
	if m.SchemaName == "" {
		return &ManifestValidationError{Field: "schema_name", Message: "is required"}
	}
	if m.FormatVersion == "" {
		return &ManifestValidationError{Field: "format_version", Message: "is required"}
	}

	// Identity fields
	if m.DatasetID == "" {
		return &ManifestValidationError{Field: "dataset_id", Message: "is required"}
	}
	if m.SnapshotID == "" {
		return &ManifestValidationError{Field: "snapshot_id", Message: "is required"}
	}

	// Timestamp
	if m.CreatedAt.IsZero() {
		return &ManifestValidationError{Field: "created_at", Message: "is required"}
	}

	// Metadata - per CONTRACT_CORE.md: nil metadata is invalid
	if m.Metadata == nil {
		return &ManifestValidationError{Field: "metadata", Message: "must not be nil (use empty map for no metadata)"}
	}

	// Files list - must not be nil (empty is valid for zero-record snapshots)
	if m.Files == nil {
		return &ManifestValidationError{Field: "files", Message: "must not be nil (use empty slice for no files)"}
	}

	// Row count - must be non-negative
	if m.RowCount < 0 {
		return &ManifestValidationError{Field: "row_count", Message: "must be non-negative"}
	}

	// Component fields - per CONTRACT_LAYOUT.md: must be explicit, never nil/empty
	if m.Codec == "" {
		return &ManifestValidationError{Field: "codec", Message: "is required"}
	}
	if m.Compressor == "" {
		return &ManifestValidationError{Field: "compressor", Message: "is required"}
	}
	if m.Partitioner == "" {
		return &ManifestValidationError{Field: "partitioner", Message: "is required"}
	}

	// Validate individual file references
	for i, f := range m.Files {
		if f.Path == "" {
			return &ManifestValidationError{
				Field:   fmt.Sprintf("files[%d].path", i),
				Message: "is required",
			}
		}
		if f.SizeBytes < 0 {
			return &ManifestValidationError{
				Field:   fmt.Sprintf("files[%d].size_bytes", i),
				Message: "must be non-negative",
			}
		}
	}

	return nil
}
