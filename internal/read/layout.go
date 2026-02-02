package read

import (
	"path"
	"strings"

	"github.com/justapithecus/lode/lode"
)

// Layout abstracts storage path construction.
//
// Per CONTRACT_READ_API.md, layouts are pluggable but must ensure:
//   - Manifests remain discoverable via listing
//   - Object paths in manifests are accurate and resolvable
//   - Commit semantics (manifest presence = visibility) are preserved
//
// Alternative layouts (e.g., partitions nested inside segments) are valid
// provided these invariants hold.
type Layout interface {
	// DatasetsPrefix returns the storage prefix for listing all datasets.
	DatasetsPrefix() string

	// SegmentsPrefix returns the storage prefix for listing segments in a dataset.
	SegmentsPrefix(dataset lode.DatasetID) string

	// ManifestPath returns the storage path for a segment's manifest.
	ManifestPath(dataset lode.DatasetID, segment lode.SnapshotID) string

	// IsManifest returns true if the path is a manifest file.
	IsManifest(p string) bool

	// ParseDatasetID extracts the dataset ID from a manifest path.
	// Returns empty string if the path is not a valid manifest path.
	ParseDatasetID(manifestPath string) lode.DatasetID

	// ParseSegmentID extracts the segment ID from a manifest path.
	// Returns empty string if the path is not a valid manifest path.
	ParseSegmentID(manifestPath string) lode.SnapshotID

	// ExtractPartitionPath extracts the partition path from a file path.
	// Returns empty string if no partition.
	ExtractPartitionPath(filePath string) string
}

// DefaultLayout implements the reference layout from CONTRACT_READ_API.md:
//
//	/datasets/<dataset>/snapshots/<segment_id>/
//	  manifest.json
//	  /data/
//	    [partition/]filename
//
// This layout nests partitions inside data directories within segments.
type DefaultLayout struct{}

// Default layout constants.
const (
	defaultDatasetsDir  = "datasets"
	defaultSnapshotsDir = "snapshots"
	defaultManifestFile = "manifest.json"
	defaultDataDir      = "data"
)

// DatasetsPrefix returns "datasets/".
func (l DefaultLayout) DatasetsPrefix() string {
	return defaultDatasetsDir + "/"
}

// SegmentsPrefix returns "datasets/<dataset>/snapshots/".
func (l DefaultLayout) SegmentsPrefix(dataset lode.DatasetID) string {
	return path.Join(defaultDatasetsDir, string(dataset), defaultSnapshotsDir) + "/"
}

// ManifestPath returns "datasets/<dataset>/snapshots/<segment>/manifest.json".
func (l DefaultLayout) ManifestPath(dataset lode.DatasetID, segment lode.SnapshotID) string {
	return path.Join(defaultDatasetsDir, string(dataset), defaultSnapshotsDir, string(segment), defaultManifestFile)
}

// IsManifest returns true if the path matches the canonical manifest location:
// datasets/<dataset_id>/snapshots/<segment_id>/manifest.json
//
// This is path-aware to prevent stray manifest.json files from polluting
// dataset discovery. Per CONTRACT_READ_API.md, "manifest presence = commit signal"
// applies only to manifests in the correct location.
func (l DefaultLayout) IsManifest(p string) bool {
	return l.isValidManifestPath(p)
}

// ParseDatasetID extracts dataset ID from path format:
// datasets/<dataset_id>/snapshots/<segment_id>/manifest.json
//
// Returns empty string if the path doesn't match the canonical layout.
// This ensures only manifests in /snapshots/ directories count toward
// dataset existence, per "manifest presence = commit signal" rule.
func (l DefaultLayout) ParseDatasetID(manifestPath string) lode.DatasetID {
	if !l.isValidManifestPath(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")
	return lode.DatasetID(parts[1])
}

// ParseSegmentID extracts segment ID from path format:
// datasets/<dataset_id>/snapshots/<segment_id>/manifest.json
//
// Returns empty string if the path doesn't match the canonical layout.
func (l DefaultLayout) ParseSegmentID(manifestPath string) lode.SnapshotID {
	if !l.isValidManifestPath(manifestPath) {
		return ""
	}
	parts := strings.Split(manifestPath, "/")
	return lode.SnapshotID(parts[3])
}

// isValidManifestPath checks if path matches:
// datasets/<dataset_id>/snapshots/<segment_id>/manifest.json
func (l DefaultLayout) isValidManifestPath(p string) bool {
	parts := strings.Split(p, "/")
	// Must be exactly: datasets / <dataset> / snapshots / <segment> / manifest.json
	if len(parts) != 5 {
		return false
	}
	return parts[0] == defaultDatasetsDir &&
		parts[1] != "" &&
		parts[2] == defaultSnapshotsDir &&
		parts[3] != "" &&
		parts[4] == defaultManifestFile
}

// ExtractPartitionPath extracts the partition path from a file path.
// File paths have format: datasets/<id>/snapshots/<id>/data/[partition/]filename
// Returns empty string if no partition.
func (l DefaultLayout) ExtractPartitionPath(filePath string) string {
	parts := strings.Split(filePath, "/")

	// Find the "data" component
	dataIdx := -1
	for i, p := range parts {
		if p == defaultDataDir {
			dataIdx = i
			break
		}
	}

	if dataIdx < 0 || dataIdx >= len(parts)-1 {
		return ""
	}

	// Everything between "data" and the filename is the partition path
	partParts := parts[dataIdx+1 : len(parts)-1]
	if len(partParts) == 0 {
		return ""
	}

	return strings.Join(partParts, "/")
}

// Ensure DefaultLayout implements Layout.
var _ Layout = DefaultLayout{}
