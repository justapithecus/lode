// Example: Default Layout Round-Trip
//
// This example demonstrates the write → list → read flow using the default layout:
//   datasets/<dataset>/snapshots/<segment>/
//     manifest.json
//     data/[partition/]filename
//
// Run with: go run ./examples/default_layout
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/justapithecus/lode/internal/codec"
	"github.com/justapithecus/lode/internal/compress"
	"github.com/justapithecus/lode/internal/dataset"
	"github.com/justapithecus/lode/internal/partition"
	"github.com/justapithecus/lode/internal/read"
	"github.com/justapithecus/lode/internal/storage"
	"github.com/justapithecus/lode/lode"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// Create a temporary directory for storage
	tmpDir, err := os.MkdirTemp("", "lode-example-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("Storage root: %s\n\n", tmpDir)

	// Create filesystem-backed store
	store, err := storage.NewFS(tmpDir)
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	// -------------------------------------------------------------------------
	// WRITE: Create dataset and write data
	// -------------------------------------------------------------------------
	fmt.Println("=== WRITE ===")

	// Create dataset with default layout (no Layout specified = DefaultLayout)
	ds, err := dataset.New("events", dataset.Config{
		Store:       store,
		Codec:       codec.NewJSONL(),
		Compressor:  compress.NewNoop(),
		Partitioner: partition.NewNoop(),
		// Layout: nil uses DefaultLayout
	})
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	// Write some records
	records := []any{
		map[string]any{"id": 1, "event": "login", "user": "alice"},
		map[string]any{"id": 2, "event": "click", "user": "bob"},
		map[string]any{"id": 3, "event": "logout", "user": "alice"},
	}

	snapshot, err := ds.Write(ctx, records, lode.Metadata{"source": "example"})
	if err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	fmt.Printf("Created snapshot: %s\n", snapshot.ID)
	fmt.Printf("Files in manifest:\n")
	for _, f := range snapshot.Manifest.Files {
		fmt.Printf("  - %s (%d bytes)\n", f.Path, f.SizeBytes)
	}
	fmt.Println()

	// Show the actual file structure
	fmt.Println("File structure:")
	err = filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(tmpDir, path)
		if rel != "." {
			if info.IsDir() {
				fmt.Printf("  %s/\n", rel)
			} else {
				fmt.Printf("  %s\n", rel)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// LIST: Discover datasets and segments using Reader
	// -------------------------------------------------------------------------
	fmt.Println("=== LIST ===")

	reader := read.NewReader(store)

	// List all datasets
	datasets, err := reader.ListDatasets(ctx, read.DatasetListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}
	fmt.Printf("Datasets found: %v\n", datasets)

	// List segments in the dataset
	segments, err := reader.ListSegments(ctx, "events", "", read.SegmentListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list segments: %w", err)
	}
	fmt.Printf("Segments in 'events': %v\n", segments)
	fmt.Println()

	// -------------------------------------------------------------------------
	// READ: Load manifest and read data back
	// -------------------------------------------------------------------------
	fmt.Println("=== READ ===")

	// Get manifest for the segment
	manifest, err := reader.GetManifest(ctx, "events", segments[0])
	if err != nil {
		return fmt.Errorf("failed to get manifest: %w", err)
	}
	fmt.Printf("Manifest schema: %s v%s\n", manifest.SchemaName, manifest.FormatVersion)
	fmt.Printf("Row count: %d\n", manifest.RowCount)
	fmt.Printf("Codec: %s, Compressor: %s\n", manifest.Codec, manifest.Compressor)

	// Read data through the dataset
	readRecords, err := ds.Read(ctx, lode.SnapshotID(segments[0].ID))
	if err != nil {
		return fmt.Errorf("failed to read: %w", err)
	}

	fmt.Printf("\nRecords read back:\n")
	for _, r := range readRecords {
		fmt.Printf("  %v\n", r)
	}

	fmt.Println("\n=== SUCCESS ===")
	fmt.Println("Default layout round-trip complete!")

	return nil
}
