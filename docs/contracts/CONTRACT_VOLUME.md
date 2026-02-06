# Lode Volume Model — Contract (v0.6+ Draft)

This document defines the planned contract for the `Volume` persistence paradigm.

Status:
- Targeted for v0.6.0 implementation
- Becomes active when `Volume` is introduced into the public API

---

## Goals

1. Represent sparse, resumable byte-range persistence explicitly.
2. Preserve manifest-driven commit semantics for partial materialization workflows.
3. Keep adapter behavior backend-agnostic and lifecycle semantics explicit.

---

## Non-goals

- Torrent protocol behavior (pieces, peers, trackers).
- Networking, scheduling, runtime orchestration, or execution policy.
- Hidden background mutation or inference of missing ranges.

---

## Conceptual Model

### Volume

A logical byte address space `[0..N)` identified by `VolumeID` and `TotalLength`.

### Volume Snapshot

An immutable commit that records a set of verified committed ranges.

### Volume Snapshot ID

A stable identifier for a committed Volume snapshot.

### SegmentRef

An immutable reference to a committed segment (offset, length, object path).

### Volume Manifest

The authoritative description of committed ranges for a snapshot.

---

## Manifest Requirements

Volume manifests MUST include at minimum:
- schema name + version
- volume ID
- snapshot ID
- total length
- creation time
- explicit metadata object
- committed segments list

Each committed segment MUST record:
- offset
- length
- immutable object reference/path
- optional checksum metadata when configured

Absence is meaningful:
- Ranges not listed are uncommitted and MUST NOT be inferred.

---

## Write Lifecycle

Volume write lifecycle is explicit:

1. Stage: data may be written provisionally.
2. Verify: caller/runtime validates data externally.
3. Commit: verified segments are recorded in a new immutable snapshot manifest.

Rules:
- Unverified or uncommitted ranges MUST NOT appear in committed manifests.
- Commit visibility is manifest-driven, same as Dataset.
- Snapshots are immutable once committed.

---

## Read Semantics

Volume reads are range-first:
- `ReadAt(snapshotID, offset, length)` succeeds only if the entire requested
  range is covered by committed segments.

Required behavior:
- If any sub-range is missing, return an explicit missing-range error.
- No zero-fill fallback.
- No partial-success ambiguity for committed read paths.

---

## Public API Surface (v0.6)

The public Volume API is explicit and minimal:

```go
// VolumeID uniquely identifies a volume.
type VolumeID string

// NewVolume creates a volume with a fixed total length.
func NewVolume(id VolumeID, storeFactory StoreFactory, totalLength int64, opts ...VolumeOption) (*Volume, error)

// Volume represents a sparse, range-addressable byte space.
type Volume struct { /* opaque */ }

// StageWriteAt writes data at an offset and returns a committed-segment handle.
// Staged data is not visible until Commit is called.
func (v *Volume) StageWriteAt(ctx context.Context, offset int64, r io.Reader) (SegmentRef, error)

// Commit records the provided segments into a new immutable snapshot.
func (v *Volume) Commit(ctx context.Context, segments []SegmentRef, metadata Metadata) (*VolumeSnapshot, error)

// ReadAt reads a fully committed range from a snapshot.
func (v *Volume) ReadAt(ctx context.Context, snapshotID VolumeSnapshotID, offset, length int64) ([]byte, error)
```

API constraints:
- `StageWriteAt` MUST NOT create snapshot visibility.
- `Commit` MUST be manifest-driven and immutable.
- `ReadAt` MUST return a missing-range error if any sub-range is uncommitted.

---

## Error Semantics

- Missing committed range MUST return `ErrRangeMissing`.
- Range reads MUST NOT return partial data without error.

---

## Concurrency

Single-writer semantics per volume apply unless callers provide external
coordination.

Concurrent writers MAY produce conflicting parent relationships or overlapping
intent; Lode does not resolve these conflicts automatically.

---

## Adapter Obligations

No new storage engine type is required.

Existing adapter capabilities remain sufficient:
- immutable object writes
- list/exists/get/delete behavior
- range reads

Adapters MAY choose physical layout strategies (segment objects, sparse files),
but MUST preserve manifest-defined truth semantics.

---

## Dataset Boundary

`Dataset` and `Volume` are coequal abstractions with distinct semantics:

- Dataset streaming: atomic construction of complete objects
- Volume streaming: incremental commitment of partial truth

If a workflow requires sparse, resumable, out-of-order, or range-verified truth,
it belongs to `Volume`, not `Dataset`.

---

## Prohibited Behaviors

- Inferring committed ranges not present in the manifest.
- Treating staged/unverified data as committed truth.
- Implicit background compaction or mutation of committed segments.
