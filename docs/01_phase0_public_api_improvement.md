# Lode Public API Fix Plan (v0)

This document defines a precise, executable plan for revising Lode’s **public API**
to enforce immutability, streaming semantics, and strict abstraction boundaries.

This is an API-only plan. No implementation work is required here.
Future implementations are expected to take advantage of the additional clarity
introduced by this contract.

---

## Core Constraints

The public API MUST:

- Enforce immutability at the type/contract level
- Represent persistence structure only, not execution
- Be streaming-first (no forced materialization)
- Require explicit metadata
- Remain small and invariant-driven

The public API MUST NOT:

- Permit deletion of data referenced by snapshots
- Force `[]any` materialization
- Encode query execution or optimization
- Leak backend-specific behavior

---

## Historical Model (Explicit)

- Dataset history is **strictly linear** in v0
- Each dataset has exactly one head snapshot
- Branching and merges are not supported
- Snapshot lineage is defined by parent pointers, not timestamps
- `Current()` always refers to the unique head snapshot

---

## Step 0 — API file layout (refactor only)

Split the existing `lode/api.go` into focused API files:

```
/lode
  errors.go
  types.go          // IDs, metadata, checksums
  store.go
  codec.go
  compression.go
  partition.go
  dataset.go
  snapshot.go
  manifest.go
```

All files remain in package `lode`.

---

## Step 1 — Remove immutability leakage via Store.Delete

### Problem

The current public Store interface exposes Delete, allowing consumers to delete
data or manifest objects referenced by immutable snapshots.

### Fix

Split storage capabilities by responsibility.

### Public API (no deletion, ObjectRef is canonical)

```go
type ObjectStore interface {
    Put(ctx context.Context, key string, r io.Reader, meta ObjectMeta) (ObjectRef, error)
    Get(ctx context.Context, ref ObjectRef) (io.ReadCloser, ObjectMeta, error)
    Head(ctx context.Context, ref ObjectRef) (ObjectMeta, error)
    List(ctx context.Context, prefix string) (ObjectIterator, error)
}
```

Supporting types:

```go
type ObjectMeta struct {
    SizeBytes int64
    ETag      string
    CreatedAt time.Time
    User      map[string]string
}

type ObjectRef struct {
    Key  string
    Meta ObjectMeta
}
```

```go
type ObjectIterator interface {
    Next() bool
    Ref() ObjectRef
    Err() error
    Close() error
}
```

### ObjectRef metadata freshness

`ObjectRef.Meta` is **informational** and represents metadata observed at the
time the reference was created. Implementations are not required to keep
`ObjectRef.Meta` synchronized with subsequent `Head` or `Get` results.

Consumers requiring fresh metadata MUST call `Head` or `Get`.

### Internal-only capability (NOT public)

```go
type MutableStore interface {
    ObjectStore
    Delete(ctx context.Context, key string) error
}
```

No public API may expose Delete.

---

## Step 2 — Streaming-first records

```go
type RecordIterator interface {
    Next() bool
    Record() any
    Err() error
    Close() error
}
```

Iterator contract:

- `Close()` must be idempotent
- `Err()` may be called after `Close()`
- `Next()` returns false after exhaustion or close
- Resources must be released on `Close()` or exhaustion

### Record opacity

Values returned by `RecordIterator.Record()` are **opaque to Lode**.
Lode does not inspect, validate, or interpret record contents.
Record shape semantics are defined entirely by the associated `Codec`
and any user-provided `Partitioner`.

---

## Step 3 — Codec (explicit, dataset-bound)

Codec selection is part of persistence structure and must be explicit.

```go
type Codec interface {
    Name() string
    FileExtension() string

    Encode(w io.Writer, records RecordIterator) error
    Decode(r io.Reader) (RecordIterator, error)
}
```

A Dataset is bound to a single Codec for its lifetime.

---

## Step 4 — Dataset and Snapshot separation

```go
type Dataset interface {
    ID() DatasetID

    Write(ctx context.Context, records RecordIterator, meta SnapshotMetadata) (Snapshot, error)

    Current(ctx context.Context) (Snapshot, error)
    Snapshot(ctx context.Context, id SnapshotID) (Snapshot, error)

    ListSnapshots(ctx context.Context) (SnapshotIterator, error)
}
```

```go
type Snapshot interface {
    ID() SnapshotID

    Manifest(ctx context.Context) (*Manifest, error)

    Records(ctx context.Context) (RecordIterator, error)
}
```

```go
type SnapshotIterator interface {
    Next() bool
    Snapshot() Snapshot
    Err() error
    Close() error
}
```

### Snapshot listing order

`ListSnapshots` returns snapshots in ascending order along the parent chain
(oldest to newest). Consumers must not rely on timestamps for ordering.

Future versions may support snapshot enumeration options (e.g. limits,
ranges, direction), provided the linear-history invariant is preserved.

---

## Step 5 — Explicit metadata semantics

```go
type SnapshotMetadata struct {
    Values map[string]string
    Set    bool
}
```

Rules:

- `Set == true` REQUIRED for `Dataset.Write`
- Empty map with `Set == true` is explicitly empty metadata
- `Set == false` passed to `Write` MUST return `ErrInvalidMetadata`

---

## Step 6 — Manifest typing

```go
type ManifestVersion string

const ManifestV1 ManifestVersion = "v1"
```

```go
type Checksum struct {
    Algorithm string `json:"algorithm"`
    Hex       string `json:"hex"`
}
```

Checksums are optional but unambiguous.

---

## Step 7 — Partitioning

```go
type Partitioner interface {
    Name() string
    PartitionPath(record any) (string, error)
}
```

Partitioner and Codec must agree on record interpretation.
This agreement is explicitly a user-land responsibility.

---

## Step 8 — Errors

```go
var (
    ErrNotFound         = errors.New("not found")
    ErrAlreadyExists    = errors.New("already exists")
    ErrNoSnapshots      = errors.New("no snapshots")
    ErrInvalidMetadata  = errors.New("invalid snapshot metadata")
)
```

---

## Step 9 — Safe deletion (future-facing)

Deletion may be exposed ONLY via:

- Garbage collection tooling operating on unreachable objects
- Administrative APIs accepting dataset or snapshot identifiers
- Offline tooling with explicit user intent

Deletion must never accept raw object keys in the public API.

---

## Step 10 — Public surface justification

Public types correspond directly to persistence invariants:
Dataset, Snapshot, Manifest, ObjectStore, Codec, Partitioner.

Execution helpers, adapters, and convenience utilities MUST live in `/internal`
or `examples`.

---

## Final Review Checklist

- No public Delete
- ObjectRef is canonical for reads
- Streaming-first everywhere
- Codec bound explicitly to Dataset
- Linear history only
- Explicit metadata presence and error behavior
- Iterator semantics well-defined
- Snapshot listing order explicit
- Minimal, invariant-driven public API
