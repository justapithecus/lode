# Lode Storage Adapter — Contract

This document defines the required semantics for storage adapters.
It is authoritative for any implementation of the `Store` interface.

---

## Goals

1. Safe, immutable writes.
2. Accurate existence checks and listings.
3. Backend-agnostic behavior.

---

## Adapter Obligations

### Put
- MUST write data to the given path.
- MUST NOT overwrite existing data.
- If the path already exists, MUST return `ErrPathExists` (or an equivalent error).

#### One-Shot vs Streaming Put

Adapters MAY implement Put using different mechanisms based on payload size:

**One-Shot Path** (for payloads ≤ adapter-defined threshold):
- MUST use atomic conditional-create semantics where the backend supports it
  (e.g., S3 `If-None-Match: "*"`).
- Overwrite protection is atomic: no TOCTOU window.
- Duplicate writes MUST return `ErrPathExists`.

**Streaming/Multipart Path** (for payloads > threshold):
- Used when the backend requires chunked uploads for large payloads.
- If the backend does not support conditional completion (e.g., S3 multipart),
  the adapter MUST perform a preflight existence check before starting the upload.
- Overwrite protection is best-effort: a TOCTOU window exists between the
  existence check and upload completion.
- If the preflight check detects an existing path, MUST return `ErrPathExists`.
- **Single-writer or external coordination is REQUIRED** to guarantee no-overwrite
  semantics on backends without conditional multipart completion.

#### Adapter Documentation Requirements

Adapters MUST document:
- The size threshold for one-shot vs streaming/multipart routing.
- Which mechanism provides atomic vs best-effort overwrite protection.
- Any backend-specific limitations affecting the no-overwrite guarantee.

### Get
- MUST return a readable stream for an existing path.
- If the path does not exist, MUST return `ErrNotFound` (or an equivalent error).

### Exists
- MUST accurately report existence.
- MUST not create or mutate data.

### List
- MUST return all paths under the given prefix.
- Ordering is unspecified.
- Pagination behavior (if any) MUST be documented by the adapter.

### Delete
- MUST remove the path if it exists.
- MUST be safe to call on a missing path (idempotent or `ErrNotFound`).

### ReadRange
- MUST return bytes from `[offset, offset+length)` for the given path.
- If the path does not exist, MUST return `ErrNotFound`.
- If offset or length is negative, MUST return `ErrInvalidPath`.
- If length exceeds platform `int` capacity, MUST return `ErrInvalidPath`.
- If offset+length would overflow, MUST return `ErrInvalidPath`.
- If the range extends beyond EOF, MUST return available bytes (not an error).
- If offset is beyond EOF, MUST return an empty slice.
- MUST use true range reads (not whole-file read) where the backend supports it.

### ReaderAt
- MUST return an `io.ReaderAt` for random access reads.
- If the path does not exist, MUST return `ErrNotFound`.
- The returned `ReaderAt` MUST support concurrent reads at different offsets.
- Callers are responsible for closing the underlying resource if it implements `io.Closer`.

---

## Commit Semantics

- Manifests (or explicit commit markers) define visibility.
- Writers MUST write data objects before the manifest.
- Readers MUST treat manifest presence as the commit signal.

## Streamed Writes

- Adapters MUST allow data objects to be written before manifest commit.
- Adapters MUST NOT provide any implicit commit signal outside manifest presence.
- Safe write semantics (no overwrite) apply to streamed objects as well.
- Adapters MUST allow deletion of partial objects via `Delete` for cleanup.

### Streaming Write Atomicity

For streaming writes that use the multipart/chunked path:
- The no-overwrite guarantee depends on the Put path used (see "One-Shot vs Streaming Put").
- On backends without conditional multipart completion, concurrent writers may
  create a race condition where both detect "not exists" and proceed to write.
- Callers using streaming writes on such backends MUST ensure single-writer
  semantics or use external coordination.
- Failure during multipart upload SHOULD trigger best-effort abort/cleanup.
- Cleanup MUST use an independent context (not the caller's potentially-canceled
  context) to maximize cleanup success.

---

## Consistency Notes

Adapters MUST document:
- Consistency guarantees for `List` and `Exists`
- Any required read-after-write delays or mitigations
