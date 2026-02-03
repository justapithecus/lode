# Blob Upload — Large Binary Guidance

This example focuses on raw blob storage using the default bundle. For large
binary artifacts, prefer single-pass streaming writes rather than modeling chunks
as logical records.

## Write APIs

**One-shot write (`Write`):**
- Use for in-memory data or small blobs
- Data is buffered and written atomically

**Streaming write (`StreamWrite`):**
- Use for large binary payloads that should be streamed once
- Data flows directly to the final object path (no temp files)
- The snapshot becomes visible only after `Commit` writes the manifest
- If a stream is aborted or fails, no snapshot is created

## Single-Pass Streaming

`StreamWrite` performs single-pass writes:
1. Data is written directly to the final object path
2. Compression and checksums are computed on-the-fly
3. `Commit` finalizes the stream and writes the manifest
4. `Abort` or `Close` without `Commit` leaves no snapshot

This design avoids temp files and copy-on-commit overhead.

## Range-Read Access

For efficient partial reads:
- Store a single object and rely on `ReadRange`/`ReaderAt` for offsets.
- If chunking is required, write multiple objects and record an explicit
  chunk index (object keys, byte ranges, order) in metadata.

## Cleanup Guidance

Partial objects may remain if a stream is aborted or fails. Lode performs
best-effort cleanup on abort, but does not guarantee deletion.

If cleanup is critical:
- Object paths are deterministic (based on dataset ID and snapshot ID)
- Callers can list and delete orphaned objects with matching prefixes
- Consider periodic garbage collection for long-running systems

## Metadata Expectations

Record artifact metadata explicitly (size, media type, chunk index, etc.).
Checksum fields are recorded only when a checksum component is configured.
