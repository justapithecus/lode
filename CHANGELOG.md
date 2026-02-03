# CHANGELOG

## v0.2.0 — 2026-02-03
- S3 adapter is now part of the public API under `lode/s3`, with docs and examples for AWS S3, MinIO, LocalStack, and Cloudflare R2.
- Project is now licensed under Apache 2.0.
- README now lists supported backends and the license.
- `CHANGELOG.md` added for release tracking.

## v0.1.0 — 2026-02-03
- Public API for datasets and readers, immutable snapshots, manifests, and explicit metadata.
- Layout system with default, hive, and flat layouts; dataset enumeration and partition-pruning semantics.
- Storage adapters for filesystem and in-memory; experimental S3 adapter under `internal/`.
- Codec/compression: JSONL codec, gzip compressor, and no-op defaults.
- Range-read support (`ReadRange`, `ReaderAt`) for partial object access.
- Examples covering default layout, hive layout, manifest-driven discovery, blob upload, and S3 experimental.

### Notable internal work
- Contract alignment for read/write semantics, manifest validation, iterator lifecycle compliance, and cross-adapter consistency tests.
- CI/release automation and tooling setup.
