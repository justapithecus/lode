# Lode Layout & Components — Contract

This document defines layout ownership and component behavior:
partitioning, compression, and codec roles.

---

## Goals

1. Portable, backend-agnostic layout.
2. Explicit component choices in manifests.
3. No implicit defaults or inference.

---

## Layout Ownership

Layout is the **unified abstraction** that governs:
- Path topology for datasets, segments, manifests, and objects.
- Whether and how partition semantics are encoded into paths.

- Logical key layout is defined by the persistence plane.
- Storage adapters MUST NOT invent persistence structure.
- Adapters MAY rewrite keys, but MUST return canonical keys that are persisted.

---

## Partitioner (Logical Semantics)

- Defines logical partition semantics (keys, values, normalization).
- Layout determines if/how partition semantics are encoded into paths.
- MUST be recorded in manifests by name.

---

## Compressor

- Defines compression format and file extension.
- MUST be recorded in manifests by name.
- No-op compression is explicit, not implicit.

---

## Codec

- Defines data serialization format when data is structured.
- Codec configuration is optional.
- When a codec is configured, it MUST be recorded in manifests by name.

---

## NoOp Components

- Canonical NoOp implementations MUST exist.
- Dataset configuration MUST never use nil partitioner or compressor components.
- Manifests MUST record `partitioner = "noop"` and `compression = "noop"` when applicable.
- When a codec is not configured, codec fields MUST be omitted from manifests.

---

## Key Persistence

- Only canonical object keys returned by adapters are persisted.
- Ephemeral metadata or hints MUST NOT be persisted.
