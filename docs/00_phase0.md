# Phase 0 Plan: Interface Contract + Manifest Schema

This plan covers two concrete deliverables:
1) draft the public interfaces with invariant comments
2) design the manifest JSON schema as the persistence contract

The work is scoped to Lode's persistence structure only.

## Workstream A: Public interfaces with invariant comments

- Inventory existing public types and interfaces under `lode/` to avoid expanding the public surface.
- Identify the minimal set of interfaces that define persistence structure (datasets, snapshots, manifests, metadata, safe writes).
- For each interface method, add invariant-focused doc comments:
  - must/never language for immutability
  - explicit metadata requirements
  - snapshot and manifest self-description guarantees
- Confirm that any helper types live under `internal/` unless they are part of the minimal public surface.
- Add or update example workflows that exercise the interfaces and implicitly restate invariants through usage.

Acceptance checks:
- Public API surface remains minimal and unchanged in shape.
- Comments describe invariants, not implementation behavior.
- No new execution or query semantics are introduced.

## Workstream B: Manifest JSON schema

- Define the manifest as the persistence contract that crosses boundaries.
- Specify required fields, types, and constraints in a JSON schema document:
  - format/version identifier
  - dataset/snapshot identifiers
  - metadata block (explicit and persisted)
  - references to immutable data files
- Document immutability and self-description requirements directly in schema annotations.
- Define forward-compatibility rules (how unknown fields are handled) without implying execution logic.

Acceptance checks:
- Schema is explicit, inspectable, and stable.
- Schema matches the invariants described in interface comments.
- No implied query planning, scheduling, or execution semantics.

## Sequencing

1) Complete interface inventory and confirm minimal public surface.
2) Draft invariant comments for interfaces and methods.
3) Draft example workflows that use the interfaces.
4) Define the manifest JSON schema with required fields and constraints.
5) Cross-check terminology and invariants across interfaces, examples, and schema.

## Open questions for implementer

- Where should the manifest schema live (docs vs internal reference)?
- Is there an existing manifest draft or format to extend?
- What is the expected versioning policy for manifest format evolution?
