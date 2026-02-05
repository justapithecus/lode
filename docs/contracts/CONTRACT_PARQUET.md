# Lode Parquet Codec — Contract

This document defines the required semantics for Parquet codec integration.
It is authoritative for any Parquet codec implementation within Lode.

---

## Goals

1. Columnar storage format for efficient analytical queries.
2. Compatible with external readers (DuckDB, Spark, Polars, PyArrow).
3. Schema-explicit encoding (no schema inference from data).
4. Codec interface compliance with Lode's existing abstractions.

---

## Non-goals

- Query execution or predicate pushdown.
- Automatic schema evolution or merging.
- Row-group-level partitioning within Lode (external readers handle this).
- Encryption or column-level access control.

---

## Parquet Codec Interface

The Parquet codec MUST implement the `Codec` interface:

```go
type Codec interface {
    Name() string                             // Returns "parquet"
    Encode(w io.Writer, records []any) error  // Batch encoding
    Decode(r io.Reader) ([]any, error)        // Batch decoding
}
```

### Name

- `Name()` MUST return `"parquet"`.
- This name is recorded in manifests.

### Encode

- `Encode(w, records)` MUST write a valid Parquet file to `w`.
- The Parquet file MUST include the footer (not streamable without buffering).
- Records MUST be encoded according to the configured schema.
- Empty records (`len(records) == 0`) MUST produce a valid Parquet file with zero rows.
- Encoding errors MUST be returned, not silently ignored.

### Decode

- `Decode(r)` MUST read a complete Parquet file from `r`.
- Returns records as `[]any` where each record is `map[string]any`.
- Field types are mapped according to the Type Mapping table below.
- Decode MUST support files written by this codec and standard Parquet writers.

---

## Schema Requirements

Parquet requires a schema. Lode Parquet codec MUST support explicit schema configuration.

### Schema Definition

Schemas are defined at codec construction time:

```go
codec := lode.NewParquetCodec(lode.ParquetSchema{
    Fields: []lode.ParquetField{
        {Name: "id", Type: lode.ParquetInt64},
        {Name: "name", Type: lode.ParquetString},
        {Name: "timestamp", Type: lode.ParquetTimestamp},
    },
})
```

### Schema Validation

- Records MUST contain all required fields defined in the schema.
- Extra fields in records that are not in the schema MUST be silently ignored.
- Missing required fields MUST return an error during encoding.
- Type mismatches MUST return an error during encoding.

### No Schema Inference

- The codec MUST NOT infer schema from record data.
- Explicit schema is required for predictable, portable output.
- This aligns with Lode's principle: "stores facts, not interpretations."

---

## Type Mapping

| Parquet Type         | Go Source Type       | Decoded Go Type   |
|---------------------|---------------------|-------------------|
| `ParquetInt32`      | `int`, `int32`      | `int32`           |
| `ParquetInt64`      | `int`, `int64`      | `int64`           |
| `ParquetFloat32`    | `float32`           | `float32`         |
| `ParquetFloat64`    | `float64`           | `float64`         |
| `ParquetString`     | `string`            | `string`          |
| `ParquetBool`       | `bool`              | `bool`            |
| `ParquetBytes`      | `[]byte`            | `[]byte`          |
| `ParquetTimestamp`  | `time.Time`         | `time.Time`       |

### Nullable Fields

- Fields MAY be marked as nullable in the schema.
- Nullable fields accept `nil` values and encode as Parquet null.
- Non-nullable fields with `nil` values MUST return an error.

---

## Streaming Limitations

Parquet files require a footer that references row group metadata.
This has implications for streaming APIs.

### StreamWriteRecords Compatibility

The Parquet codec MAY implement `StreamingRecordCodec`:

```go
type StreamingRecordCodec interface {
    Codec
    NewStreamEncoder(w io.Writer) (RecordStreamEncoder, error)
}
```

**Implementation constraints:**

- `NewStreamEncoder` MUST buffer records until `Close()` is called.
- `Close()` writes the complete Parquet file (data + footer).
- Memory usage scales with record count (not truly streaming).
- Large datasets may require external chunking by the caller.

**Alternative: Batch-only codec**

Implementations MAY choose to NOT implement `StreamingRecordCodec`:
- `StreamWriteRecords` will return `ErrCodecNotStreamable`.
- Callers use `Write` for batched encoding.
- This is a valid design choice for memory-constrained environments.

### Recommendation

Implementations SHOULD document:
- Whether streaming is supported.
- Memory characteristics for streaming mode.
- Recommended batch sizes for optimal row group sizing.

---

## Row Group Configuration

Row groups affect read performance and memory usage.

### Defaults

- Default row group size: 128 MB (compressed) or implementation-defined.
- Single row group is acceptable for small datasets.

### Optional Configuration

Implementations MAY expose row group configuration:

```go
codec := lode.NewParquetCodec(schema,
    lode.WithRowGroupSize(64 * 1024 * 1024), // 64 MB
)
```

Row group configuration is optional and implementation-defined.

---

## Compression

Parquet supports internal compression per column chunk.

### Codec-Level Compression

- Parquet codec MAY apply internal compression (Snappy, Zstd, etc.).
- Internal compression is orthogonal to Lode's `Compressor` interface.
- Using both internal and external compression is valid but may be redundant.

### Recommended Configuration

- Use internal Parquet compression (Snappy or Zstd) for columnar efficiency.
- Set Lode's compressor to `"noop"` when using internal compression.
- This avoids double-compression overhead.

### Manifest Recording

- Lode's `Compressor` field records the external compressor (`"noop"` or other).
- Internal Parquet compression is part of the file format, not recorded separately.

---

## Statistics and Metadata

Parquet files contain statistics (min/max, null count, row count).

### Manifest Integration

The following statistics MAY be extracted and recorded in manifests:

- `RowCount`: Total rows in the file (from Parquet metadata).
- `MinTimestamp` / `MaxTimestamp`: When a timestamp column is designated.

### Future: Extended Manifest Stats

Future versions MAY add:
- Column-level min/max statistics.
- Null counts per column.
- Byte size per column.

These extensions are additive and do not affect this contract.

---

## Error Semantics

### Encoding Errors

| Condition                     | Error                           |
|------------------------------|---------------------------------|
| Missing required field       | `ErrSchemaViolation` (new)      |
| Type mismatch                | `ErrSchemaViolation` (new)      |
| Nil value for non-nullable   | `ErrSchemaViolation` (new)      |
| Write failure                | Underlying io error             |

### Decoding Errors

| Condition                     | Error                           |
|------------------------------|---------------------------------|
| Invalid Parquet file         | `ErrInvalidFormat` (new)        |
| Corrupted data               | `ErrInvalidFormat` (new)        |
| Read failure                 | Underlying io error             |

### New Error Sentinels

```go
var (
    ErrSchemaViolation = errors.New("parquet: schema violation")
    ErrInvalidFormat   = errors.New("parquet: invalid format")
)
```

These errors SHOULD be added to `api.go` if Parquet codec is promoted to public API.

---

## Construction API

### Minimal API

```go
// NewParquetCodec creates a Parquet codec with the given schema.
func NewParquetCodec(schema ParquetSchema, opts ...ParquetOption) Codec

// ParquetSchema defines the record structure.
type ParquetSchema struct {
    Fields []ParquetField
}

// ParquetField defines a single field.
type ParquetField struct {
    Name     string
    Type     ParquetType
    Nullable bool
}

// ParquetType enumerates supported Parquet logical types.
type ParquetType int

const (
    ParquetInt32 ParquetType = iota
    ParquetInt64
    ParquetFloat32
    ParquetFloat64
    ParquetString
    ParquetBool
    ParquetBytes
    ParquetTimestamp
)
```

### Options

```go
// WithRowGroupSize sets the target row group size in bytes.
func WithRowGroupSize(bytes int64) ParquetOption

// WithCompression sets internal Parquet compression.
func WithCompression(codec ParquetCompression) ParquetOption

type ParquetCompression int

const (
    ParquetCompressionNone ParquetCompression = iota
    ParquetCompressionSnappy
    ParquetCompressionGzip
    ParquetCompressionZstd
)
```

---

## Compatibility Requirements

### External Reader Compatibility

Parquet files produced by this codec MUST be readable by:
- Apache Arrow / PyArrow
- DuckDB
- Apache Spark
- Polars

### Standard Compliance

- Output MUST conform to Apache Parquet specification.
- Use standard logical types (not custom extensions).

---

## Implementation Notes

### Recommended Library

The `parquet-go` library (github.com/parquet-go/parquet-go) is recommended:
- Pure Go implementation.
- Supports schema-based encoding.
- Active maintenance.

### Alternative Libraries

- `github.com/xitongsys/parquet-go`: Older, less maintained.
- `github.com/apache/arrow-go`: Heavier dependency, more features.

Library choice is implementation detail, not contract-bound.

---

## Testing Requirements

### Unit Tests

- Encode/decode round-trip for all supported types.
- Schema validation (missing fields, type mismatches, nullability).
- Empty record handling.
- Large record batches (verify row group behavior).

### Integration Tests

- Read files with DuckDB.
- Read files with PyArrow.
- Verify statistics extraction.

### Contract Compliance Tests

- `Name()` returns `"parquet"`.
- Manifests record codec as `"parquet"`.
- Error sentinels are returned for documented conditions.

---

## Open Questions

The following decisions are deferred to implementation:

1. **Nested types**: Should the codec support nested structs/lists/maps?
   - Recommendation: Start with flat schemas, add nested types later.

2. **Schema from struct tags**: Should schemas be derivable from Go struct tags?
   - Recommendation: Explicit schema first, struct tags as convenience later.

3. **Streaming memory limit**: Should streaming mode have a configurable memory cap?
   - Recommendation: Document memory characteristics, let callers chunk.

---

## References

- [Apache Parquet Specification](https://parquet.apache.org/docs/)
- [parquet-go Library](https://github.com/parquet-go/parquet-go)
- [CONTRACT_LAYOUT.md](CONTRACT_LAYOUT.md) — Codec recording in manifests
- [CONTRACT_CORE.md](CONTRACT_CORE.md) — Manifest requirements
