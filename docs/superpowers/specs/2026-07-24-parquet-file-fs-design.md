# parquet-file-fs — Design

**Date:** 2026-07-24
**Status:** Approved

## Purpose

A Rust-core Python package (built with maturin/PyO3) that treats parquet files as
read-only archives of files and exposes their contents through an fsspec-compatible
filesystem. The primary consumer is a coding agent: it gets `ls`, `cat`, `open`,
`glob`, and `walk` over datasets shipped as parquet shards (e.g. path + content
columns, such as akimichi page-image shards) without loading whole shards into
memory.

## Semantics

- **Archive mode only.** Each parquet row is one file: a path column holds the
  file's path inside the virtual tree, a content column holds its bytes.
  Directories are derived from path prefixes; they are implicit and not stored.
- **Path canonicalization.** Paths are normalized by dropping leading/trailing
  separators and empty segments, so `a//b`, `/a/b` and `a/b/` name the same
  file. A row whose path normalizes to empty is an error at index-build time.
  When a path is both a file and another path's directory prefix (`a` plus
  `a/b`), listings show the name once as the file — `ls` then agrees with
  `info`/`read`, and the nested children remain reachable via `find`/`glob`.
- **Read-only.** All fsspec write/mutation methods raise
  `NotImplementedError("read-only filesystem")`.
- **Multi-shard.** One filesystem instance may span many parquet files. The
  constructor accepts a string or list of strings; each entry may be a concrete
  path/URL or a glob. All shards' path→content mappings merge into one tree.

## Architecture

Two layers:

1. **Rust core** (`parquet_file_fs._core`, PyO3, abi3 wheels): parquet decoding,
   index, byte-range I/O, native protocol adapters.
2. **Python layer** (`parquet_file_fs`): `ParquetFileSystem` (subclass of
   `fsspec.AbstractFileSystem`), the protocol adapter registry, and the shim that
   adapts fsspec filesystems to the adapter interface.

Only hard runtime dependency: `fsspec`.

### Rust core

- **`RangeReader` trait** — the I/O abstraction all parquet reading goes through:
  - `size(url) -> u64`
  - `read_range(url, offset, length) -> bytes`
  - `list(glob) -> Vec<String>` (glob expansion for shard discovery)
- **`NativeAdapter`** implements `RangeReader` with the `object_store` crate.
  Built-in schemes: `file://` (and bare local paths), `s3://`, `http://`,
  `https://`. Async internally; a private tokio runtime is created lazily and all
  Python-facing calls block on it, so the public API is fully synchronous.
- **`PyAdapter`** implements `RangeReader` by calling a registered Python object
  (acquiring the GIL per call). This is the bridge that makes the registry
  extensible from Python.
- **Adapter registry** (in Rust, populated from Python): maps URL scheme →
  adapter. Native schemes are pre-registered; `register_adapter(scheme, obj)`
  installs or replaces an entry with a `PyAdapter`.
- **Index build** (constructor time): for each shard, read the parquet footer
  metadata and decode *only* the path column. Record for every row:
  `path → (shard_id, row_group_index, row_offset_within_group)`. A directory
  tree is derived from path prefixes. The content column is never read at index
  time.
- **File sizes are lazy.** Parquet metadata does not store per-value byte sizes,
  so exact sizes require decoding the content column. Names-only listings
  (`ls(detail=False)`, `exists`, `glob`) use the index alone. The first
  `info`/`ls(detail=True)` touching a row group decodes that group's content
  column value lengths once and caches per-row sizes in the index.
- **Content read** (on demand): locate the row via the index, fetch and decode
  only the containing row group's content column chunk, slice out the row's
  value. A small LRU cache (4 decoded chunks) avoids re-decoding when an agent
  reads sibling files from the same row group.

### Python layer

```python
from parquet_file_fs import ParquetFileSystem, register_adapter

register_adapter("gs", gcsfs.GCSFileSystem())  # any fsspec fs, or any object
                                               # with size/read_range/glob
fs = ParquetFileSystem(["data/*.parquet", "s3://bucket/shard-*.parquet"])
fs.ls("/images")
fs.cat_file("/labels/p1.json")
with fs.open("/images/p1.png") as f:
    data = f.read()
fs.info("/labels/p1.json")   # {"name": ..., "size": ..., "type": "file",
                             #  "metadata": {<extra parquet columns>}}
```

- `ParquetFileSystem(fsspec.AbstractFileSystem)` with `protocol = "pfs"`,
  registered with fsspec at import time. Implemented natively: `ls`, `info`,
  `cat_file`, `_open` (read-only file-like over fetched bytes), `exists`.
  `du` and `walk` are inherited from fsspec's generic implementations. `glob`
  and `find` are overridden with index-only fast paths that preserve fsspec
  semantics — the generic versions would decode content chunks just to list
  names. `glob` with an explicit `maxdepth` falls back to the generic
  implementation.
- **Column mapping — convention + override.** Auto-detect path column from
  `path`, `filename`, `file_name`, `key` (first match, in that order) and content
  column from `content`, `data`, `bytes`. Constructor args `path_column=` /
  `content_column=` override detection. Path column must be a string type;
  content column binary or string.
- **Extra columns** (anything besides path/content) surface per-row in
  `info(path)["metadata"]` as a dict. Non-scalar values are converted to their
  Python equivalents; content-sized blobs in extra columns are not special-cased
  in v1.
- **Adapter interface** (duck-typed): `size(url) -> int`,
  `read_range(url, offset, length) -> bytes`, `glob(pattern) -> list[str]`.
  `register_adapter(scheme, obj)` accepts either such an object or an fsspec
  `AbstractFileSystem`, which is wrapped in a shim:
  `read_range → cat_file(url, start=offset, end=offset+length)`,
  `size → info(url)["size"]`, `glob → glob(pattern)`.

## Error handling

- **Duplicate path across shards** → error at index-build time by default;
  constructor arg `on_duplicate="error" | "first" | "last"` overrides
  (keep the first / last shard's row, in constructor shard order).
- **No detectable path/content column** → `ValueError` listing the parquet
  file's actual columns and pointing at `path_column=`/`content_column=`.
- **Unknown URL scheme** with no registered adapter → `ValueError` naming the
  scheme and showing `register_adapter(...)` usage.
- **Adapter failure** → exception propagated wrapped with the failing
  `scheme://url` for context.
- **Missing virtual path** on read/info → `FileNotFoundError`.

## Testing

- **Rust unit tests:** index construction, directory-tree derivation,
  duplicate-path policies, row-group/row-offset math.
- **Python (pytest), fixtures generated with pyarrow:**
  - shards with text + binary content and extra metadata columns
  - multi-row-group shards (content read touches the correct group only)
  - multi-shard merge, glob-based shard discovery, duplicate handling modes
  - column auto-detection and explicit override, bad-schema error message
  - a dict-backed in-memory custom adapter registered for a fake scheme,
    proving the registry end to end
  - fsspec behavior on fixtures: `ls`, `glob`, `walk`, `find`, `open`/read,
    `info` metadata, write methods raising
- **CI:** `cargo test`, `maturin develop`, `pytest`.

## Out of scope (v1)

- Write/append support
- FUSE mount and CLI
- Structure-VFS mode (exposing schema/row-groups of arbitrary parquet)
- fsspec chained URLs (`pfs::s3://...`)
- Async Python API
