# Design: `pack` — create parquet archives from a glob or zip

**Date:** 2026-07-25
**Status:** Approved

## Problem

parquet-file-fs reads parquet "archive" shards (one row per file: a path
column + a content column) but offers no way to create them. Users need to
pack files — matched by a glob, listed explicitly, or contained in a zip
archive — into a shard that `ParquetFileSystem` can read.

## Decision summary

- Packing logic lives in the Rust core and is exposed twice: a Rust CLI
  binary (`pfs`) and a Python function (`parquet_file_fs.pack`).
- The repo becomes a cargo workspace with three crates: core, py (pyo3
  extension), cli.
- Output is a single parquet file (no sharding); sharding can be added
  later without breaking the API.

## 1. Repo restructure (cargo workspace)

```
Cargo.toml                      # [workspace]; workspace.package carries the
                                # x-release-please-version marker
crates/
  core/    parquet-file-fs        # existing lib code (adapter.rs, archive.rs,
                                  # chunk_reader.rs, index.rs, native.rs)
                                  # + new pack.rs; rust integration tests
                                  # move to crates/core/tests/
  py/      parquet-file-fs-py     # cdylib named `_core`: current py.rs plus
                                  # the pyo3 module init from lib.rs
  cli/     parquet-file-fs-cli    # [[bin]] name = "pfs"; clap; thin layer
                                  # over core::pack
```

- Member crates use `version.workspace = true`; release-please bumps the
  workspace version in the root `Cargo.toml` (marker moves there) and
  `Cargo.lock`. `release-please-config.json` and the workflows are updated
  for the new paths.
- `pyproject.toml`: `[tool.maturin] manifest-path = "crates/py/Cargo.toml"`;
  the `python/parquet_file_fs/` layout is unchanged, wheel contents are
  unchanged.
- CLI distribution: `cargo install parquet-file-fs-cli` installs `pfs`.
  The wheel does not ship the binary.
- CI: cargo commands run workspace-wide; the maturin build steps point at
  the py crate manifest.

## 2. Core packing module (`crates/core/src/pack.rs`)

### API

```rust
pub struct PackOptions {
    pub path_column: String,        // "path"
    pub content_column: String,     // "content"
    pub compression: PackCompression, // Zstd (default) | Snappy | None
    pub max_row_group_bytes: usize, // 32 MiB default
}

pub struct PackSummary { pub files: u64, pub bytes: u64 }

pub fn pack_glob(pattern: &str, root: Option<&Path>, out: &Path,
                 opts: &PackOptions) -> Result<PackSummary, FsError>;
pub fn pack_files(paths: &[PathBuf], root: &Path, out: &Path,
                  opts: &PackOptions) -> Result<PackSummary, FsError>;
pub fn pack_zip(zip: &Path, out: &Path,
                opts: &PackOptions) -> Result<PackSummary, FsError>;
```

### Semantics

- **Schema:** `path: Utf8` (non-null) + `content: LargeBinary` (non-null) —
  types the existing reader auto-detects. Column names come from
  `PackOptions` so odd archives can be produced for interop.
- **Root / stored paths (glob & files):** stored path = source path made
  relative to `root`, separators normalized to `/`. Default root for
  `pack_glob` is the pattern's wildcard-free directory prefix
  (`data/images/**/*.png` → root `data/images`, stored `a/x.png`); the
  current directory when the pattern has no fixed prefix. `pack_files`
  requires an explicit root. A matched file that does not live under root
  is an error.
- **Glob expansion:** via the `glob` crate. Only regular files are stored;
  directories are skipped; symlinks to files are read through.
- **Zip:** entry names are stored verbatim after normalization (forward
  slashes, strip any leading `/`, reject entries containing `..`
  components or whose name normalizes to empty). Directory entries are
  skipped. Uses the `zip` crate (new core dependency).
- **Determinism:** entries are sorted lexicographically by stored path
  before writing.
- **Duplicates:** two inputs normalizing to the same stored path → error
  (mirrors the reader's default duplicate policy).
- **Empty input:** a glob matching zero files, an empty path list, or a
  zip with no file entries → error ("no files matched").
- **Streaming:** files are read one at a time into arrow builders
  (`StringBuilder` + `LargeBinaryBuilder`); a row group is flushed whenever
  accumulated content reaches `max_row_group_bytes`. Peak memory ≈
  threshold + largest single file. 32 MiB default keeps the reader's lazy
  per-row-group decode cheap.
- **Compression:** parquet page compression, zstd by default; `snappy` and
  `none` selectable.

## 3. CLI (`pfs`)

```
pfs pack <SOURCE> <OUT.parquet>
         [--root DIR]
         [--path-column NAME] [--content-column NAME]
         [--compression zstd|snappy|none]
```

- `SOURCE` auto-detection, in order: existing file ending in `.zip` → zip
  mode (`--root` is rejected in zip mode); existing directory → pack
  `dir/**` with root = dir; otherwise treated as a glob pattern.
- Success prints a one-line summary (`packed N files (M bytes) -> out`);
  errors print to stderr and exit 1.
- Arg parsing with `clap` (cli crate only; wheel and core stay lean).

## 4. Python API

```python
from parquet_file_fs import pack

pack("data/images/**/*.png", "out.parquet", root="data")
pack("bundle.zip", "out.parquet")
pack(["a.txt", "b.txt"], "out.parquet", root=".")
```

- `pack(source, out, *, root=None, path_column="path",
  content_column="content", compression="zstd")`.
- `source`: `str` (same auto-detection as the CLI) or `list[str]`
  (explicit files; `root` required, matching `pack_files`).
- Returns `{"files": n, "bytes": total, "path": out}`.
- Implemented as a pyo3 function in the py crate, wrapped in a small
  `python/parquet_file_fs/pack.py`, re-exported from `__init__.py`.
- Errors map through the existing `to_py`: bad input → `ValueError`,
  I/O failures → `OSError`, missing files → `FileNotFoundError`.

## 5. Error handling

Reuse `FsError`, adding variants only where existing ones don't fit
(e.g. `Pack(String)` for duplicate/empty/outside-root cases if `Schema`
reads wrong). The CLI maps any error to exit code 1 with the display
message; Python inherits the existing exception mapping.

## 6. Testing

- **Rust (core):** pack → read back through the existing `Archive`
  (glob, explicit files, zip); duplicate-path error; empty-match error;
  outside-root error; zip `..` rejection; row-group flushing verified with
  a tiny `max_row_group_bytes`; compression options produce readable files.
- **Rust (cli):** integration test spawning `CARGO_BIN_EXE_pfs` — happy
  path per source kind, exit codes and stderr on failure.
- **Python:** `pack()` then `ParquetFileSystem` round-trip for glob, list
  and zip inputs; error-mapping cases.
- **Existing suites** (index/archive/fs/adapters/http) must pass unchanged
  after the workspace move.

## 7. Documentation

README gains a "Creating archives" section covering `pfs pack` (with
`cargo install parquet-file-fs-cli`) and `parquet_file_fs.pack`, plus notes
on root/stored-path behavior. Development section updated for the
workspace layout.

## Out of scope

- Sharded output (`--max-shard-bytes`) — single file only for now.
- Extra metadata columns (size, mtime, mime) — path + content only.
- Packing from remote sources (s3/http) — local files and zips only.
- tar/tar.gz input.
