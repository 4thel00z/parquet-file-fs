# Design: `pack` — create parquet archives from a glob or archive file

**Date:** 2026-07-25
**Status:** Approved

## Problem

parquet-file-fs reads parquet "archive" shards (one row per file: a path
column + a content column) but offers no way to create them. Users need to
pack files — matched by a glob, listed explicitly, or contained in an
archive file (zip, tar, tar.gz, rar, …) — into a shard that
`ParquetFileSystem` can read.

## Decision summary

- Packing logic lives in the Rust core and is exposed twice: a Rust CLI
  binary (`pfs`) and Python functions (`parquet_file_fs.pack` /
  `pack_archive`).
- The repo becomes a cargo workspace with three crates: core, py (pyo3
  extension), cli.
- **No source-type magic.** A glob stores exactly what it matches — a
  matched `.zip` is stored as a file like any other. Expanding an archive
  is always an explicit call (`pack_archive` / `pfs pack-archive`).
- `pack_archive` handles multiple formats (zip, tar, tar.{gz,bz2,xz,zst},
  rar, 7z) with format detection by magic bytes inside the explicit call,
  plus a manual override.
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

pub enum ArchiveFormat { Zip, Tar, TarGz, TarBz2, TarXz, TarZst, Rar, SevenZ }

pub fn pack_glob(pattern: &str, root: Option<&Path>, out: &Path,
                 opts: &PackOptions) -> Result<PackSummary, FsError>;
pub fn pack_files(paths: &[PathBuf], root: &Path, out: &Path,
                  opts: &PackOptions) -> Result<PackSummary, FsError>;
pub fn pack_archive(archive: &Path, format: Option<ArchiveFormat>,
                    out: &Path, opts: &PackOptions)
                    -> Result<PackSummary, FsError>;
```

### Shared semantics

- **Schema:** `path: Utf8` (non-null) + `content: LargeBinary` (non-null) —
  types the existing reader auto-detects. Column names come from
  `PackOptions` so odd archives can be produced for interop.
- **Streaming:** entries are read one at a time into arrow builders
  (`StringBuilder` + `LargeBinaryBuilder`); a row group is flushed whenever
  accumulated content reaches `max_row_group_bytes`. Peak memory ≈
  threshold + largest single entry. 32 MiB default keeps the reader's lazy
  per-row-group decode cheap.
- **Compression:** parquet page compression, zstd by default; `snappy` and
  `none` selectable.
- **Duplicates:** two inputs normalizing to the same stored path → error
  (mirrors the reader's default duplicate policy).
- **Empty input:** zero matched files / entries → error ("no files
  matched" / "archive contains no files").

### Glob & file-list sources

- **Root / stored paths:** stored path = source path made relative to
  `root`, separators normalized to `/`. Default root for `pack_glob` is
  the pattern's wildcard-free directory prefix
  (`data/images/**/*.png` → root `data/images`, stored `a/x.png`); the
  current directory when the pattern has no fixed prefix. `pack_files`
  requires an explicit root. A matched file that does not live under root
  is an error.
- **Expansion:** via the `glob` crate. Only regular files are stored;
  directories are skipped; symlinks to files are read through. Matched
  archive files (zip/tar/…) are stored as ordinary file content — no
  expansion.
- **Determinism:** matched paths are sorted lexicographically before
  writing (filesystem traversal order is not stable).

### Archive sources (`pack_archive`)

- **Formats:** zip, tar, tar.gz, tar.bz2, tar.xz, tar.zst, rar, 7z.
- **Detection:** by magic bytes (zip `PK`, gzip `1f 8b`, bzip2 `BZh`, xz,
  zstd, rar `Rar!`, 7z `37 7A BC AF 27 1C`; tar via `ustar` at offset
  257), with file extension as tie-breaker. A compressed stream (gz/bz2/xz/zst) is assumed to contain a
  tar; a bare compressed non-tar file fails with a clear message. The
  `format` parameter overrides detection.
- **Stored paths:** entry names verbatim after normalization (forward
  slashes, strip any leading `/`, reject entries containing `..`
  components or whose name normalizes to empty).
- **Entry filtering:** only regular file entries are stored; directories,
  symlinks, hardlinks and special entries are skipped.
- **Order:** entries are written in archive order (already deterministic
  for a given archive; avoids a second decompression pass for tar
  streams).
- **Dependencies:** `zip`, `tar`, `flate2` (gz), `bzip2` (bz2),
  `liblzma`/`xz2` (xz), `zstd` (zst), `sevenz-rust2` (7z, pure Rust — no
  feature gate needed) in core. **Rar** uses the `unrar`
  crate (bindings to the vendored unrar C++ library; its license permits
  decompression use but is not OSI-approved) behind a core cargo feature
  `rar`, enabled by default in the cli and py crates. Without the feature,
  rar input fails with "rar support not compiled in". If the C++ build
  proves problematic in wheel CI, the feature ships off in wheels and the
  error message points to the CLI.

## 3. CLI (`pfs`)

```
pfs pack <GLOB|DIR> <OUT.parquet>
         [--root DIR]
         [--path-column NAME] [--content-column NAME]
         [--compression zstd|snappy|none]

pfs pack-archive <ARCHIVE> <OUT.parquet>
         [--format zip|tar|tar.gz|tar.bz2|tar.xz|tar.zst|rar|7z]
         [--path-column NAME] [--content-column NAME]
         [--compression zstd|snappy|none]
```

- `pfs pack`: an existing directory is shorthand for `dir/**` with
  root = dir (a directory cannot itself be stored, so this is not
  ambiguous); anything else is a glob pattern. Archive files matched by
  the glob are stored as bytes, never expanded.
- `pfs pack-archive`: explicitly expands one archive file; format detected
  from magic bytes unless `--format` is given.
- Success prints a one-line summary (`packed N files (M bytes) -> out`);
  errors print to stderr and exit 1.
- Arg parsing with `clap` (cli crate only; wheel and core stay lean).

## 4. Python API

```python
from parquet_file_fs import pack, pack_archive

pack("data/images/**/*.png", "out.parquet", root="data")
pack(["a.txt", "b.txt"], "out.parquet", root=".")
pack_archive("bundle.tar.gz", "out.parquet")
pack_archive("weird-extension.bin", "out.parquet", format="zip")
```

- `pack(source, out, *, root=None, path_column="path",
  content_column="content", compression="zstd")` — `source` is a glob
  string, an existing directory (shorthand for `dir/**`), or a list of
  files (`root` required). Never expands archives.
- `pack_archive(archive, out, *, format=None, path_column="path",
  content_column="content", compression="zstd")` — `format` is one of
  `"zip" | "tar" | "tar.gz" | "tar.bz2" | "tar.xz" | "tar.zst" | "rar" |
  "7z"`, default auto-detect by magic bytes.
- Both return `{"files": n, "bytes": total, "path": out}`.
- Implemented as pyo3 functions in the py crate, wrapped in a small
  `python/parquet_file_fs/pack.py`, re-exported from `__init__.py`.
- Errors map through the existing `to_py`: bad input → `ValueError`,
  I/O failures → `OSError`, missing files → `FileNotFoundError`.

## 5. Error handling

Reuse `FsError`, adding variants only where existing ones don't fit
(e.g. `Pack(String)` for duplicate/empty/outside-root/unsupported-format
cases if `Schema` reads wrong). The CLI maps any error to exit code 1 with
the display message; Python inherits the existing exception mapping.

## 6. Testing

- **Rust (core):** pack → read back through the existing `Archive` for
  glob, explicit files, and each archive format (fixtures generated in the
  test via the same third-party crates); format auto-detection incl.
  extension-vs-magic disagreement; duplicate-path error; empty-match
  error; outside-root error; `..` entry rejection; non-regular entries
  skipped; row-group flushing verified with a tiny `max_row_group_bytes`;
  compression options produce readable files. Rar round-trip runs only
  with the `rar` feature (fixture checked in — rar cannot be created by
  the test); the 7z fixture is checked in as well if `sevenz-rust2`
  cannot write archives.
- **Rust (cli):** integration test spawning `CARGO_BIN_EXE_pfs` — happy
  path for glob, dir and archive sources; exit codes and stderr on
  failure.
- **Python:** `pack()` / `pack_archive()` then `ParquetFileSystem`
  round-trip; a `.zip` matched by a glob is stored as bytes, not expanded;
  error-mapping cases.
- **Existing suites** (index/archive/fs/adapters/http) must pass unchanged
  after the workspace move.

## 7. Documentation

README gains a "Creating archives" section covering `pfs pack` /
`pfs pack-archive` (with `cargo install parquet-file-fs-cli`) and
`parquet_file_fs.pack` / `pack_archive`, plus notes on root/stored-path
behavior and supported archive formats. Development section updated for
the workspace layout.

## Out of scope

- Sharded output (`--max-shard-bytes`) — single file only for now.
- Extra metadata columns (size, mtime, mime) — path + content only.
- Packing from remote sources (s3/http) — local files and archives only.
- Nested expansion (archives inside archives).
