# pack / pack_archive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create parquet archive shards (one row per file: `path` + `content`) from a glob/file-list or by expanding a zip/tar/tar.*/rar/7z archive, exposed as a Rust CLI (`pfs`) and Python API (`parquet_file_fs.pack` / `pack_archive`).

**Architecture:** The repo becomes a cargo workspace: `crates/core` (existing reader lib + new `pack.rs`), `crates/py` (pyo3 cdylib `_core`), `crates/cli` (`pfs` binary). Packing streams entries one at a time into arrow builders and flushes a parquet row group every 32 MiB. No source-type magic: globs store matched archive files as bytes; expansion is always the explicit `pack_archive` surface.

**Tech Stack:** Rust (arrow/parquet 55, pyo3 0.25 abi3-py39, clap 4, zip, tar, flate2, bzip2, liblzma, zstd, sevenz-rust2, unrar behind a `rar` feature), maturin mixed layout, pytest.

**Spec:** `docs/superpowers/specs/2026-07-25-pack-glob-zip-design.md`

## Global Constraints

- Runtime Python dependency stays exactly `fsspec>=2024.2.0`; pyarrow is dev-only.
- `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test --locked` must pass at every commit; run `cargo update --workspace` whenever a member crate is added/renamed so `--locked` keeps working.
- Root `Cargo.toml` must keep a line matching `^version = "[^"]+" # x-release-please-version$` (release-please generic updater + `tests/test_release_meta.py` regex).
- Python support floor is 3.9 (`abi3-py39`); wheel module is `parquet_file_fs._core`.
- Conventional commit messages (release-please derives releases from them).
- Format strings use inline args (`format!("...{e}")`) — matches existing style and clippy's `uninlined_format_args`.
- Python invocations use `uv run ...` (`uv run maturin develop`, `uv run pytest`).
- Reader compatibility: path column `Utf8` non-null, content column `LargeBinary` non-null — these are in the reader's auto-detect sets (`src/index.rs` `PATH_NAMES`/`CONTENT_NAMES`, `src/archive.rs` `binary_value`).

---

### Task 1: Cargo workspace restructure (core + py crates, no behavior change)

**Files:**
- Create: `Cargo.toml` (workspace root, replaces current package manifest)
- Create: `crates/core/Cargo.toml`
- Create: `crates/py/Cargo.toml`
- Move: `src/{adapter,archive,chunk_reader,index,native}.rs` → `crates/core/src/`
- Move: `src/py.rs` + pymodule block of `src/lib.rs` → `crates/py/src/lib.rs`
- Create: `crates/core/src/lib.rs` (module declarations only)
- Move: `tests/index_test.rs`, `tests/archive_test.rs`, `tests/common/` → `crates/core/tests/`
- Modify: `pyproject.toml` (`[tool.maturin] manifest-path`)
- Modify: `.github/workflows/python-ci.yml` (paths filters `src/**` → `crates/**`)
- Modify: `tests/test_release_meta.py` (workspace version lookup, multi-crate lock check)

**Interfaces:**
- Consumes: existing code as-is.
- Produces: workspace where `parquet-file-fs` (lib `parquet_file_fs`) lives at `crates/core` with `pub mod adapter/archive/chunk_reader/index/native`, and `parquet-file-fs-py` at `crates/py` builds the `_core` extension. All later tasks depend on these paths.

- [ ] **Step 1: Move the Rust sources with git mv**

```bash
mkdir -p crates/core/src crates/core/tests crates/py/src
git mv src/adapter.rs src/archive.rs src/chunk_reader.rs src/index.rs src/native.rs crates/core/src/
git mv src/py.rs crates/py/src/lib.rs
git mv tests/index_test.rs tests/archive_test.rs tests/common crates/core/tests/
git rm src/lib.rs
```

- [ ] **Step 2: Write the root workspace Cargo.toml** (replace entire file)

```toml
[workspace]
resolver = "2"
members = ["crates/core", "crates/py"]

[workspace.package]
version = "0.1.0" # x-release-please-version
edition = "2021"
license = "MIT OR Apache-2.0"
repository = "https://github.com/4thel00z/parquet-file-fs"

# Release builds trade compile time for cross-crate inlining on the
# parquet decode path. `panic` stays "unwind": the extension module relies on
# unwinding to turn Rust panics into Python exceptions rather than aborting
# the interpreter.
[profile.release]
lto = "thin"
codegen-units = 1
```

- [ ] **Step 3: Write `crates/core/Cargo.toml`**

```toml
[package]
name = "parquet-file-fs"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
description = "Read-only fsspec filesystem over parquet archives: ls, cat and glob the files stored inside parquet shards"
keywords = ["parquet", "fsspec", "filesystem", "arrow", "dataset"]
categories = ["filesystem"]

[lib]
name = "parquet_file_fs"

[dependencies]
parquet = { version = "55", features = ["arrow"] }
arrow-array = "55"
arrow-cast = "55"
arrow-schema = "55"
object_store = { version = "0.12", features = ["aws", "http"] }
tokio = { version = "1", features = ["rt-multi-thread"] }
futures = "0.3"
bytes = "1"
glob = "0.3"
globset = "0.4"
lru = "0.12"
once_cell = "1"
thiserror = "2"
url = "2"

[dev-dependencies]
tempfile = "3"
```

(No `pyo3`, no `crate-type = ["cdylib"]` — the extension moves to the py crate.)

- [ ] **Step 4: Write `crates/core/src/lib.rs`**

```rust
pub mod adapter;
pub mod archive;
pub mod chunk_reader;
pub mod index;
pub mod native;
```

- [ ] **Step 5: Write `crates/py/Cargo.toml`**

```toml
[package]
name = "parquet-file-fs-py"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
description = "pyo3 extension module for parquet-file-fs"
publish = false

[lib]
name = "parquet_file_fs_py"
crate-type = ["cdylib"]

[features]
default = []
extension-module = ["pyo3/extension-module"]

[dependencies]
parquet-file-fs = { path = "../core" }
pyo3 = { version = "0.25", features = ["abi3-py39"] }
bytes = "1"
```

- [ ] **Step 6: Fix up `crates/py/src/lib.rs`** (the moved `py.rs`)

At the top, replace the `use crate::...` imports with core-crate paths and append the pymodule from the old `src/lib.rs`:

```rust
use parquet_file_fs::adapter::{FsError, RangeReader};
use parquet_file_fs::archive::InfoResult;
use parquet_file_fs::index::{normalize, DupPolicy, MetaValue};
```

In the body, replace the remaining `crate::` references: `crate::adapter::register` → `parquet_file_fs::adapter::register`, `crate::archive::Archive` → `parquet_file_fs::archive::Archive` (two sites: `register_adapter`, `PyArchive::new`; also the `inner: crate::archive::Archive` field type). Remove `pub` from items that no longer need exporting is NOT required — leave visibility as moved. At the end of the file add:

```rust
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<PyArchive>()?;
    m.add_function(wrap_pyfunction!(register_adapter, m)?)?;
    Ok(())
}
```

- [ ] **Step 7: Point maturin at the py crate** — in `pyproject.toml` replace the `[tool.maturin]` table with:

```toml
[tool.maturin]
manifest-path = "crates/py/Cargo.toml"
python-source = "python"
module-name = "parquet_file_fs._core"
features = ["extension-module"]
```

- [ ] **Step 8: Update CI paths filters** — in `.github/workflows/python-ci.yml`, replace both occurrences of `- "src/**"` (push + pull_request) with `- "crates/**"`.

- [ ] **Step 9: Update `tests/test_release_meta.py` for the workspace**

Replace `_package_version` and `test_lockfile_pins_current_version`:

```python
def _package_version() -> str:
    manifest = tomllib.loads((REPO / "Cargo.toml").read_text())
    return manifest["workspace"]["package"]["version"]
```

```python
    def test_lockfile_pins_current_version(self) -> None:
        """Cargo.lock matches the workspace version for every member crate.

        A mismatch is exactly the state a release tag ends up in when the
        version bump lands without a lockfile regeneration.
        """
        version = _package_version()
        lock = tomllib.loads((REPO / "Cargo.lock").read_text())
        pinned = {
            pkg["name"]: pkg["version"]
            for pkg in lock["package"]
            if pkg["name"].startswith("parquet-file-fs")
        }
        assert pinned, "parquet-file-fs crates not found in Cargo.lock"
        assert set(pinned.values()) == {version}, (
            f"Cargo.lock pins {pinned} but the workspace is at {version}; "
            "run `cargo update --workspace` and commit the lockfile"
        )
```

- [ ] **Step 10: Sync the lockfile and run the Rust suite**

Run: `cargo update --workspace && cargo test --locked`
Expected: all existing tests pass (adapter/chunk_reader unit tests, index_test, archive_test).

- [ ] **Step 11: Lint**

Run: `cargo fmt --all && cargo clippy --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 12: Rebuild the extension and run the Python suite**

Run: `uv run maturin develop && uv run pytest -q`
Expected: all Python tests pass, including the updated `test_release_meta.py`.

- [ ] **Step 13: Commit**

```bash
git add -A
git commit -m "refactor: split into a cargo workspace (core + py crates)"
```

---

### Task 2: Core pack module — options, writer, `pack_files`

**Files:**
- Modify: `crates/core/src/adapter.rs` (add `FsError::Pack`)
- Create: `crates/core/src/pack.rs`
- Modify: `crates/core/src/lib.rs` (add `pub mod pack;`)
- Test: `crates/core/tests/pack_test.rs`

**Interfaces:**
- Consumes: `FsError` (`crate::adapter`), `Archive::open` (read-back in tests).
- Produces (used by Tasks 3–7):
  - `pub enum PackCompression { Zstd, Snappy, None }` with `pub fn parse(s: &str) -> Result<Self, FsError>` (`"zstd" | "snappy" | "none"`)
  - `pub struct PackOptions { pub path_column: String, pub content_column: String, pub compression: PackCompression, pub max_row_group_bytes: usize }` with `Default` (`"path"`, `"content"`, `Zstd`, 32 MiB)
  - `pub struct PackSummary { pub files: u64, pub bytes: u64 }`
  - `pub fn pack_files(paths: &[PathBuf], root: &Path, out: &Path, opts: &PackOptions) -> Result<PackSummary, FsError>`
  - crate-internal: `struct PackWriter` (`create`, `append(stored: String, content: &[u8])`, `flush_row_group`, `finish`), `fn stored_path(file: &Path, root: &Path) -> Result<String, FsError>`, `fn write_pairs(pairs: Vec<(String, PathBuf)>, out: &Path, opts: &PackOptions) -> Result<PackSummary, FsError>`

- [ ] **Step 1: Add the error variant** — in `crates/core/src/adapter.rs`, inside `enum FsError` after the `Io` variant add:

```rust
    #[error("{0}")]
    Pack(String),
```

- [ ] **Step 2: Write the failing tests** — `crates/core/tests/pack_test.rs`:

```rust
use std::path::{Path, PathBuf};

use parquet_file_fs::archive::Archive;
use parquet_file_fs::index::DupPolicy;
use parquet_file_fs::pack::{pack_files, PackCompression, PackOptions};

fn open(out: &Path) -> Archive {
    Archive::open(
        &[out.to_str().unwrap().to_string()],
        None,
        None,
        DupPolicy::Error,
    )
    .unwrap()
}

fn tree(dir: &Path) -> Vec<PathBuf> {
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    std::fs::write(dir.join("a.txt"), b"alpha").unwrap();
    std::fs::write(dir.join("sub/b.bin"), b"beta").unwrap();
    vec![dir.join("a.txt"), dir.join("sub/b.bin")]
}

#[test]
fn pack_files_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let files = tree(tmp.path());
    let out = tmp.path().join("out.parquet");
    let s = pack_files(&files, tmp.path(), &out, &PackOptions::default()).unwrap();
    assert_eq!((s.files, s.bytes), (2, 9));
    let a = open(&out);
    assert_eq!(a.read("a.txt").unwrap(), b"alpha");
    assert_eq!(a.read("sub/b.bin").unwrap(), b"beta");
    assert_eq!(a.paths(), vec!["a.txt".to_string(), "sub/b.bin".to_string()]);
}

#[test]
fn pack_files_rejects_duplicates() {
    let tmp = tempfile::tempdir().unwrap();
    let files = tree(tmp.path());
    let dup = vec![files[0].clone(), files[0].clone()];
    let out = tmp.path().join("out.parquet");
    let err = pack_files(&dup, tmp.path(), &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("duplicate path 'a.txt'"), "{err}");
    assert!(!out.exists(), "failed pack must not leave output behind");
}

#[test]
fn pack_files_rejects_out_of_root_and_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let files = tree(tmp.path());
    let out = tmp.path().join("out.parquet");
    let err = pack_files(&files, &tmp.path().join("sub"), &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("outside root"), "{err}");
    let err = pack_files(&[], tmp.path(), &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("no files"), "{err}");
}

#[test]
fn row_groups_flush_at_threshold() {
    let tmp = tempfile::tempdir().unwrap();
    let mut files = Vec::new();
    for i in 0..4 {
        let p = tmp.path().join(format!("f{i}.bin"));
        std::fs::write(&p, vec![b'x'; 10]).unwrap();
        files.push(p);
    }
    let out = tmp.path().join("out.parquet");
    let opts = PackOptions {
        max_row_group_bytes: 20,
        ..PackOptions::default()
    };
    pack_files(&files, tmp.path(), &out, &opts).unwrap();
    let f = std::fs::File::open(&out).unwrap();
    let reader =
        parquet::file::reader::SerializedFileReader::new(f).unwrap();
    use parquet::file::reader::FileReader;
    assert_eq!(reader.metadata().num_row_groups(), 2); // 2 files per 20-byte group
    let a = open(&out);
    assert_eq!(a.read("f3.bin").unwrap(), vec![b'x'; 10]);
}

#[test]
fn compression_variants_are_readable() {
    let tmp = tempfile::tempdir().unwrap();
    let files = tree(tmp.path());
    for comp in ["zstd", "snappy", "none"] {
        let out = tmp.path().join(format!("out-{comp}.parquet"));
        let opts = PackOptions {
            compression: PackCompression::parse(comp).unwrap(),
            ..PackOptions::default()
        };
        pack_files(&files, tmp.path(), &out, &opts).unwrap();
        assert_eq!(open(&out).read("a.txt").unwrap(), b"alpha");
    }
    assert!(PackCompression::parse("brotli").is_err());
}

#[test]
fn custom_column_names() {
    let tmp = tempfile::tempdir().unwrap();
    let files = tree(tmp.path());
    let out = tmp.path().join("out.parquet");
    let opts = PackOptions {
        path_column: "file_name".into(),
        content_column: "image_bytes".into(),
        ..PackOptions::default()
    };
    pack_files(&files, tmp.path(), &out, &opts).unwrap();
    let a = Archive::open(
        &[out.to_str().unwrap().to_string()],
        Some("file_name"),
        Some("image_bytes"),
        DupPolicy::Error,
    )
    .unwrap();
    assert_eq!(a.read("a.txt").unwrap(), b"alpha");
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p parquet-file-fs --test pack_test`
Expected: compile error — `parquet_file_fs::pack` does not exist.

- [ ] **Step 4: Implement `crates/core/src/pack.rs`** and add `pub mod pack;` to `crates/core/src/lib.rs`:

```rust
//! Write parquet archive shards: one row per file (path + content).

use std::collections::HashSet;
use std::fs::File;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use arrow_array::builder::{ArrayBuilder, LargeBinaryBuilder, StringBuilder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::adapter::FsError;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PackCompression {
    Zstd,
    Snappy,
    None,
}

impl PackCompression {
    pub fn parse(s: &str) -> Result<Self, FsError> {
        match s {
            "zstd" => Ok(Self::Zstd),
            "snappy" => Ok(Self::Snappy),
            "none" => Ok(Self::None),
            other => Err(FsError::Pack(format!(
                "compression must be 'zstd', 'snappy' or 'none', got '{other}'"
            ))),
        }
    }

    fn to_parquet(self) -> Compression {
        match self {
            Self::Zstd => Compression::ZSTD(ZstdLevel::default()),
            Self::Snappy => Compression::SNAPPY,
            Self::None => Compression::UNCOMPRESSED,
        }
    }
}

pub struct PackOptions {
    pub path_column: String,
    pub content_column: String,
    pub compression: PackCompression,
    /// Flush a row group once buffered content reaches this many bytes.
    /// Small groups keep the reader's lazy per-row-group decode cheap.
    pub max_row_group_bytes: usize,
}

impl Default for PackOptions {
    fn default() -> Self {
        Self {
            path_column: "path".into(),
            content_column: "content".into(),
            compression: PackCompression::Zstd,
            max_row_group_bytes: 32 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PackSummary {
    pub files: u64,
    pub bytes: u64,
}

fn io_err(path: &Path, e: std::io::Error) -> FsError {
    FsError::Io {
        url: path.display().to_string(),
        source: e,
    }
}

fn pq_err(path: &Path, e: parquet::errors::ParquetError) -> FsError {
    FsError::Parquet {
        url: path.display().to_string(),
        source: e,
    }
}

pub(crate) struct PackWriter {
    out: PathBuf,
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    paths: StringBuilder,
    contents: LargeBinaryBuilder,
    pending: usize,
    max_pending: usize,
    seen: HashSet<String>,
    files: u64,
    bytes: u64,
}

impl PackWriter {
    pub(crate) fn create(out: &Path, opts: &PackOptions) -> Result<Self, FsError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(&opts.path_column, DataType::Utf8, false),
            Field::new(&opts.content_column, DataType::LargeBinary, false),
        ]));
        let file = File::create(out).map_err(|e| io_err(out, e))?;
        let props = WriterProperties::builder()
            .set_compression(opts.compression.to_parquet())
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| pq_err(out, e))?;
        Ok(Self {
            out: out.to_path_buf(),
            writer,
            schema,
            paths: StringBuilder::new(),
            contents: LargeBinaryBuilder::new(),
            pending: 0,
            max_pending: opts.max_row_group_bytes.max(1),
            seen: HashSet::new(),
            files: 0,
            bytes: 0,
        })
    }

    pub(crate) fn append(&mut self, stored: String, content: &[u8]) -> Result<(), FsError> {
        if !self.seen.insert(stored.clone()) {
            return Err(FsError::Pack(format!("duplicate path '{stored}' in input")));
        }
        self.paths.append_value(&stored);
        self.contents.append_value(content);
        self.pending += content.len();
        self.files += 1;
        self.bytes += content.len() as u64;
        if self.pending >= self.max_pending {
            self.flush_row_group()?;
        }
        Ok(())
    }

    fn flush_row_group(&mut self) -> Result<(), FsError> {
        if ArrayBuilder::len(&self.paths) == 0 {
            return Ok(());
        }
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(self.paths.finish()), Arc::new(self.contents.finish())],
        )
        .map_err(|e| FsError::Schema(e.to_string()))?;
        self.writer.write(&batch).map_err(|e| pq_err(&self.out, e))?;
        // ArrowWriter::flush closes the in-progress row group.
        self.writer.flush().map_err(|e| pq_err(&self.out, e))?;
        self.pending = 0;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<PackSummary, FsError> {
        self.flush_row_group()?;
        self.writer.close().map_err(|e| pq_err(&self.out, e))?;
        Ok(PackSummary {
            files: self.files,
            bytes: self.bytes,
        })
    }
}

/// Strip `.` components so textual prefix-matching works for roots like `.`.
fn clean(p: &Path) -> PathBuf {
    p.components()
        .filter(|c| !matches!(c, Component::CurDir))
        .collect()
}

/// Virtual path for `file` relative to `root`, `/`-separated.
pub(crate) fn stored_path(file: &Path, root: &Path) -> Result<String, FsError> {
    let cf = clean(file);
    let cr = clean(root);
    let rel = cf.strip_prefix(&cr).map_err(|_| {
        FsError::Pack(format!(
            "file '{}' is outside root '{}'",
            file.display(),
            root.display()
        ))
    })?;
    let mut segs = Vec::new();
    for c in rel.components() {
        match c {
            Component::Normal(s) => segs.push(s.to_string_lossy().into_owned()),
            _ => {
                return Err(FsError::Pack(format!(
                    "path '{}' escapes the pack root",
                    file.display()
                )))
            }
        }
    }
    if segs.is_empty() {
        return Err(FsError::Pack(format!(
            "'{}' yields an empty stored path",
            file.display()
        )));
    }
    Ok(segs.join("/"))
}

/// Stream sorted (stored_path, source_file) pairs into `out`; removes `out`
/// on failure so errors never leave a partial shard behind.
pub(crate) fn write_pairs(
    mut pairs: Vec<(String, PathBuf)>,
    out: &Path,
    opts: &PackOptions,
) -> Result<PackSummary, FsError> {
    pairs.sort();
    let mut w = PackWriter::create(out, opts)?;
    let res = (|| {
        for (stored, src) in pairs {
            let data = std::fs::read(&src).map_err(|e| io_err(&src, e))?;
            w.append(stored, &data)?;
        }
        w.finish()
    })();
    if res.is_err() {
        let _ = std::fs::remove_file(out);
    }
    res
}

pub fn pack_files(
    paths: &[PathBuf],
    root: &Path,
    out: &Path,
    opts: &PackOptions,
) -> Result<PackSummary, FsError> {
    if paths.is_empty() {
        return Err(FsError::Pack("no files to pack".into()));
    }
    let mut pairs = Vec::with_capacity(paths.len());
    for p in paths {
        let md = std::fs::metadata(p).map_err(|e| io_err(p, e))?;
        if !md.is_file() {
            return Err(FsError::Pack(format!(
                "'{}' is not a regular file",
                p.display()
            )));
        }
        pairs.push((stored_path(p, root)?, p.clone()));
    }
    write_pairs(pairs, out, opts)
}
```

Note the borrow in `PackWriter::append`: `self.paths.append_value(&stored)` happens after the `seen.insert(stored.clone())` check — keep that order.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p parquet-file-fs --test pack_test`
Expected: all 6 tests PASS.

- [ ] **Step 6: Lint and full suite**

Run: `cargo fmt --all && cargo clippy --all-targets -- -D warnings && cargo test --locked`
Expected: clean; existing suites unaffected.

- [ ] **Step 7: Commit**

```bash
git add crates/core docs Cargo.lock
git commit -m "feat: core pack writer and pack_files"
```

---

### Task 3: `pack_glob` (glob + directory shorthand + root inference)

**Files:**
- Modify: `crates/core/src/pack.rs`
- Test: `crates/core/tests/pack_test.rs` (append), `crates/core/src/pack.rs` (`#[cfg(test)]` for prefix helper)

**Interfaces:**
- Consumes: `stored_path`, `write_pairs`, `PackOptions` from Task 2.
- Produces: `pub fn pack_glob(pattern: &str, root: Option<&Path>, out: &Path, opts: &PackOptions) -> Result<PackSummary, FsError>`. Behavior: directory pattern → packs `dir/**/*` with root = dir; default root = wildcard-free directory prefix of the pattern; matched archives stored as bytes.

- [ ] **Step 1: Write the failing tests** — append to `crates/core/tests/pack_test.rs`:

```rust
use parquet_file_fs::pack::pack_glob;

#[test]
fn pack_glob_roundtrip_with_inferred_root() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    tree(&data);
    let out = tmp.path().join("out.parquet");
    let pattern = format!("{}/**/*", data.display());
    let s = pack_glob(&pattern, None, &out, &PackOptions::default()).unwrap();
    assert_eq!(s.files, 2);
    let a = open(&out);
    // root inferred as `<tmp>/data`, so paths are relative to it
    assert_eq!(a.paths(), vec!["a.txt".to_string(), "sub/b.bin".to_string()]);
}

#[test]
fn pack_glob_explicit_root() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    tree(&data);
    let out = tmp.path().join("out.parquet");
    let pattern = format!("{}/**/*", data.display());
    pack_glob(&pattern, Some(tmp.path()), &out, &PackOptions::default()).unwrap();
    let a = open(&out);
    assert_eq!(
        a.paths(),
        vec!["data/a.txt".to_string(), "data/sub/b.bin".to_string()]
    );
}

#[test]
fn pack_glob_directory_shorthand() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    tree(&data);
    let out = tmp.path().join("out.parquet");
    let s = pack_glob(data.to_str().unwrap(), None, &out, &PackOptions::default()).unwrap();
    assert_eq!(s.files, 2);
    assert_eq!(open(&out).read("sub/b.bin").unwrap(), b"beta");
}

#[test]
fn pack_glob_no_match_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("out.parquet");
    let pattern = format!("{}/nope/**/*", tmp.path().display());
    let err = pack_glob(&pattern, None, &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("no files matched"), "{err}");
    assert!(!out.exists());
}

#[test]
fn pack_glob_stores_matched_archives_as_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    // minimal zip built with the zip crate (a dependency from Task 4 — for
    // Task 3, write literal bytes instead: an empty-zip magic is enough)
    std::fs::write(data.join("bundle.zip"), b"PK\x05\x06 not really a full zip").unwrap();
    let out = tmp.path().join("out.parquet");
    pack_glob(
        &format!("{}/**/*", data.display()),
        None,
        &out,
        &PackOptions::default(),
    )
    .unwrap();
    let a = open(&out);
    assert_eq!(a.paths(), vec!["bundle.zip".to_string()]);
    assert!(a.read("bundle.zip").unwrap().starts_with(b"PK"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p parquet-file-fs --test pack_test`
Expected: compile error — `pack_glob` not found.

- [ ] **Step 3: Implement in `crates/core/src/pack.rs`**

```rust
/// Longest wildcard-free directory prefix of a glob pattern; the last
/// segment never counts (a concrete filename is not its own root).
fn glob_fixed_prefix(pattern: &str) -> PathBuf {
    let segs: Vec<&str> = pattern.split('/').collect();
    let mut prefix = PathBuf::new();
    for (i, seg) in segs.iter().enumerate() {
        if i + 1 == segs.len() || seg.contains(['*', '?', '[']) {
            break;
        }
        if seg.is_empty() {
            prefix.push("/"); // leading empty segment of an absolute pattern
            continue;
        }
        prefix.push(seg);
    }
    prefix
}

pub fn pack_glob(
    pattern: &str,
    root: Option<&Path>,
    out: &Path,
    opts: &PackOptions,
) -> Result<PackSummary, FsError> {
    let (pattern, default_root) = if Path::new(pattern).is_dir() {
        let dir = pattern.trim_end_matches('/');
        (format!("{dir}/**/*"), PathBuf::from(dir))
    } else {
        (pattern.to_string(), glob_fixed_prefix(pattern))
    };
    let root = root.map(Path::to_path_buf).unwrap_or(default_root);
    let mut pairs = Vec::new();
    for entry in glob::glob(&pattern)
        .map_err(|e| FsError::Pack(format!("bad glob pattern '{pattern}': {e}")))?
    {
        let p = entry.map_err(|e| {
            let path = e.path().display().to_string();
            FsError::Io {
                url: path,
                source: e.into_error(),
            }
        })?;
        if p.is_file() {
            pairs.push((stored_path(&p, &root)?, p));
        }
    }
    if pairs.is_empty() {
        return Err(FsError::Pack(format!("no files matched '{pattern}'")));
    }
    write_pairs(pairs, out, opts)
}
```

Add a unit test module at the bottom of `pack.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_prefix_of_patterns() {
        assert_eq!(glob_fixed_prefix("data/images/**/*.png"), PathBuf::from("data/images"));
        assert_eq!(glob_fixed_prefix("*.txt"), PathBuf::new());
        assert_eq!(glob_fixed_prefix("a/b/c.txt"), PathBuf::from("a/b"));
        assert_eq!(glob_fixed_prefix("/abs/dir/*.bin"), PathBuf::from("/abs/dir"));
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p parquet-file-fs`
Expected: PASS (pack_test + unit tests).

- [ ] **Step 5: Lint, then commit**

Run: `cargo fmt --all && cargo clippy --all-targets -- -D warnings`

```bash
git add crates/core
git commit -m "feat: pack_glob with root inference and directory shorthand"
```

---

### Task 4: `pack_archive` — format detection, zip, tar family

**Files:**
- Modify: `crates/core/Cargo.toml` (new dependencies)
- Modify: `crates/core/src/pack.rs`
- Test: `crates/core/tests/pack_archive_test.rs` (new), `pack.rs` unit tests (detection)

**Interfaces:**
- Consumes: `PackWriter`, `PackOptions` from Task 2.
- Produces (used by Tasks 5–7):
  - `pub enum ArchiveFormat { Zip, Tar, TarGz, TarBz2, TarXz, TarZst, Rar, SevenZ }` with `pub fn parse(s: &str) -> Result<Self, FsError>` accepting `"zip" | "tar" | "tar.gz" | "tgz" | "tar.bz2" | "tbz2" | "tar.xz" | "txz" | "tar.zst" | "tzst" | "rar" | "7z"`
  - `pub fn pack_archive(archive: &Path, format: Option<ArchiveFormat>, out: &Path, opts: &PackOptions) -> Result<PackSummary, FsError>`
  - crate-internal: `fn entry_stored_path(raw: &str) -> Result<String, FsError>`, `fn sniff_format(archive: &Path) -> Result<ArchiveFormat, FsError>` (Rar/SevenZ variants exist now; their readers arrive in Task 5 as `Err(Pack("... support not implemented yet"))` stubs replaced in Task 5)

- [ ] **Step 1: Add dependencies**

```bash
cargo add -p parquet-file-fs zip --no-default-features --features "deflate,deflate64,bzip2,zstd,lzma"
cargo add -p parquet-file-fs tar flate2 bzip2 liblzma zstd
```

(`zstd`/`flate2` are already in the tree via parquet, this just makes them direct. `liblzma` is the maintained xz2 fork; its reader type is `liblzma::read::XzDecoder`.)

- [ ] **Step 2: Write the failing tests** — `crates/core/tests/pack_archive_test.rs`:

```rust
use std::io::Write;
use std::path::Path;

use parquet_file_fs::archive::Archive;
use parquet_file_fs::index::DupPolicy;
use parquet_file_fs::pack::{pack_archive, ArchiveFormat, PackOptions};

fn open(out: &Path) -> Archive {
    Archive::open(
        &[out.to_str().unwrap().to_string()],
        None,
        None,
        DupPolicy::Error,
    )
    .unwrap()
}

fn assert_roundtrip(out: &Path) {
    let a = open(out);
    assert_eq!(a.read("a.txt").unwrap(), b"alpha");
    assert_eq!(a.read("sub/b.bin").unwrap(), b"beta");
    assert_eq!(a.paths().len(), 2);
}

fn make_zip(path: &Path) {
    let f = std::fs::File::create(path).unwrap();
    let mut z = zip::ZipWriter::new(f);
    let o: zip::write::SimpleFileOptions = Default::default();
    z.add_directory("sub/", o).unwrap();
    z.start_file("a.txt", o).unwrap();
    z.write_all(b"alpha").unwrap();
    z.start_file("sub/b.bin", o).unwrap();
    z.write_all(b"beta").unwrap();
    z.finish().unwrap();
}

fn tar_bytes() -> Vec<u8> {
    let mut b = tar::Builder::new(Vec::new());
    let mut h = tar::Header::new_gnu();
    h.set_size(5);
    h.set_mode(0o644);
    h.set_cksum();
    b.append_data(&mut h, "a.txt", &b"alpha"[..]).unwrap();
    let mut h = tar::Header::new_gnu();
    h.set_size(4);
    h.set_mode(0o644);
    h.set_cksum();
    b.append_data(&mut h, "sub/b.bin", &b"beta"[..]).unwrap();
    b.into_inner().unwrap()
}

#[test]
fn zip_roundtrip_detected_by_magic() {
    let tmp = tempfile::tempdir().unwrap();
    let ar = tmp.path().join("weird-name.bin"); // detection must not need the extension
    make_zip(&ar);
    let out = tmp.path().join("out.parquet");
    let s = pack_archive(&ar, None, &out, &PackOptions::default()).unwrap();
    assert_eq!(s.files, 2); // the directory entry is skipped
    assert_roundtrip(&out);
}

#[test]
fn tar_and_compressed_tar_roundtrips() {
    let tmp = tempfile::tempdir().unwrap();
    let raw = tar_bytes();

    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("t.tar", raw.clone()),
        ("t.tar.gz", {
            let mut e = flate2::write::GzEncoder::new(Vec::new(), Default::default());
            e.write_all(&raw).unwrap();
            e.finish().unwrap()
        }),
        ("t.tar.bz2", {
            let mut e = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::default());
            e.write_all(&raw).unwrap();
            e.finish().unwrap()
        }),
        ("t.tar.xz", {
            let mut e = liblzma::write::XzEncoder::new(Vec::new(), 6);
            e.write_all(&raw).unwrap();
            e.finish().unwrap()
        }),
        ("t.tar.zst", zstd::stream::encode_all(&raw[..], 0).unwrap()),
    ];
    for (name, bytes) in cases {
        let ar = tmp.path().join(name);
        std::fs::write(&ar, bytes).unwrap();
        let out = tmp.path().join(format!("{name}.parquet"));
        pack_archive(&ar, None, &out, &PackOptions::default()).unwrap();
        assert_roundtrip(&out);
    }
}

#[test]
fn format_override_beats_detection() {
    let tmp = tempfile::tempdir().unwrap();
    let ar = tmp.path().join("mislabeled.zip"); // says zip, is tar
    std::fs::write(&ar, tar_bytes()).unwrap();
    let out = tmp.path().join("out.parquet");
    // magic sniffing sees ustar and wins over the extension
    pack_archive(&ar, None, &out, &PackOptions::default()).unwrap();
    assert_roundtrip(&out);
    // explicit override forces the wrong reader -> clear error
    let err = pack_archive(&ar, Some(ArchiveFormat::Zip), &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("zip"), "{err}");
}

#[test]
fn zip_slip_and_empty_archive_are_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let evil = tmp.path().join("evil.zip");
    let f = std::fs::File::create(&evil).unwrap();
    let mut z = zip::ZipWriter::new(f);
    let o: zip::write::SimpleFileOptions = Default::default();
    z.start_file("../escape.txt", o).unwrap();
    z.write_all(b"nope").unwrap();
    z.finish().unwrap();
    let out = tmp.path().join("out.parquet");
    let err = pack_archive(&evil, None, &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains(".."), "{err}");
    assert!(!out.exists());

    let empty = tmp.path().join("empty.zip");
    let zf = std::fs::File::create(&empty).unwrap();
    zip::ZipWriter::new(zf).finish().unwrap();
    let err = pack_archive(&empty, None, &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("contains no files"), "{err}");
}

#[test]
fn bare_gz_without_tar_gives_clear_error() {
    let tmp = tempfile::tempdir().unwrap();
    let ar = tmp.path().join("plain.gz");
    let mut e = flate2::write::GzEncoder::new(Vec::new(), Default::default());
    e.write_all(b"just text, no tar").unwrap();
    std::fs::write(&ar, e.finish().unwrap()).unwrap();
    let out = tmp.path().join("out.parquet");
    let err = pack_archive(&ar, None, &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("tar"), "{err}");
}

#[test]
fn unknown_format_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let ar = tmp.path().join("mystery.dat");
    std::fs::write(&ar, b"not an archive at all").unwrap();
    let out = tmp.path().join("out.parquet");
    let err = pack_archive(&ar, None, &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("could not detect"), "{err}");
    assert!(ArchiveFormat::parse("tar.lol").is_err());
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p parquet-file-fs --test pack_archive_test`
Expected: compile error — `pack_archive` / `ArchiveFormat` not found.

- [ ] **Step 4: Implement in `crates/core/src/pack.rs`**

```rust
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ArchiveFormat {
    Zip,
    Tar,
    TarGz,
    TarBz2,
    TarXz,
    TarZst,
    Rar,
    SevenZ,
}

impl ArchiveFormat {
    pub fn parse(s: &str) -> Result<Self, FsError> {
        match s {
            "zip" => Ok(Self::Zip),
            "tar" => Ok(Self::Tar),
            "tar.gz" | "tgz" => Ok(Self::TarGz),
            "tar.bz2" | "tbz2" => Ok(Self::TarBz2),
            "tar.xz" | "txz" => Ok(Self::TarXz),
            "tar.zst" | "tzst" => Ok(Self::TarZst),
            "rar" => Ok(Self::Rar),
            "7z" => Ok(Self::SevenZ),
            other => Err(FsError::Pack(format!(
                "unknown archive format '{other}'; expected zip, tar, tar.gz, \
                 tar.bz2, tar.xz, tar.zst, rar or 7z"
            ))),
        }
    }
}

/// Magic-byte detection with the file extension as a fallback tie-breaker.
fn sniff_format(archive: &Path) -> Result<ArchiveFormat, FsError> {
    use std::io::Read;
    let mut f = File::open(archive).map_err(|e| io_err(archive, e))?;
    let mut head = Vec::with_capacity(262);
    f.take(262)
        .read_to_end(&mut head)
        .map_err(|e| io_err(archive, e))?;
    let starts = |m: &[u8]| head.starts_with(m);
    if starts(&[0x50, 0x4B, 0x03, 0x04]) || starts(&[0x50, 0x4B, 0x05, 0x06]) {
        return Ok(ArchiveFormat::Zip);
    }
    if starts(&[0x1F, 0x8B]) {
        return Ok(ArchiveFormat::TarGz);
    }
    if starts(b"BZh") {
        return Ok(ArchiveFormat::TarBz2);
    }
    if starts(&[0xFD, b'7', b'z', b'X', b'Z', 0x00]) {
        return Ok(ArchiveFormat::TarXz);
    }
    if starts(&[0x28, 0xB5, 0x2F, 0xFD]) {
        return Ok(ArchiveFormat::TarZst);
    }
    if starts(b"Rar!\x1A\x07") {
        return Ok(ArchiveFormat::Rar);
    }
    if starts(&[0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C]) {
        return Ok(ArchiveFormat::SevenZ);
    }
    if head.len() >= 262 && &head[257..262] == b"ustar" {
        return Ok(ArchiveFormat::Tar);
    }
    let name = archive
        .file_name()
        .map(|n| n.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    for (suffix, fmt) in [
        (".zip", ArchiveFormat::Zip),
        (".tar.gz", ArchiveFormat::TarGz),
        (".tgz", ArchiveFormat::TarGz),
        (".tar.bz2", ArchiveFormat::TarBz2),
        (".tbz2", ArchiveFormat::TarBz2),
        (".tar.xz", ArchiveFormat::TarXz),
        (".txz", ArchiveFormat::TarXz),
        (".tar.zst", ArchiveFormat::TarZst),
        (".tzst", ArchiveFormat::TarZst),
        (".tar", ArchiveFormat::Tar),
        (".rar", ArchiveFormat::Rar),
        (".7z", ArchiveFormat::SevenZ),
    ] {
        if name.ends_with(suffix) {
            return Ok(fmt);
        }
    }
    Err(FsError::Pack(format!(
        "could not detect archive format of '{}'; pass the format explicitly",
        archive.display()
    )))
}

/// Normalize an archive entry name into a stored path: `/`-separators,
/// no leading `/`, no `.`/empty segments; `..` is rejected outright.
fn entry_stored_path(raw: &str) -> Result<String, FsError> {
    let mut segs = Vec::new();
    for seg in raw.replace('\\', "/").split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                return Err(FsError::Pack(format!(
                    "archive entry '{raw}' contains a '..' component"
                )))
            }
            s => segs.push(s.to_string()),
        }
    }
    if segs.is_empty() {
        return Err(FsError::Pack(format!(
            "archive entry '{raw}' normalizes to an empty path"
        )));
    }
    Ok(segs.join("/"))
}

fn pack_zip_entries(archive: &Path, w: &mut PackWriter) -> Result<(), FsError> {
    use std::io::Read;
    let f = File::open(archive).map_err(|e| io_err(archive, e))?;
    let mut z = zip::ZipArchive::new(f).map_err(|e| {
        FsError::Pack(format!("failed to open zip '{}': {e}", archive.display()))
    })?;
    for i in 0..z.len() {
        let mut entry = z.by_index(i).map_err(|e| {
            FsError::Pack(format!(
                "failed to read zip entry {i} in '{}': {e}",
                archive.display()
            ))
        })?;
        if !entry.is_file() {
            continue;
        }
        let stored = entry_stored_path(entry.name())?;
        let mut data = Vec::with_capacity(entry.size() as usize);
        entry.read_to_end(&mut data).map_err(|e| io_err(archive, e))?;
        w.append(stored, &data)?;
    }
    Ok(())
}

fn pack_tar_entries<R: std::io::Read>(
    reader: R,
    archive: &Path,
    w: &mut PackWriter,
) -> Result<(), FsError> {
    let bad = |e: std::io::Error| {
        FsError::Pack(format!(
            "failed to read tar stream from '{}': {e}; is this a (compressed) tar archive?",
            archive.display()
        ))
    };
    let mut a = tar::Archive::new(reader);
    for entry in a.entries().map_err(bad)? {
        let mut entry = entry.map_err(bad)?;
        if !entry.header().entry_type().is_file() {
            continue; // directories, symlinks, hardlinks, devices
        }
        let raw = entry.path().map_err(bad)?.to_string_lossy().into_owned();
        let stored = entry_stored_path(&raw)?;
        let mut data = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut data).map_err(bad)?;
        w.append(stored, &data)?;
    }
    Ok(())
}

// Replaced with real readers in Task 5.
fn pack_rar_entries(_archive: &Path, _w: &mut PackWriter) -> Result<(), FsError> {
    Err(FsError::Pack("rar support not implemented yet".into()))
}

fn pack_7z_entries(_archive: &Path, _w: &mut PackWriter) -> Result<(), FsError> {
    Err(FsError::Pack("7z support not implemented yet".into()))
}

pub fn pack_archive(
    archive: &Path,
    format: Option<ArchiveFormat>,
    out: &Path,
    opts: &PackOptions,
) -> Result<PackSummary, FsError> {
    let fmt = match format {
        Some(f) => f,
        None => sniff_format(archive)?,
    };
    let mut w = PackWriter::create(out, opts)?;
    let res = (|| {
        let open = || File::open(archive).map_err(|e| io_err(archive, e));
        match fmt {
            ArchiveFormat::Zip => pack_zip_entries(archive, &mut w)?,
            ArchiveFormat::Tar => pack_tar_entries(open()?, archive, &mut w)?,
            ArchiveFormat::TarGz => {
                pack_tar_entries(flate2::read::GzDecoder::new(open()?), archive, &mut w)?
            }
            ArchiveFormat::TarBz2 => {
                pack_tar_entries(bzip2::read::BzDecoder::new(open()?), archive, &mut w)?
            }
            ArchiveFormat::TarXz => {
                pack_tar_entries(liblzma::read::XzDecoder::new(open()?), archive, &mut w)?
            }
            ArchiveFormat::TarZst => pack_tar_entries(
                zstd::stream::read::Decoder::new(open()?).map_err(|e| io_err(archive, e))?,
                archive,
                &mut w,
            )?,
            ArchiveFormat::Rar => pack_rar_entries(archive, &mut w)?,
            ArchiveFormat::SevenZ => pack_7z_entries(archive, &mut w)?,
        }
        w.finish()
    })();
    match res {
        Ok(s) if s.files == 0 => {
            let _ = std::fs::remove_file(out);
            Err(FsError::Pack(format!(
                "archive '{}' contains no files",
                archive.display()
            )))
        }
        Ok(s) => Ok(s),
        Err(e) => {
            let _ = std::fs::remove_file(out);
            Err(e)
        }
    }
}
```

Add detection unit tests to the existing `#[cfg(test)] mod tests` in `pack.rs`:

```rust
    #[test]
    fn entry_paths_normalize_and_reject() {
        assert_eq!(entry_stored_path("/a//b/./c.txt").unwrap(), "a/b/c.txt");
        assert_eq!(entry_stored_path("w\\x.txt").unwrap(), "w/x.txt");
        assert!(entry_stored_path("../up.txt").is_err());
        assert!(entry_stored_path("./").is_err());
    }

    #[test]
    fn format_strings_parse() {
        for s in ["zip", "tar", "tar.gz", "tgz", "tar.bz2", "tar.xz", "tar.zst", "rar", "7z"] {
            assert!(ArchiveFormat::parse(s).is_ok(), "{s}");
        }
    }
```

If a third-party API differs from the calls shown (e.g. `zip::write::SimpleFileOptions` naming across zip versions), check the version resolved in `Cargo.lock` on docs.rs and adapt the call sites — keep the test assertions unchanged.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p parquet-file-fs`
Expected: PASS (the rar/7z stub paths are not exercised yet).

- [ ] **Step 6: Lint, full suite, commit**

Run: `cargo fmt --all && cargo clippy --all-targets -- -D warnings && cargo test --locked`

```bash
git add crates/core Cargo.lock
git commit -m "feat: pack_archive with magic-byte detection for zip and tar family"
```

---

### Task 5: 7z and rar readers

**Files:**
- Modify: `crates/core/Cargo.toml` (`sevenz-rust2`; `unrar` optional behind `rar` feature)
- Modify: `crates/core/src/pack.rs` (real `pack_7z_entries` / `pack_rar_entries`)
- Modify: `crates/py/Cargo.toml` (default feature `rar`)
- Create: `crates/core/tests/fixtures/simple.rar` (checked-in fixture)
- Test: `crates/core/tests/pack_archive_test.rs` (append)

**Interfaces:**
- Consumes: `PackWriter::append`, `entry_stored_path`, `pack_archive` dispatch from Task 4.
- Produces: working `ArchiveFormat::SevenZ` always; `ArchiveFormat::Rar` behind cargo feature `rar` (core: `rar = ["dep:unrar"]`; py crate: `default = ["rar"]`, `rar = ["parquet-file-fs/rar"]`). Without the feature, rar input fails with `"rar support not compiled in; rebuild with the 'rar' feature or install the CLI (cargo install parquet-file-fs-cli)"`.

- [ ] **Step 1: Add dependencies**

```bash
cargo add -p parquet-file-fs sevenz-rust2
cargo add -p parquet-file-fs unrar --optional
```

Then in `crates/core/Cargo.toml` add:

```toml
[features]
rar = ["dep:unrar"]
```

And in `crates/py/Cargo.toml` change the features table to:

```toml
[features]
default = ["rar"]
rar = ["parquet-file-fs/rar"]
extension-module = ["pyo3/extension-module"]
```

- [ ] **Step 2: Create the rar fixture** — `crates/core/tests/fixtures/simple.rar` containing `a.txt` = `alpha` and `sub/b.bin` = `beta` (no trailing newlines):

```bash
command -v rar || brew install rar   # rarlab cask; metalbrew works too
cd "$(mktemp -d)"
printf 'alpha' > a.txt
mkdir sub && printf 'beta' > sub/b.bin
rar a simple.rar a.txt sub/b.bin
cp simple.rar <repo>/crates/core/tests/fixtures/
```

If no `rar` binary can be installed, create `crates/core/tests/fixtures/README.md` with the recipe above, mark the rar round-trip test `#[ignore = "requires fixtures/simple.rar (see fixtures/README.md)"]`, and say so in the task report — do not fake the fixture.

- [ ] **Step 3: Write the failing tests** — append to `crates/core/tests/pack_archive_test.rs`:

```rust
#[test]
fn sevenz_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir_all(src.join("sub")).unwrap();
    std::fs::write(src.join("a.txt"), b"alpha").unwrap();
    std::fs::write(src.join("sub/b.bin"), b"beta").unwrap();
    let ar = tmp.path().join("simple.7z");
    sevenz_rust2::compress_to_path(&src, &ar).unwrap();
    let out = tmp.path().join("out.parquet");
    pack_archive(&ar, None, &out, &PackOptions::default()).unwrap();
    assert_roundtrip(&out);
}

#[cfg(feature = "rar")]
#[test]
fn rar_roundtrip_from_fixture() {
    let ar = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/simple.rar");
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("out.parquet");
    pack_archive(&ar, None, &out, &PackOptions::default()).unwrap();
    assert_roundtrip(&out);
}

#[cfg(not(feature = "rar"))]
#[test]
fn rar_without_feature_gives_clear_error() {
    let tmp = tempfile::tempdir().unwrap();
    let ar = tmp.path().join("x.rar");
    std::fs::write(&ar, b"Rar!\x1a\x07\x01\x00rest").unwrap();
    let out = tmp.path().join("out.parquet");
    let err = pack_archive(&ar, None, &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("rar support not compiled in"), "{err}");
}
```

Also add `sevenz-rust2` usage note: the crate must be resolvable from the test (it is a regular dependency of core, so tests can use it directly).

- [ ] **Step 4: Run tests to verify they fail**

Run: `cargo test -p parquet-file-fs --features rar --test pack_archive_test`
Expected: `sevenz_roundtrip` and `rar_roundtrip_from_fixture` FAIL against the Task 4 stubs ("not implemented yet").

- [ ] **Step 5: Implement the readers** — replace the two stubs in `pack.rs`:

```rust
fn pack_7z_entries(archive: &Path, w: &mut PackWriter) -> Result<(), FsError> {
    use std::io::Read;
    let bad = |e: String| {
        FsError::Pack(format!("failed to read 7z '{}': {e}", archive.display()))
    };
    let mut r = sevenz_rust2::ArchiveReader::open(archive, sevenz_rust2::Password::empty())
        .map_err(|e| bad(e.to_string()))?;
    let mut inner: Result<(), FsError> = Ok(());
    r.for_each_entries(&mut |entry, reader| {
        if entry.is_directory() {
            std::io::copy(reader, &mut std::io::sink())?;
            return Ok(true);
        }
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;
        match entry_stored_path(entry.name()).and_then(|p| w.append(p, &data)) {
            Ok(()) => Ok(true),
            Err(e) => {
                inner = Err(e);
                Ok(false) // stop iterating; surface `inner` below
            }
        }
    })
    .map_err(|e| bad(e.to_string()))?;
    inner
}

#[cfg(feature = "rar")]
fn pack_rar_entries(archive: &Path, w: &mut PackWriter) -> Result<(), FsError> {
    let bad = |e: String| {
        FsError::Pack(format!("failed to read rar '{}': {e}", archive.display()))
    };
    let mut ar = unrar::Archive::new(archive)
        .open_for_processing()
        .map_err(|e| bad(e.to_string()))?;
    while let Some(header) = ar.read_header().map_err(|e| bad(e.to_string()))? {
        let is_file = header.entry().is_file();
        let raw = header.entry().filename.to_string_lossy().into_owned();
        ar = if is_file {
            let (data, next) = header.read().map_err(|e| bad(e.to_string()))?;
            w.append(entry_stored_path(&raw)?, &data)?;
            next
        } else {
            header.skip().map_err(|e| bad(e.to_string()))?
        };
    }
    Ok(())
}

#[cfg(not(feature = "rar"))]
fn pack_rar_entries(_archive: &Path, _w: &mut PackWriter) -> Result<(), FsError> {
    Err(FsError::Pack(
        "rar support not compiled in; rebuild with the 'rar' feature or \
         install the CLI (cargo install parquet-file-fs-cli)"
            .into(),
    ))
}
```

Both crates' exact call signatures (sevenz-rust2's `ArchiveReader`/`for_each_entries`/`Password`, unrar's cursor API `open_for_processing`/`read_header`/`read`/`skip`) must be checked against the versions resolved in `Cargo.lock` on docs.rs; adapt the plumbing to compile, keeping the behavior (skip dirs, normalize names, `..` rejected, entries streamed in archive order) and the test assertions identical.

- [ ] **Step 6: Run tests both ways**

Run: `cargo test -p parquet-file-fs --features rar && cargo test -p parquet-file-fs`
Expected: with `rar` — sevenz + rar round-trips pass; without — `rar_without_feature_gives_clear_error` passes. (In workspace-wide runs, feature unification from the py/cli defaults may enable `rar` for core's tests too; the `-p` invocations above pin both paths.)

- [ ] **Step 7: Lint, full suite, commit**

Run: `cargo fmt --all && cargo clippy --all-targets --features rar -- -D warnings && cargo test --locked`

```bash
git add crates Cargo.lock
git commit -m "feat: 7z and rar archive readers (rar behind a default-on cargo feature)"
```

---

### Task 6: CLI crate (`pfs`)

**Files:**
- Create: `crates/cli/Cargo.toml`, `crates/cli/src/main.rs`
- Modify: root `Cargo.toml` (add `"crates/cli"` to members)
- Test: `crates/cli/tests/cli_test.rs`

**Interfaces:**
- Consumes: `pack::{pack_glob, pack_archive, ArchiveFormat, PackCompression, PackOptions, PackSummary}`, `adapter::FsError` from core.
- Produces: binary `pfs` with subcommands `pack <SOURCE> <OUT>` and `pack-archive <ARCHIVE> <OUT>`, flags `--root`, `--format`, `--path-column`, `--content-column`, `--compression`; success prints `packed N files (M bytes) -> OUT`, failure prints `error: ...` to stderr with exit code 1. Installed via `cargo install parquet-file-fs-cli`.

- [ ] **Step 1: Scaffold the crate** — `crates/cli/Cargo.toml`:

```toml
[package]
name = "parquet-file-fs-cli"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
description = "pfs: create parquet archive shards readable by parquet-file-fs, from globs or archive files"

[[bin]]
name = "pfs"
path = "src/main.rs"

[features]
default = ["rar"]
rar = ["parquet-file-fs/rar"]

[dependencies]
parquet-file-fs = { path = "../core" }
clap = { version = "4", features = ["derive"] }

[dev-dependencies]
tempfile = "3"
zip = { version = "4", default-features = false, features = ["deflate"] }
```

Add `"crates/cli"` to `members` in the root `Cargo.toml`, then run `cargo update --workspace`.

(Match the `zip` major version to what Task 4's `cargo add` resolved — check `Cargo.lock`.)

- [ ] **Step 2: Write the failing integration test** — `crates/cli/tests/cli_test.rs`:

```rust
use std::io::Write;
use std::path::Path;
use std::process::Command;

use parquet_file_fs::archive::Archive;
use parquet_file_fs::index::DupPolicy;

fn pfs() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pfs"))
}

fn open(out: &Path) -> Archive {
    Archive::open(
        &[out.to_str().unwrap().to_string()],
        None,
        None,
        DupPolicy::Error,
    )
    .unwrap()
}

fn tree(dir: &Path) {
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    std::fs::write(dir.join("a.txt"), b"alpha").unwrap();
    std::fs::write(dir.join("sub/b.bin"), b"beta").unwrap();
}

#[test]
fn pack_glob_happy_path() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    tree(&data);
    let out = tmp.path().join("out.parquet");
    let o = pfs()
        .args([
            "pack",
            &format!("{}/**/*", data.display()),
            out.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(o.status.success(), "{o:?}");
    let stdout = String::from_utf8_lossy(&o.stdout);
    assert!(stdout.contains("packed 2 files"), "{stdout}");
    assert_eq!(open(&out).read("a.txt").unwrap(), b"alpha");
}

#[test]
fn pack_directory_shorthand_and_flags() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    tree(&data);
    let out = tmp.path().join("out.parquet");
    let o = pfs()
        .args([
            "pack",
            data.to_str().unwrap(),
            out.to_str().unwrap(),
            "--compression",
            "snappy",
            "--path-column",
            "file_name",
            "--content-column",
            "data",
        ])
        .output()
        .unwrap();
    assert!(o.status.success(), "{o:?}");
    let a = Archive::open(
        &[out.to_str().unwrap().to_string()],
        Some("file_name"),
        Some("data"),
        DupPolicy::Error,
    )
    .unwrap();
    assert_eq!(a.read("sub/b.bin").unwrap(), b"beta");
}

#[test]
fn pack_archive_zip() {
    let tmp = tempfile::tempdir().unwrap();
    let ar = tmp.path().join("bundle.zip");
    let f = std::fs::File::create(&ar).unwrap();
    let mut z = zip::ZipWriter::new(f);
    let o: zip::write::SimpleFileOptions = Default::default();
    z.start_file("inner.txt", o).unwrap();
    z.write_all(b"inner").unwrap();
    z.finish().unwrap();
    let out = tmp.path().join("out.parquet");
    let o = pfs()
        .args(["pack-archive", ar.to_str().unwrap(), out.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(o.status.success(), "{o:?}");
    assert_eq!(open(&out).read("inner.txt").unwrap(), b"inner");
}

#[test]
fn errors_exit_nonzero_with_stderr() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("out.parquet");
    let o = pfs()
        .args([
            "pack",
            &format!("{}/none/**/*", tmp.path().display()),
            out.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("no files matched"));

    let o = pfs()
        .args([
            "pack-archive",
            "/nonexistent.zip",
            out.to_str().unwrap(),
            "--format",
            "sit",
        ])
        .output()
        .unwrap();
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("unknown archive format"));
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p parquet-file-fs-cli`
Expected: compile error — `src/main.rs` missing.

- [ ] **Step 4: Implement `crates/cli/src/main.rs`**

```rust
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use parquet_file_fs::adapter::FsError;
use parquet_file_fs::pack::{
    pack_archive, pack_glob, ArchiveFormat, PackCompression, PackOptions, PackSummary,
};

#[derive(Parser)]
#[command(name = "pfs", version, about = "Create parquet archive shards readable by parquet-file-fs")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Pack files matched by a glob (or under a directory) into a parquet archive.
    ///
    /// Matched archive files (zip, tar, ...) are stored as plain bytes,
    /// never expanded — use `pack-archive` for that.
    Pack {
        /// Glob pattern (quote it!) or directory.
        source: String,
        /// Output parquet file.
        out: PathBuf,
        /// Directory stored paths are made relative to
        /// (default: the pattern's wildcard-free prefix).
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long, default_value = "path")]
        path_column: String,
        #[arg(long, default_value = "content")]
        content_column: String,
        /// zstd, snappy or none.
        #[arg(long, default_value = "zstd")]
        compression: String,
    },
    /// Expand one archive (zip, tar, tar.gz/bz2/xz/zst, rar, 7z) into a parquet archive.
    PackArchive {
        archive: PathBuf,
        out: PathBuf,
        /// zip|tar|tar.gz|tar.bz2|tar.xz|tar.zst|rar|7z (default: detect by magic bytes).
        #[arg(long)]
        format: Option<String>,
        #[arg(long, default_value = "path")]
        path_column: String,
        #[arg(long, default_value = "content")]
        content_column: String,
        /// zstd, snappy or none.
        #[arg(long, default_value = "zstd")]
        compression: String,
    },
}

fn build_opts(
    path_column: String,
    content_column: String,
    compression: &str,
) -> Result<PackOptions, FsError> {
    Ok(PackOptions {
        path_column,
        content_column,
        compression: PackCompression::parse(compression)?,
        ..PackOptions::default()
    })
}

fn run(cmd: Cmd) -> Result<(PackSummary, PathBuf), FsError> {
    match cmd {
        Cmd::Pack {
            source,
            out,
            root,
            path_column,
            content_column,
            compression,
        } => {
            let opts = build_opts(path_column, content_column, &compression)?;
            let s = pack_glob(&source, root.as_deref(), &out, &opts)?;
            Ok((s, out))
        }
        Cmd::PackArchive {
            archive,
            out,
            format,
            path_column,
            content_column,
            compression,
        } => {
            let fmt = format.as_deref().map(ArchiveFormat::parse).transpose()?;
            let opts = build_opts(path_column, content_column, &compression)?;
            let s = pack_archive(&archive, fmt, &out, &opts)?;
            Ok((s, out))
        }
    }
}

fn main() -> ExitCode {
    match run(Cli::parse().cmd) {
        Ok((s, out)) => {
            println!("packed {} files ({} bytes) -> {}", s.files, s.bytes, out.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_consistent() {
        Cli::command().debug_assert();
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p parquet-file-fs-cli`
Expected: all PASS.

- [ ] **Step 6: Lint, full suite, commit**

Run: `cargo fmt --all && cargo clippy --all-targets -- -D warnings && cargo test --locked && uv run pytest -q`
(pytest re-checks `test_release_meta.py`'s lockfile assertion now that a third crate exists.)

```bash
git add crates/cli Cargo.toml Cargo.lock
git commit -m "feat: pfs CLI with pack and pack-archive subcommands"
```

---

### Task 7: Python bindings and wrapper

**Files:**
- Modify: `crates/py/src/lib.rs` (pyo3 `pack_glob`/`pack_files`/`pack_archive` + registration + `Pack` error mapping)
- Create: `python/parquet_file_fs/pack.py`
- Modify: `python/parquet_file_fs/__init__.py`
- Test: `tests/test_pack.py`

**Interfaces:**
- Consumes: core `pack::*` (Tasks 2–5), `to_py` error mapping.
- Produces: `parquet_file_fs.pack(source, out, *, root=None, path_column="path", content_column="content", compression="zstd")` (source: glob str | directory str | list of files) and `parquet_file_fs.pack_archive(archive, out, *, format=None, path_column=..., content_column=..., compression=...)`; both return `{"files": int, "bytes": int, "path": str}`. `FsError::Pack` → `ValueError`.

- [ ] **Step 1: Write the failing tests** — `tests/test_pack.py`:

```python
import tarfile
import zipfile

import pytest

from parquet_file_fs import ParquetFileSystem, pack, pack_archive


def _tree(base):
    d = base / "data"
    (d / "sub").mkdir(parents=True)
    (d / "a.txt").write_bytes(b"alpha")
    (d / "sub" / "b.bin").write_bytes(b"beta")
    return d


def test_pack_glob_roundtrip(tmp_path):
    d = _tree(tmp_path)
    out = tmp_path / "out.parquet"
    info = pack(f"{d}/**/*", out)
    assert info == {"files": 2, "bytes": 9, "path": str(out)}
    fs = ParquetFileSystem(str(out))
    assert fs.cat_file("a.txt") == b"alpha"
    assert fs.cat_file("sub/b.bin") == b"beta"


def test_pack_directory_shorthand_and_explicit_root(tmp_path):
    d = _tree(tmp_path)
    out = tmp_path / "out.parquet"
    pack(str(d), out)
    assert ParquetFileSystem(str(out)).cat_file("sub/b.bin") == b"beta"
    out2 = tmp_path / "out2.parquet"
    pack(f"{d}/**/*", out2, root=str(tmp_path))
    assert ParquetFileSystem(str(out2)).cat_file("data/a.txt") == b"alpha"


def test_pack_list_requires_root(tmp_path):
    d = _tree(tmp_path)
    with pytest.raises(ValueError, match="root is required"):
        pack([str(d / "a.txt")], tmp_path / "out.parquet")


def test_pack_list_roundtrip(tmp_path):
    d = _tree(tmp_path)
    out = tmp_path / "out.parquet"
    info = pack([str(d / "a.txt"), str(d / "sub" / "b.bin")], out, root=str(d))
    assert info["files"] == 2
    assert ParquetFileSystem(str(out)).cat_file("a.txt") == b"alpha"


def test_glob_stores_zip_as_bytes(tmp_path):
    d = tmp_path / "data"
    d.mkdir()
    with zipfile.ZipFile(d / "bundle.zip", "w") as z:
        z.writestr("inner.txt", "inner")
    out = tmp_path / "out.parquet"
    pack(f"{d}/**/*", out)
    fs = ParquetFileSystem(str(out))
    assert fs.ls("", detail=False) == ["bundle.zip"]
    assert fs.cat_file("bundle.zip")[:2] == b"PK"


def test_pack_archive_zip_and_targz(tmp_path):
    src = _tree(tmp_path)
    zpath = tmp_path / "bundle.zip"
    with zipfile.ZipFile(zpath, "w") as z:
        z.write(src / "a.txt", "a.txt")
        z.write(src / "sub" / "b.bin", "sub/b.bin")
    out = tmp_path / "z.parquet"
    info = pack_archive(zpath, out)
    assert info["files"] == 2
    assert ParquetFileSystem(str(out)).cat_file("sub/b.bin") == b"beta"

    tpath = tmp_path / "bundle.tar.gz"
    with tarfile.open(tpath, "w:gz") as t:
        t.add(src / "a.txt", "a.txt")
        t.add(src / "sub" / "b.bin", "sub/b.bin")
    out2 = tmp_path / "t.parquet"
    pack_archive(tpath, out2)
    assert ParquetFileSystem(str(out2)).cat_file("a.txt") == b"alpha"


def test_pack_archive_format_override(tmp_path):
    src = _tree(tmp_path)
    weird = tmp_path / "bundle.bin"
    with zipfile.ZipFile(weird, "w") as z:
        z.write(src / "a.txt", "a.txt")
    out = tmp_path / "out.parquet"
    pack_archive(weird, out, format="zip")
    assert ParquetFileSystem(str(out)).cat_file("a.txt") == b"alpha"
    with pytest.raises(ValueError, match="unknown archive format"):
        pack_archive(weird, out, format="sit")


def test_error_mapping(tmp_path):
    d = _tree(tmp_path)
    with pytest.raises(ValueError, match="duplicate path"):
        pack([str(d / "a.txt"), str(d / "a.txt")], tmp_path / "out.parquet", root=str(d))
    with pytest.raises(ValueError, match="no files matched"):
        pack(f"{tmp_path}/none/**/*", tmp_path / "out.parquet")
    with pytest.raises(OSError):
        pack_archive(tmp_path / "missing.zip", tmp_path / "out.parquet")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pack.py -q`
Expected: ImportError — `pack` not importable from `parquet_file_fs`.

- [ ] **Step 3: Implement the pyo3 functions** — in `crates/py/src/lib.rs`:

Add imports:

```rust
use std::path::{Path, PathBuf};

use parquet_file_fs::pack::{self, ArchiveFormat, PackCompression, PackOptions, PackSummary};
```

Extend `to_py`'s ValueError arm to include the new variant:

```rust
        FsError::Schema(_)
        | FsError::UnknownScheme { .. }
        | FsError::Duplicate { .. }
        | FsError::Pack(_) => PyValueError::new_err(e.to_string()),
```

Add the functions and register them in the `_core` pymodule:

```rust
fn build_opts(
    path_column: &str,
    content_column: &str,
    compression: &str,
) -> PyResult<PackOptions> {
    Ok(PackOptions {
        path_column: path_column.to_string(),
        content_column: content_column.to_string(),
        compression: PackCompression::parse(compression).map_err(to_py)?,
        ..PackOptions::default()
    })
}

fn summary_to_py(py: Python<'_>, s: PackSummary, out: &str) -> PyResult<PyObject> {
    let d = PyDict::new(py);
    d.set_item("files", s.files)?;
    d.set_item("bytes", s.bytes)?;
    d.set_item("path", out)?;
    Ok(d.into_any().unbind())
}

#[pyfunction]
#[pyo3(signature = (pattern, out, root=None, path_column="path", content_column="content", compression="zstd"))]
fn pack_glob(
    py: Python<'_>,
    pattern: &str,
    out: &str,
    root: Option<String>,
    path_column: &str,
    content_column: &str,
    compression: &str,
) -> PyResult<PyObject> {
    let opts = build_opts(path_column, content_column, compression)?;
    let s = py
        .allow_threads(|| {
            pack::pack_glob(pattern, root.as_deref().map(Path::new), Path::new(out), &opts)
        })
        .map_err(to_py)?;
    summary_to_py(py, s, out)
}

#[pyfunction]
#[pyo3(signature = (paths, out, root, path_column="path", content_column="content", compression="zstd"))]
fn pack_files(
    py: Python<'_>,
    paths: Vec<String>,
    out: &str,
    root: &str,
    path_column: &str,
    content_column: &str,
    compression: &str,
) -> PyResult<PyObject> {
    let opts = build_opts(path_column, content_column, compression)?;
    let bufs: Vec<PathBuf> = paths.iter().map(PathBuf::from).collect();
    let s = py
        .allow_threads(|| pack::pack_files(&bufs, Path::new(root), Path::new(out), &opts))
        .map_err(to_py)?;
    summary_to_py(py, s, out)
}

#[pyfunction]
#[pyo3(signature = (archive, out, format=None, path_column="path", content_column="content", compression="zstd"))]
fn pack_archive(
    py: Python<'_>,
    archive: &str,
    out: &str,
    format: Option<String>,
    path_column: &str,
    content_column: &str,
    compression: &str,
) -> PyResult<PyObject> {
    let fmt = format
        .as_deref()
        .map(ArchiveFormat::parse)
        .transpose()
        .map_err(to_py)?;
    let opts = build_opts(path_column, content_column, compression)?;
    let s = py
        .allow_threads(|| pack::pack_archive(Path::new(archive), fmt, Path::new(out), &opts))
        .map_err(to_py)?;
    summary_to_py(py, s, out)
}
```

In the pymodule add:

```rust
    m.add_function(wrap_pyfunction!(pack_glob, m)?)?;
    m.add_function(wrap_pyfunction!(pack_files, m)?)?;
    m.add_function(wrap_pyfunction!(pack_archive, m)?)?;
```

- [ ] **Step 4: Write the Python wrapper** — `python/parquet_file_fs/pack.py`:

```python
from __future__ import annotations

from parquet_file_fs import _core


def pack(source, out, *, root=None, path_column="path",
         content_column="content", compression="zstd"):
    """Pack files into a parquet archive shard.

    `source` is a glob pattern, an existing directory (packs everything
    under it), or a list of file paths (`root` required). Matched archive
    files (zip, tar, ...) are stored as bytes — use `pack_archive` to
    expand one instead.
    """
    if isinstance(source, (list, tuple)):
        if root is None:
            raise ValueError("root is required when source is a list of files")
        return _core.pack_files([str(s) for s in source], str(out), str(root),
                                path_column, content_column, compression)
    return _core.pack_glob(str(source), str(out),
                           None if root is None else str(root),
                           path_column, content_column, compression)


def pack_archive(archive, out, *, format=None, path_column="path",
                 content_column="content", compression="zstd"):
    """Expand one archive (zip, tar, tar.gz/bz2/xz/zst, rar, 7z) into a
    parquet archive shard. `format` overrides magic-byte detection."""
    return _core.pack_archive(str(archive), str(out), format,
                              path_column, content_column, compression)
```

Update `python/parquet_file_fs/__init__.py`:

```python
from fsspec import register_implementation

from parquet_file_fs._core import __version__
from parquet_file_fs.adapters import register_adapter
from parquet_file_fs.fs import ParquetFileSystem
from parquet_file_fs.pack import pack, pack_archive

register_implementation("pfs", ParquetFileSystem, clobber=True)

__all__ = [
    "ParquetFileSystem",
    "register_adapter",
    "pack",
    "pack_archive",
    "__version__",
]
```

(Import order note: `pack.py` does `from parquet_file_fs import _core`, which is safe during package init because `_core` is a submodule import, not an attribute of the partially-initialized package.)

- [ ] **Step 5: Rebuild and run the tests**

Run: `uv run maturin develop && uv run pytest -q`
Expected: all tests pass, including the new `tests/test_pack.py`.

- [ ] **Step 6: Lint, full suite, commit**

Run: `cargo fmt --all && cargo clippy --all-targets -- -D warnings && cargo test --locked`

```bash
git add crates/py python tests/test_pack.py Cargo.lock
git commit -m "feat: python pack and pack_archive API"
```

---

### Task 8: Documentation and end-to-end verification

**Files:**
- Modify: `README.md` (new "Creating archives" section; Development section)
- Verify: whole pipeline.

**Interfaces:**
- Consumes: everything above.
- Produces: user-facing docs; a verified green tree.

- [ ] **Step 1: Add the README section** — insert after the "Protocol adapters" section:

```markdown
## Creating archives

The write side lives in the Rust core, exposed twice.

Python:

```python
from parquet_file_fs import pack, pack_archive

pack("data/images/**/*.png", "out.parquet", root="data")  # glob
pack("data/", "out.parquet")                              # whole directory
pack(["a.txt", "b.txt"], "out.parquet", root=".")         # explicit list
pack_archive("bundle.tar.gz", "out.parquet")              # expand an archive
pack_archive("weird.bin", "out.parquet", format="zip")    # detection override
```

CLI (`cargo install parquet-file-fs-cli`):

```bash
pfs pack 'data/images/**/*.png' out.parquet --root data
pfs pack-archive bundle.zip out.parquet
```

Stored paths are relative to `--root`/`root=` (default: the glob's
wildcard-free prefix, or the directory itself). A glob **never** expands
archives it matches — a matched `.zip` is stored as bytes; expanding is
always the explicit `pack-archive` call. Supported archive formats: zip,
tar, tar.gz, tar.bz2, tar.xz, tar.zst, 7z, and rar (rar via the `rar`
cargo feature, on by default). Output is zstd-compressed parquet
(`--compression snappy|none` to change), one row group per ~32 MiB so
readers stay lazy.
```

Also update the Development section's layout note: mention the workspace
(`crates/core`, `crates/py`, `crates/cli`) and that `uv run maturin develop`
builds `crates/py` per `[tool.maturin] manifest-path`.

- [ ] **Step 2: Full verification sweep**

Run each; all must succeed:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --locked
cargo test -p parquet-file-fs            # rar feature OFF path
cargo doc --no-deps
uv run maturin develop && uv run pytest -q
uv run maturin sdist --out /tmp/pfs-sdist-check   # workspace sdist sanity
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: creating archives with pfs pack / pack_archive"
```
