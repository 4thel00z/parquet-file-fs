# parquet-file-fs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A maturin/PyO3 Python package whose Rust core reads parquet "archive" shards (path + content columns) and exposes them as a read-only fsspec filesystem with a pluggable protocol-adapter registry.

**Architecture:** Rust core (`parquet_file_fs._core`) does all parquet decoding through a `RangeReader` trait with a native `object_store` adapter (file/s3/http) and a `PyAdapter` bridge for Python-registered adapters. Index is built from footers + path column only; content is decoded per row group on demand behind a small LRU; sizes/extra columns decode lazily. A thin Python layer subclasses `fsspec.AbstractFileSystem`.

**Tech Stack:** Rust (parquet, arrow-array, object_store, tokio, pyo3 abi3-py39), maturin mixed layout, Python ≥3.9, fsspec; pytest + pyarrow for tests.

**Spec:** `docs/superpowers/specs/2026-07-24-parquet-file-fs-design.md`

## Global Constraints

- Python ≥3.9; wheels are abi3 (`pyo3` feature `abi3-py39`).
- Only hard Python runtime dependency: `fsspec>=2024.2.0`. Test deps: `pytest`, `pyarrow`.
- Rust edition 2021. `parquet` and `arrow-*` crate major versions MUST match exactly.
- Extension module name: `parquet_file_fs._core`; Python source lives in `python/`.
- `cargo test` must build WITHOUT `pyo3/extension-module` (it's an optional cargo feature enabled only by maturin).
- Read-only filesystem: every mutation method raises `NotImplementedError("read-only filesystem")`.
- Adapter contract: `read_range(url, offset, length)` returns exactly `length` bytes; callers never request past EOF.
- Error messages must match the spec: duplicate paths name both shards and the `on_duplicate` options; unknown scheme names the scheme and shows `register_adapter(...)`; schema errors list the actual columns.
- Dev venv at `.venv/`; run Python tools as `.venv/bin/pytest`, `.venv/bin/maturin`.

---

### Task 1: Scaffold maturin mixed project

**Files:**
- Create: `pyproject.toml`
- Create: `Cargo.toml`
- Create: `.gitignore`
- Create: `src/lib.rs`
- Create: `python/parquet_file_fs/__init__.py`
- Test: `tests/test_smoke.py`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: importable `parquet_file_fs` package with compiled `parquet_file_fs._core` exposing `__version__: str`; `.venv/` with maturin, pytest, pyarrow, fsspec installed; cargo workspace where later tasks add modules `adapter`, `chunk_reader`, `index`, `archive`, `native`, `py` under lib name `parquet_file_fs`.

- [ ] **Step 1: Write the failing smoke test**

`tests/test_smoke.py`:
```python
def test_import():
    from parquet_file_fs import __version__

    assert __version__
```

- [ ] **Step 2: Create the venv and run the test to verify it fails**

```bash
python3 -m venv .venv
.venv/bin/pip install maturin pytest pyarrow fsspec
.venv/bin/pytest tests/test_smoke.py -q
```
Expected: FAIL — `ModuleNotFoundError: No module named 'parquet_file_fs'`

- [ ] **Step 3: Create the project files**

`pyproject.toml`:
```toml
[build-system]
requires = ["maturin>=1.7,<2"]
build-backend = "maturin"

[project]
name = "parquet-file-fs"
version = "0.1.0"
description = "Read-only fsspec filesystem over parquet archives (path + content columns)"
readme = "README.md"
requires-python = ">=3.9"
license = { text = "MIT" }
dependencies = ["fsspec>=2024.2.0"]

[project.optional-dependencies]
test = ["pytest", "pyarrow"]

[project.entry-points."fsspec.specs"]
pfs = "parquet_file_fs.ParquetFileSystem"

[tool.maturin]
python-source = "python"
module-name = "parquet_file_fs._core"
features = ["extension-module"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

`Cargo.toml` (versions are known-good minimums; if `cargo build` reports resolution conflicts, bump `parquet`/`arrow-*` together — their majors must match):
```toml
[package]
name = "parquet-file-fs"
version = "0.1.0"
edition = "2021"

[lib]
name = "parquet_file_fs"
crate-type = ["cdylib", "rlib"]

[features]
default = []
extension-module = ["pyo3/extension-module"]

[dependencies]
pyo3 = { version = "0.25", features = ["abi3-py39"] }
parquet = { version = "55", features = ["arrow"] }
arrow-array = "55"
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

`.gitignore`:
```
/target
/.venv
__pycache__/
*.so
/dist
Cargo.lock
```

`src/lib.rs`:
```rust
use pyo3::prelude::*;

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
```

`python/parquet_file_fs/__init__.py`:
```python
from parquet_file_fs._core import __version__

__all__ = ["__version__"]
```

Also create an empty `README.md` (filled in Task 10):
```markdown
# parquet-file-fs
```

- [ ] **Step 4: Build and run the test to verify it passes**

```bash
cargo check
.venv/bin/maturin develop
.venv/bin/pytest tests/test_smoke.py -q
```
Expected: `cargo check` OK; maturin installs `parquet-file-fs`; pytest `1 passed`.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml Cargo.toml .gitignore src/lib.rs python tests README.md
git commit -m "feat: scaffold maturin mixed project"
```

---

### Task 2: Rust adapter layer — errors, RangeReader trait, LocalAdapter, registry

**Files:**
- Create: `src/adapter.rs`
- Modify: `src/lib.rs` (add `pub mod adapter;`)
- Test: inline `#[cfg(test)]` in `src/adapter.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `enum FsError` (variants used everywhere: `Adapter { url, msg }`, `UnknownScheme { scheme, url }`, `NotFound(String)`, `Duplicate { path, shard_a, shard_b }`, `Schema(String)`, `Parquet { url, source }`, `Io { url, source }`).
  - `trait RangeReader: Send + Sync { fn size(&self, url: &str) -> Result<u64, FsError>; fn read_range(&self, url: &str, offset: u64, length: u64) -> Result<bytes::Bytes, FsError>; fn list(&self, pattern: &str) -> Result<Vec<String>, FsError>; }`
  - `pub fn register(scheme: &str, adapter: Arc<dyn RangeReader>)`
  - `pub fn resolve(url: &str) -> Result<Arc<dyn RangeReader>, FsError>` (registry first, then built-ins; `s3`/`http`/`https` return `UnknownScheme` until Task 6 rewires them).
  - `pub fn scheme_of(url: &str) -> &str` (`"file"` when no `://`).
  - `pub struct LocalAdapter;`

- [ ] **Step 1: Write the failing tests**

Append to (not-yet-existing) `src/adapter.rs` — create the file with ONLY the test module first:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn local_size_and_read_range() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::File::create(&p)
            .unwrap()
            .write_all(b"hello world")
            .unwrap();
        let a = LocalAdapter;
        let url = p.to_str().unwrap();
        assert_eq!(a.size(url).unwrap(), 11);
        assert_eq!(&a.read_range(url, 6, 5).unwrap()[..], b"world");
        // file:// prefix works too
        let furl = format!("file://{url}");
        assert_eq!(a.size(&furl).unwrap(), 11);
    }

    #[test]
    fn local_list_glob() {
        let dir = tempfile::tempdir().unwrap();
        for n in ["a.parquet", "b.parquet", "c.txt"] {
            std::fs::File::create(dir.path().join(n)).unwrap();
        }
        let a = LocalAdapter;
        let pat = format!("{}/*.parquet", dir.path().to_str().unwrap());
        let got = a.list(&pat).unwrap();
        assert_eq!(got.len(), 2);
        assert!(got[0].ends_with("a.parquet") && got[1].ends_with("b.parquet"));
        // non-glob returns itself
        assert_eq!(a.list("/tmp/x.parquet").unwrap(), vec!["/tmp/x.parquet"]);
    }

    #[test]
    fn scheme_parsing_and_resolution() {
        assert_eq!(scheme_of("/tmp/a"), "file");
        assert_eq!(scheme_of("s3://b/k"), "s3");
        assert!(resolve("/tmp/a").is_ok());
        let err = resolve("weird://x").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("weird") && msg.contains("register_adapter"));
    }

    #[test]
    fn registry_overrides() {
        struct Fake;
        impl RangeReader for Fake {
            fn size(&self, _: &str) -> Result<u64, FsError> {
                Ok(42)
            }
            fn read_range(&self, _: &str, _: u64, _: u64) -> Result<bytes::Bytes, FsError> {
                Ok(bytes::Bytes::new())
            }
            fn list(&self, p: &str) -> Result<Vec<String>, FsError> {
                Ok(vec![p.to_string()])
            }
        }
        register("fake", std::sync::Arc::new(Fake));
        assert_eq!(resolve("fake://x").unwrap().size("fake://x").unwrap(), 42);
    }
}
```
Add `pub mod adapter;` to the top of `src/lib.rs`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test adapter`
Expected: compilation errors — `FsError`, `RangeReader`, `LocalAdapter`, `resolve` not found.

- [ ] **Step 3: Write the implementation**

Prepend to `src/adapter.rs` (above the test module):
```rust
use bytes::Bytes;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(thiserror::Error, Debug)]
pub enum FsError {
    #[error("adapter error for {url}: {msg}")]
    Adapter { url: String, msg: String },
    #[error("unknown URL scheme '{scheme}' for {url}; register one with parquet_file_fs.register_adapter('{scheme}', adapter)")]
    UnknownScheme { scheme: String, url: String },
    #[error("path not found: {0}")]
    NotFound(String),
    #[error("duplicate path '{path}' in {shard_a} and {shard_b}; pass on_duplicate='first' or 'last' to resolve")]
    Duplicate {
        path: String,
        shard_a: String,
        shard_b: String,
    },
    #[error("{0}")]
    Schema(String),
    #[error("parquet error for {url}: {source}")]
    Parquet {
        url: String,
        source: parquet::errors::ParquetError,
    },
    #[error("io error for {url}: {source}")]
    Io {
        url: String,
        source: std::io::Error,
    },
}

pub trait RangeReader: Send + Sync {
    fn size(&self, url: &str) -> Result<u64, FsError>;
    fn read_range(&self, url: &str, offset: u64, length: u64) -> Result<Bytes, FsError>;
    /// Expand a glob pattern into concrete URLs. Non-glob inputs return themselves.
    fn list(&self, pattern: &str) -> Result<Vec<String>, FsError>;
}

pub struct LocalAdapter;

fn strip_file_scheme(url: &str) -> &str {
    url.strip_prefix("file://").unwrap_or(url)
}

impl RangeReader for LocalAdapter {
    fn size(&self, url: &str) -> Result<u64, FsError> {
        std::fs::metadata(strip_file_scheme(url))
            .map(|m| m.len())
            .map_err(|e| FsError::Io { url: url.into(), source: e })
    }

    fn read_range(&self, url: &str, offset: u64, length: u64) -> Result<Bytes, FsError> {
        use std::io::{Read, Seek, SeekFrom};
        let wrap = |e: std::io::Error| FsError::Io { url: url.into(), source: e };
        let mut f = std::fs::File::open(strip_file_scheme(url)).map_err(wrap)?;
        f.seek(SeekFrom::Start(offset)).map_err(wrap)?;
        let mut buf = vec![0u8; length as usize];
        f.read_exact(&mut buf).map_err(wrap)?;
        Ok(buf.into())
    }

    fn list(&self, pattern: &str) -> Result<Vec<String>, FsError> {
        let p = strip_file_scheme(pattern);
        if !p.contains(['*', '?', '[']) {
            return Ok(vec![p.to_string()]);
        }
        let entries = glob::glob(p).map_err(|e| FsError::Adapter {
            url: pattern.into(),
            msg: e.to_string(),
        })?;
        let mut out: Vec<String> = entries
            .filter_map(|e| e.ok())
            .map(|pb| pb.to_string_lossy().into_owned())
            .collect();
        out.sort();
        Ok(out)
    }
}

static REGISTRY: Lazy<RwLock<HashMap<String, Arc<dyn RangeReader>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub fn register(scheme: &str, adapter: Arc<dyn RangeReader>) {
    REGISTRY.write().unwrap().insert(scheme.to_string(), adapter);
}

pub fn scheme_of(url: &str) -> &str {
    url.split_once("://").map(|(s, _)| s).unwrap_or("file")
}

pub fn resolve(url: &str) -> Result<Arc<dyn RangeReader>, FsError> {
    let scheme = scheme_of(url);
    if let Some(a) = REGISTRY.read().unwrap().get(scheme) {
        return Ok(a.clone());
    }
    match scheme {
        "file" => Ok(Arc::new(LocalAdapter)),
        // Task 6 rewires s3/http/https to NativeAdapter.
        _ => Err(FsError::UnknownScheme {
            scheme: scheme.into(),
            url: url.into(),
        }),
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test adapter`
Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/adapter.rs src/lib.rs
git commit -m "feat: RangeReader trait, LocalAdapter, adapter registry"
```

---

### Task 3: Rust AdapterChunkReader (parquet I/O over RangeReader)

**Files:**
- Create: `src/chunk_reader.rs`
- Modify: `src/lib.rs` (add `pub mod chunk_reader;`)
- Test: inline `#[cfg(test)]` in `src/chunk_reader.rs`

**Interfaces:**
- Consumes: `crate::adapter::{RangeReader, FsError}` (Task 2).
- Produces: `#[derive(Clone)] pub struct AdapterChunkReader { pub adapter: Arc<dyn RangeReader>, pub url: Arc<String>, pub size: u64 }` implementing `parquet::file::reader::{ChunkReader, Length}` — the type every parquet read in Tasks 4/5 goes through.

- [ ] **Step 1: Write the failing tests**

Create `src/chunk_reader.rs` with only:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::LocalAdapter;
    use parquet::file::reader::{ChunkReader, Length};
    use std::io::{Read, Write};
    use std::sync::Arc;

    fn fixture() -> (tempfile::TempDir, AdapterChunkReader) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f.bin");
        std::fs::File::create(&p)
            .unwrap()
            .write_all(b"0123456789")
            .unwrap();
        let url = p.to_str().unwrap().to_string();
        let r = AdapterChunkReader {
            adapter: Arc::new(LocalAdapter),
            url: Arc::new(url),
            size: 10,
        };
        (dir, r)
    }

    #[test]
    fn get_bytes_reads_exact_range() {
        let (_d, r) = fixture();
        assert_eq!(r.len(), 10);
        assert_eq!(&r.get_bytes(2, 3).unwrap()[..], b"234");
    }

    #[test]
    fn get_read_streams_from_offset_and_stops_at_eof() {
        let (_d, r) = fixture();
        let mut buf = Vec::new();
        r.get_read(7).unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"789");
    }
}
```
Add `pub mod chunk_reader;` to `src/lib.rs`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test chunk_reader`
Expected: compilation error — `AdapterChunkReader` not found.

- [ ] **Step 3: Write the implementation**

Prepend to `src/chunk_reader.rs`:
```rust
use bytes::Bytes;
use parquet::errors::ParquetError;
use parquet::file::reader::{ChunkReader, Length};
use std::sync::Arc;

use crate::adapter::RangeReader;

#[derive(Clone)]
pub struct AdapterChunkReader {
    pub adapter: Arc<dyn RangeReader>,
    pub url: Arc<String>,
    pub size: u64,
}

impl Length for AdapterChunkReader {
    fn len(&self) -> u64 {
        self.size
    }
}

pub struct AdapterRead {
    inner: AdapterChunkReader,
    pos: u64,
}

impl std::io::Read for AdapterRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.inner.size.saturating_sub(self.pos);
        let want = (buf.len() as u64).min(remaining);
        if want == 0 {
            return Ok(0);
        }
        let data = self
            .inner
            .adapter
            .read_range(&self.inner.url, self.pos, want)
            .map_err(std::io::Error::other)?;
        let n = data.len().min(buf.len());
        buf[..n].copy_from_slice(&data[..n]);
        self.pos += n as u64;
        Ok(n)
    }
}

impl ChunkReader for AdapterChunkReader {
    type T = AdapterRead;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(AdapterRead {
            inner: self.clone(),
            pos: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let data = self
            .adapter
            .read_range(&self.url, start, length as u64)
            .map_err(|e| ParquetError::External(Box::new(e)))?;
        if data.len() != length {
            return Err(ParquetError::General(format!(
                "adapter returned {} bytes for {}, expected {}",
                data.len(),
                self.url,
                length
            )));
        }
        Ok(data)
    }
}
```
Note: `std::io::Error::other` requires Rust ≥1.74; if unavailable use `std::io::Error::new(std::io::ErrorKind::Other, e.to_string())`. If the installed `parquet` version's `ChunkReader::get_bytes` signature differs (older versions take `length: usize`, newer may change), follow the compiler.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test chunk_reader`
Expected: 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/chunk_reader.rs src/lib.rs
git commit -m "feat: parquet ChunkReader over RangeReader adapters"
```

---

### Task 4: Rust index — column detection, path index, directory tree, duplicate policies

**Files:**
- Create: `src/index.rs`
- Create: `tests/common/mod.rs`
- Modify: `src/lib.rs` (add `pub mod index;`)
- Test: `tests/index_test.rs`

**Interfaces:**
- Consumes: `crate::adapter::{resolve, FsError}` (Task 2), `crate::chunk_reader::AdapterChunkReader` (Task 3).
- Produces (all `pub` in `crate::index`):
  - `fn normalize(path: &str) -> String` — strips leading/trailing `/`.
  - `fn locate(offsets: &[usize], global: usize) -> (usize, usize)` — global row → (row_group, row_in_group).
  - `enum DupPolicy { Error, First, Last }` with `fn parse(s: &str) -> Result<Self, FsError>` accepting `"error" | "first" | "last"`.
  - `enum MetaValue { Null, Bool(bool), Int(i64), Float(f64), Str(String), Bytes(Vec<u8>) }` (Clone, Debug).
  - `struct RowLoc { pub shard: usize, pub row_group: usize, pub row: usize }` (Clone, Copy, Debug).
  - `struct FileEntry { pub loc: RowLoc }`.
  - `struct Shard { pub url: String, pub reader: AdapterChunkReader, pub content_col: usize, pub extra_cols: Vec<usize>, pub extra_names: Vec<String>, pub row_group_offsets: Vec<usize> }`.
  - `struct DirEntry { pub name: String, pub is_dir: bool }` (Clone, Debug).
  - `struct Index { pub shards: Vec<Shard>, pub files: BTreeMap<String, FileEntry>, pub dirs: BTreeSet<String> }` with methods `ls(&self, path: &str) -> Result<Vec<DirEntry>, FsError>` and `is_dir(&self, norm: &str) -> bool`.
  - `fn build_index(sources: &[String], path_column: Option<&str>, content_column: Option<&str>, on_duplicate: DupPolicy) -> Result<Index, FsError>`.
- Test helper produced for later tasks: `tests/common/mod.rs::write_shard(path, rows: &[(&str, &[u8])], rows_per_group: usize)` and `write_shard_custom(path, path_col: &str, content_col: &str, rows: &[(&str, &[u8])], extra_num: Option<&[i64]>, rows_per_group: usize)`.

- [ ] **Step 1: Write the fixture helper and failing tests**

`tests/common/mod.rs`:
```rust
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

pub fn write_shard_custom(
    path: &std::path::Path,
    path_col: &str,
    content_col: &str,
    rows: &[(&str, &[u8])],
    extra_num: Option<&[i64]>,
    rows_per_group: usize,
) {
    let paths = StringArray::from_iter_values(rows.iter().map(|(p, _)| *p));
    let contents = BinaryArray::from_iter_values(rows.iter().map(|(_, c)| *c));
    let mut cols: Vec<(&str, ArrayRef)> = vec![
        (path_col, Arc::new(paths) as ArrayRef),
        (content_col, Arc::new(contents) as ArrayRef),
    ];
    if let Some(nums) = extra_num {
        cols.push(("num", Arc::new(Int64Array::from(nums.to_vec())) as ArrayRef));
    }
    let batch = RecordBatch::try_from_iter(cols).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(rows_per_group)
        .build();
    let file = std::fs::File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

pub fn write_shard(path: &std::path::Path, rows: &[(&str, &[u8])], rows_per_group: usize) {
    write_shard_custom(path, "path", "content", rows, None, rows_per_group);
}
```

`tests/index_test.rs`:
```rust
mod common;

use common::{write_shard, write_shard_custom};
use parquet_file_fs::index::{build_index, locate, normalize, DupPolicy};

const ROWS: &[(&str, &[u8])] = &[
    ("images/a.png", b"PNG-A"),
    ("images/b.png", b"PNG-B"),
    ("labels/a.json", b"{}"),
    ("readme.txt", b"hi"),
    ("images/sub/c.png", b"PNG-C"),
];

#[test]
fn normalize_strips_slashes() {
    assert_eq!(normalize("/a/b/"), "a/b");
    assert_eq!(normalize(""), "");
    assert_eq!(normalize("/"), "");
}

#[test]
fn locate_maps_global_rows() {
    let offsets = vec![0, 2, 4];
    assert_eq!(locate(&offsets, 0), (0, 0));
    assert_eq!(locate(&offsets, 1), (0, 1));
    assert_eq!(locate(&offsets, 2), (1, 0));
    assert_eq!(locate(&offsets, 4), (2, 0));
}

#[test]
fn builds_index_with_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s.parquet");
    write_shard(&p, ROWS, 2);
    let idx = build_index(
        &[p.to_str().unwrap().to_string()],
        None,
        None,
        DupPolicy::Error,
    )
    .unwrap();
    assert_eq!(idx.files.len(), 5);
    assert_eq!(idx.shards.len(), 1);
    let e = &idx.files["images/sub/c.png"];
    assert_eq!((e.loc.row_group, e.loc.row), (2, 0)); // row 4 with rows_per_group=2
    assert!(idx.dirs.contains("images") && idx.dirs.contains("images/sub"));
}

#[test]
fn ls_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s.parquet");
    write_shard(&p, ROWS, 100);
    let idx = build_index(&[p.to_str().unwrap().to_string()], None, None, DupPolicy::Error).unwrap();

    let root: Vec<String> = idx.ls("").unwrap().into_iter().map(|e| e.name).collect();
    assert!(root.contains(&"images".to_string()) && root.contains(&"readme.txt".to_string()));

    let images = idx.ls("/images/").unwrap();
    let names: Vec<&str> = images.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, vec!["images/a.png", "images/b.png", "images/sub"]);
    assert!(images.iter().find(|e| e.name == "images/sub").unwrap().is_dir);

    // ls of a file returns itself
    let f = idx.ls("readme.txt").unwrap();
    assert_eq!(f.len(), 1);
    assert!(!f[0].is_dir);

    assert!(idx.ls("nope").is_err());
    assert!(idx.is_dir("images") && !idx.is_dir("readme.txt"));
}

#[test]
fn duplicate_policies() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.parquet");
    let b = dir.path().join("b.parquet");
    write_shard(&a, &[("x.txt", b"from-a")], 100);
    write_shard(&b, &[("x.txt", b"from-b")], 100);
    let sources = vec![
        a.to_str().unwrap().to_string(),
        b.to_str().unwrap().to_string(),
    ];

    let err = build_index(&sources, None, None, DupPolicy::Error).unwrap_err();
    assert!(err.to_string().contains("duplicate path 'x.txt'"));

    let first = build_index(&sources, None, None, DupPolicy::First).unwrap();
    assert_eq!(first.files["x.txt"].loc.shard, 0);
    let last = build_index(&sources, None, None, DupPolicy::Last).unwrap();
    assert_eq!(last.files["x.txt"].loc.shard, 1);
}

#[test]
fn column_detection_and_override() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("odd.parquet");
    write_shard_custom(&p, "file_name", "image_bytes", &[("a", b"1")], None, 100);
    let src = vec![p.to_str().unwrap().to_string()];

    // auto-detect finds file_name but not image_bytes
    let err = build_index(&src, None, None, DupPolicy::Error).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("content") && msg.contains("image_bytes"));

    let idx = build_index(&src, None, Some("image_bytes"), DupPolicy::Error).unwrap();
    assert_eq!(idx.files.len(), 1);

    // explicit missing column names available ones
    let err = build_index(&src, Some("nope"), Some("image_bytes"), DupPolicy::Error).unwrap_err();
    assert!(err.to_string().contains("file_name"));
}

#[test]
fn multi_shard_glob_and_empty_match() {
    let dir = tempfile::tempdir().unwrap();
    write_shard(&dir.path().join("s1.parquet"), &[("a.txt", b"1")], 100);
    write_shard(&dir.path().join("s2.parquet"), &[("b.txt", b"2")], 100);
    let pat = format!("{}/*.parquet", dir.path().to_str().unwrap());
    let idx = build_index(&[pat], None, None, DupPolicy::Error).unwrap();
    assert_eq!(idx.files.len(), 2);
    assert_eq!(idx.shards.len(), 2);

    let none = format!("{}/zzz-*.parquet", dir.path().to_str().unwrap());
    assert!(build_index(&[none], None, None, DupPolicy::Error).is_err());
}
```
Add `pub mod index;` to `src/lib.rs`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --test index_test`
Expected: compilation error — module `index` has no items.

- [ ] **Step 3: Write the implementation**

`src/index.rs`:
```rust
use std::collections::btree_map::Entry as MapEntry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, LargeStringArray, StringArray};
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use crate::adapter::{resolve, FsError};
use crate::chunk_reader::AdapterChunkReader;

pub fn normalize(path: &str) -> String {
    path.trim_start_matches('/').trim_end_matches('/').to_string()
}

/// Map a global row index to (row_group, row_within_group) given cumulative offsets.
pub fn locate(offsets: &[usize], global: usize) -> (usize, usize) {
    let rg = offsets.partition_point(|&o| o <= global) - 1;
    (rg, global - offsets[rg])
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DupPolicy {
    Error,
    First,
    Last,
}

impl DupPolicy {
    pub fn parse(s: &str) -> Result<Self, FsError> {
        match s {
            "error" => Ok(Self::Error),
            "first" => Ok(Self::First),
            "last" => Ok(Self::Last),
            other => Err(FsError::Schema(format!(
                "on_duplicate must be 'error', 'first' or 'last', got '{other}'"
            ))),
        }
    }
}

#[derive(Clone, Debug)]
pub enum MetaValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
}

#[derive(Clone, Copy, Debug)]
pub struct RowLoc {
    pub shard: usize,
    pub row_group: usize,
    pub row: usize,
}

#[derive(Debug)]
pub struct FileEntry {
    pub loc: RowLoc,
}

pub struct Shard {
    pub url: String,
    pub reader: AdapterChunkReader,
    pub content_col: usize,
    pub extra_cols: Vec<usize>,
    pub extra_names: Vec<String>,
    pub row_group_offsets: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

pub struct Index {
    pub shards: Vec<Shard>,
    pub files: BTreeMap<String, FileEntry>,
    pub dirs: BTreeSet<String>,
}

impl Index {
    pub fn is_dir(&self, norm: &str) -> bool {
        norm.is_empty() || self.dirs.contains(norm)
    }

    pub fn ls(&self, path: &str) -> Result<Vec<DirEntry>, FsError> {
        let prefix = normalize(path);
        if !prefix.is_empty() && self.files.contains_key(&prefix) {
            return Ok(vec![DirEntry { name: prefix, is_dir: false }]);
        }
        if !self.is_dir(&prefix) {
            return Err(FsError::NotFound(prefix));
        }
        let prefix_slash = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        let mut out = Vec::new();
        let mut seen_dirs = BTreeSet::new();
        for (k, _) in self.files.range(prefix_slash.clone()..) {
            if !k.starts_with(&prefix_slash) {
                break;
            }
            let rest = &k[prefix_slash.len()..];
            match rest.split_once('/') {
                None => out.push(DirEntry { name: k.clone(), is_dir: false }),
                Some((d, _)) => {
                    if seen_dirs.insert(d.to_string()) {
                        out.push(DirEntry {
                            name: format!("{prefix_slash}{d}"),
                            is_dir: true,
                        });
                    }
                }
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }
}

const PATH_NAMES: &[&str] = &["path", "filename", "file_name", "key"];
const CONTENT_NAMES: &[&str] = &["content", "data", "bytes"];

fn detect_column(
    fields: &[String],
    candidates: &[&str],
    explicit: Option<&str>,
    kind: &str,
    url: &str,
) -> Result<usize, FsError> {
    if let Some(name) = explicit {
        return fields.iter().position(|f| f == name).ok_or_else(|| {
            FsError::Schema(format!(
                "column '{name}' not found in {url}; available columns: {fields:?}"
            ))
        });
    }
    candidates
        .iter()
        .find_map(|c| fields.iter().position(|f| f.eq_ignore_ascii_case(c)))
        .ok_or_else(|| {
            FsError::Schema(format!(
                "could not detect {kind} column in {url}; available columns: {fields:?}; \
                 pass path_column=/content_column= explicitly"
            ))
        })
}

fn path_strings(col: &ArrayRef, url: &str) -> Result<Vec<String>, FsError> {
    let bad_null = || FsError::Schema(format!("path column contains a null value in {url}"));
    match col.data_type() {
        DataType::Utf8 => {
            let a = col.as_any().downcast_ref::<StringArray>().unwrap();
            (0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        Err(bad_null())
                    } else {
                        Ok(a.value(i).to_string())
                    }
                })
                .collect()
        }
        DataType::LargeUtf8 => {
            let a = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            (0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        Err(bad_null())
                    } else {
                        Ok(a.value(i).to_string())
                    }
                })
                .collect()
        }
        DataType::Utf8View => {
            let a = col
                .as_any()
                .downcast_ref::<arrow_array::StringViewArray>()
                .unwrap();
            (0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        Err(bad_null())
                    } else {
                        Ok(a.value(i).to_string())
                    }
                })
                .collect()
        }
        dt => Err(FsError::Schema(format!(
            "path column in {url} must be a string type, got {dt}"
        ))),
    }
}

pub fn build_index(
    sources: &[String],
    path_column: Option<&str>,
    content_column: Option<&str>,
    on_duplicate: DupPolicy,
) -> Result<Index, FsError> {
    let mut urls = Vec::new();
    for s in sources {
        let adapter = resolve(s)?;
        let expanded = adapter.list(s)?;
        if expanded.is_empty() {
            return Err(FsError::Schema(format!("no shards matched '{s}'")));
        }
        urls.extend(expanded);
    }

    let mut index = Index {
        shards: Vec::new(),
        files: BTreeMap::new(),
        dirs: BTreeSet::new(),
    };

    for url in urls {
        let adapter = resolve(&url)?;
        let size = adapter.size(&url)?;
        let reader = AdapterChunkReader {
            adapter: adapter.clone(),
            url: Arc::new(url.clone()),
            size,
        };
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader.clone())
            .map_err(|e| FsError::Parquet { url: url.clone(), source: e })?;
        let md = builder.metadata().clone();
        let fields: Vec<String> = builder
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let path_idx = detect_column(&fields, PATH_NAMES, path_column, "path", &url)?;
        let content_idx = detect_column(&fields, CONTENT_NAMES, content_column, "content", &url)?;
        if path_idx == content_idx {
            return Err(FsError::Schema(format!(
                "path and content columns are the same ('{}') in {url}",
                fields[path_idx]
            )));
        }
        let extra_cols: Vec<usize> = (0..fields.len())
            .filter(|i| *i != path_idx && *i != content_idx)
            .collect();
        let extra_names: Vec<String> = extra_cols.iter().map(|&i| fields[i].clone()).collect();

        let mut row_group_offsets = Vec::new();
        let mut acc = 0usize;
        for rg in md.row_groups() {
            row_group_offsets.push(acc);
            acc += rg.num_rows() as usize;
        }

        let shard_id = index.shards.len();
        index.shards.push(Shard {
            url: url.clone(),
            reader,
            content_col: content_idx,
            extra_cols,
            extra_names,
            row_group_offsets: row_group_offsets.clone(),
        });

        let mask = ProjectionMask::roots(md.file_metadata().schema_descr(), [path_idx]);
        let batches = builder
            .with_projection(mask)
            .build()
            .map_err(|e| FsError::Parquet { url: url.clone(), source: e })?;

        let mut global = 0usize;
        for batch in batches {
            let batch = batch.map_err(|e| FsError::Parquet { url: url.clone(), source: e })?;
            for raw in path_strings(batch.column(0), &url)? {
                let norm = normalize(&raw);
                let (row_group, row) = locate(&row_group_offsets, global);
                global += 1;
                let entry = FileEntry {
                    loc: RowLoc { shard: shard_id, row_group, row },
                };
                match index.files.entry(norm) {
                    MapEntry::Vacant(v) => {
                        v.insert(entry);
                    }
                    MapEntry::Occupied(mut o) => match on_duplicate {
                        DupPolicy::Error => {
                            return Err(FsError::Duplicate {
                                path: o.key().clone(),
                                shard_a: index.shards[o.get().loc.shard].url.clone(),
                                shard_b: url.clone(),
                            })
                        }
                        DupPolicy::First => {}
                        DupPolicy::Last => {
                            o.insert(entry);
                        }
                    },
                }
            }
        }
    }

    for k in index.files.keys() {
        let mut p = k.as_str();
        while let Some(i) = p.rfind('/') {
            p = &p[..i];
            if !index.dirs.insert(p.to_string()) {
                break;
            }
        }
    }

    Ok(index)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test index_test`
Expected: 7 tests PASS. Also run `cargo test` — all previous tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/index.rs src/lib.rs tests/common tests/index_test.rs
git commit -m "feat: shard index with column detection, dir tree, duplicate policies"
```

---

### Task 5: Rust archive — content reads, LRU chunk cache, lazy sizes and metadata

**Files:**
- Create: `src/archive.rs`
- Modify: `src/lib.rs` (add `pub mod archive;`)
- Test: `tests/archive_test.rs`

**Interfaces:**
- Consumes: Task 4's `crate::index::*` (`build_index`, `Index`, `DupPolicy`, `MetaValue`, `normalize`, `DirEntry`), Task 3's `AdapterChunkReader`, Task 2's `FsError`.
- Produces (all `pub` in `crate::archive`):
  - `struct Archive` (Send + Sync) with:
    - `fn open(sources: &[String], path_column: Option<&str>, content_column: Option<&str>, on_duplicate: DupPolicy) -> Result<Archive, FsError>`
    - `fn read(&self, path: &str) -> Result<Vec<u8>, FsError>`
    - `fn info(&self, path: &str) -> Result<InfoResult, FsError>`
    - `fn ls(&self, path: &str) -> Result<Vec<DirEntry>, FsError>`
    - `fn exists(&self, path: &str) -> bool`
    - `fn is_dir(&self, path: &str) -> bool`
    - `fn paths(&self) -> Vec<String>` (sorted), `fn dirs(&self) -> Vec<String>` (sorted)
  - `enum InfoResult { File { size: u64, meta: Vec<(String, MetaValue)> }, Dir }`

- [ ] **Step 1: Write the failing tests**

`tests/archive_test.rs`:
```rust
mod common;

use common::{write_shard, write_shard_custom};
use parquet_file_fs::archive::{Archive, InfoResult};
use parquet_file_fs::index::{DupPolicy, MetaValue};

fn open(paths: &[&std::path::Path]) -> Archive {
    let sources: Vec<String> = paths.iter().map(|p| p.to_str().unwrap().to_string()).collect();
    Archive::open(&sources, None, None, DupPolicy::Error).unwrap()
}

#[test]
fn reads_content_across_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s.parquet");
    let rows: Vec<(String, Vec<u8>)> = (0..7)
        .map(|i| (format!("f/{i}.bin"), format!("content-{i}").into_bytes()))
        .collect();
    let rows_ref: Vec<(&str, &[u8])> = rows.iter().map(|(p, c)| (p.as_str(), c.as_slice())).collect();
    write_shard(&p, &rows_ref, 3); // 3 row groups: 3+3+1
    let a = open(&[&p]);
    for i in 0..7 {
        assert_eq!(a.read(&format!("f/{i}.bin")).unwrap(), format!("content-{i}").into_bytes());
    }
    // leading slash tolerated
    assert_eq!(a.read("/f/0.bin").unwrap(), b"content-0");
    assert!(matches!(a.read("f/99.bin"), Err(e) if e.to_string().contains("not found")));
}

#[test]
fn info_sizes_and_extra_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s.parquet");
    write_shard_custom(
        &p,
        "path",
        "content",
        &[("a.txt", b"12345"), ("b.txt", b"12")],
        Some(&[10, 20]),
        1,
    );
    let a = open(&[&p]);
    match a.info("a.txt").unwrap() {
        InfoResult::File { size, meta } => {
            assert_eq!(size, 5);
            assert!(matches!(
                meta.iter().find(|(k, _)| k == "num"),
                Some((_, MetaValue::Int(10)))
            ));
        }
        InfoResult::Dir => panic!("expected file"),
    }
    assert!(matches!(a.info("").unwrap(), InfoResult::Dir));
    assert!(a.info("zzz").is_err());
}

#[test]
fn exists_paths_dirs() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s.parquet");
    write_shard(&p, &[("x/y.txt", b"1"), ("z.txt", b"2")], 100);
    let a = open(&[&p]);
    assert!(a.exists("x/y.txt") && a.exists("x") && a.exists(""));
    assert!(!a.exists("nope"));
    assert!(a.is_dir("x") && !a.is_dir("z.txt"));
    assert_eq!(a.paths(), vec!["x/y.txt".to_string(), "z.txt".to_string()]);
    assert_eq!(a.dirs(), vec!["x".to_string()]);
}

#[test]
fn string_content_column_works() {
    // pyarrow-style shards sometimes store text content as Utf8
    use arrow_array::{ArrayRef, RecordBatch, StringArray};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s.parquet");
    let batch = RecordBatch::try_from_iter(vec![
        ("path", Arc::new(StringArray::from(vec!["a.txt"])) as ArrayRef),
        ("content", Arc::new(StringArray::from(vec!["hello"])) as ArrayRef),
    ])
    .unwrap();
    let f = std::fs::File::create(&p).unwrap();
    let mut w = ArrowWriter::try_new(f, batch.schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();

    let a = open(&[&p]);
    assert_eq!(a.read("a.txt").unwrap(), b"hello");
}
```
Add `pub mod archive;` to `src/lib.rs`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --test archive_test`
Expected: compilation error — module `archive` has no items.

- [ ] **Step 3: Write the implementation**

`src/archive.rs`:
```rust
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use lru::LruCache;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use crate::adapter::FsError;
use crate::index::{build_index, normalize, DirEntry, DupPolicy, Index, MetaValue};

pub enum InfoResult {
    File { size: u64, meta: Vec<(String, MetaValue)> },
    Dir,
}

struct RgDetail {
    sizes: Vec<u64>,
    metas: Vec<Vec<(String, MetaValue)>>,
}

pub struct Archive {
    index: Index,
    chunks: Mutex<LruCache<(usize, usize), ArrayRef>>,
    details: Mutex<HashMap<(usize, usize), Arc<RgDetail>>>,
}

fn binary_value(col: &ArrayRef, i: usize) -> Result<&[u8], FsError> {
    use arrow_array::*;
    match col.data_type() {
        DataType::Binary => Ok(col.as_any().downcast_ref::<BinaryArray>().unwrap().value(i)),
        DataType::LargeBinary => Ok(col
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(i)),
        DataType::BinaryView => Ok(col
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap()
            .value(i)),
        DataType::Utf8 => Ok(col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(i)
            .as_bytes()),
        DataType::LargeUtf8 => Ok(col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(i)
            .as_bytes()),
        DataType::Utf8View => Ok(col
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(i)
            .as_bytes()),
        dt => Err(FsError::Schema(format!(
            "content column must be binary or string, got {dt}"
        ))),
    }
}

fn meta_value(col: &ArrayRef, i: usize) -> MetaValue {
    use arrow_array::*;
    if col.is_null(i) {
        return MetaValue::Null;
    }
    macro_rules! prim {
        ($t:ty, $variant:ident, $conv:expr) => {{
            let a = col.as_any().downcast_ref::<$t>().unwrap();
            #[allow(clippy::redundant_closure_call)]
            MetaValue::$variant($conv(a.value(i)))
        }};
    }
    match col.data_type() {
        DataType::Boolean => prim!(BooleanArray, Bool, |v| v),
        DataType::Int8 => prim!(Int8Array, Int, |v| v as i64),
        DataType::Int16 => prim!(Int16Array, Int, |v| v as i64),
        DataType::Int32 => prim!(Int32Array, Int, |v| v as i64),
        DataType::Int64 => prim!(Int64Array, Int, |v| v),
        DataType::UInt8 => prim!(UInt8Array, Int, |v| v as i64),
        DataType::UInt16 => prim!(UInt16Array, Int, |v| v as i64),
        DataType::UInt32 => prim!(UInt32Array, Int, |v| v as i64),
        DataType::UInt64 => prim!(UInt64Array, Int, |v| v as i64),
        DataType::Float32 => prim!(Float32Array, Float, |v| v as f64),
        DataType::Float64 => prim!(Float64Array, Float, |v| v),
        DataType::Utf8 => prim!(StringArray, Str, |v: &str| v.to_string()),
        DataType::LargeUtf8 => prim!(LargeStringArray, Str, |v: &str| v.to_string()),
        DataType::Utf8View => prim!(StringViewArray, Str, |v: &str| v.to_string()),
        DataType::Binary => prim!(BinaryArray, Bytes, |v: &[u8]| v.to_vec()),
        DataType::LargeBinary => prim!(LargeBinaryArray, Bytes, |v: &[u8]| v.to_vec()),
        DataType::BinaryView => prim!(BinaryViewArray, Bytes, |v: &[u8]| v.to_vec()),
        _ => MetaValue::Null, // non-scalar / unsupported types
    }
}

impl Archive {
    pub fn open(
        sources: &[String],
        path_column: Option<&str>,
        content_column: Option<&str>,
        on_duplicate: DupPolicy,
    ) -> Result<Self, FsError> {
        let index = build_index(sources, path_column, content_column, on_duplicate)?;
        Ok(Self {
            index,
            chunks: Mutex::new(LruCache::new(NonZeroUsize::new(4).unwrap())),
            details: Mutex::new(HashMap::new()),
        })
    }

    /// Decode one row group's worth of a single projected column as one Array.
    fn read_rg_columns(
        &self,
        shard_id: usize,
        rg: usize,
        cols: &[usize],
    ) -> Result<Vec<ArrayRef>, FsError> {
        let shard = &self.index.shards[shard_id];
        let wrap = |e: parquet::errors::ParquetError| FsError::Parquet {
            url: shard.url.clone(),
            source: e,
        };
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(shard.reader.clone()).map_err(wrap)?;
        let rows = builder.metadata().row_group(rg).num_rows() as usize;
        let mask = ProjectionMask::roots(
            builder.metadata().file_metadata().schema_descr(),
            cols.iter().copied(),
        );
        let mut it = builder
            .with_projection(mask)
            .with_row_groups(vec![rg])
            .with_batch_size(rows.max(1))
            .build()
            .map_err(wrap)?;
        let batch = it
            .next()
            .ok_or_else(|| FsError::Schema(format!("empty row group {rg} in {}", shard.url)))?
            .map_err(wrap)?;
        Ok(batch.columns().to_vec())
    }

    fn load_content_chunk(&self, shard_id: usize, rg: usize) -> Result<ArrayRef, FsError> {
        if let Some(a) = self.chunks.lock().unwrap().get(&(shard_id, rg)) {
            return Ok(a.clone());
        }
        let content_col = self.index.shards[shard_id].content_col;
        let col = self.read_rg_columns(shard_id, rg, &[content_col])?.remove(0);
        self.chunks.lock().unwrap().put((shard_id, rg), col.clone());
        Ok(col)
    }

    fn load_detail(&self, shard_id: usize, rg: usize) -> Result<Arc<RgDetail>, FsError> {
        if let Some(d) = self.details.lock().unwrap().get(&(shard_id, rg)) {
            return Ok(d.clone());
        }
        let content = self.load_content_chunk(shard_id, rg)?;
        let sizes: Vec<u64> = (0..content.len())
            .map(|i| binary_value(&content, i).map(|b| b.len() as u64))
            .collect::<Result<_, _>>()?;
        let shard = &self.index.shards[shard_id];
        let metas: Vec<Vec<(String, MetaValue)>> = if shard.extra_cols.is_empty() {
            vec![Vec::new(); content.len()]
        } else {
            let cols = self.read_rg_columns(shard_id, rg, &shard.extra_cols)?;
            (0..content.len())
                .map(|i| {
                    shard
                        .extra_names
                        .iter()
                        .zip(cols.iter())
                        .map(|(n, c)| (n.clone(), meta_value(c, i)))
                        .collect()
                })
                .collect()
        };
        let d = Arc::new(RgDetail { sizes, metas });
        self.details.lock().unwrap().insert((shard_id, rg), d.clone());
        Ok(d)
    }

    pub fn read(&self, path: &str) -> Result<Vec<u8>, FsError> {
        let norm = normalize(path);
        let entry = self
            .index
            .files
            .get(&norm)
            .ok_or(FsError::NotFound(norm))?;
        let col = self.load_content_chunk(entry.loc.shard, entry.loc.row_group)?;
        binary_value(&col, entry.loc.row).map(|b| b.to_vec())
    }

    pub fn info(&self, path: &str) -> Result<InfoResult, FsError> {
        let norm = normalize(path);
        if let Some(entry) = self.index.files.get(&norm) {
            let d = self.load_detail(entry.loc.shard, entry.loc.row_group)?;
            return Ok(InfoResult::File {
                size: d.sizes[entry.loc.row],
                meta: d.metas[entry.loc.row].clone(),
            });
        }
        if self.index.is_dir(&norm) {
            return Ok(InfoResult::Dir);
        }
        Err(FsError::NotFound(norm))
    }

    pub fn ls(&self, path: &str) -> Result<Vec<DirEntry>, FsError> {
        self.index.ls(path)
    }

    pub fn exists(&self, path: &str) -> bool {
        let norm = normalize(path);
        self.index.files.contains_key(&norm) || self.index.is_dir(&norm)
    }

    pub fn is_dir(&self, path: &str) -> bool {
        self.index.is_dir(&normalize(path))
    }

    pub fn paths(&self) -> Vec<String> {
        self.index.files.keys().cloned().collect()
    }

    pub fn dirs(&self) -> Vec<String> {
        self.index.dirs.iter().cloned().collect()
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test archive_test`
Expected: 4 tests PASS. Run `cargo test` — everything passes.

- [ ] **Step 5: Commit**

```bash
git add src/archive.rs src/lib.rs tests/archive_test.rs
git commit -m "feat: archive content reads with LRU cache and lazy sizes/metadata"
```

---

### Task 6: Rust NativeAdapter — s3/http(s) via object_store

**Files:**
- Create: `src/native.rs`
- Modify: `src/adapter.rs` (rewire `resolve` for `s3`/`http`/`https`)
- Modify: `src/lib.rs` (add `pub mod native;`)
- Test: inline `#[cfg(test)]` in `src/native.rs` + one assertion change in `src/adapter.rs` tests

**Interfaces:**
- Consumes: Task 2's `RangeReader`, `FsError`, registry.
- Produces: `pub struct NativeAdapter` implementing `RangeReader`; `pub fn native() -> Arc<NativeAdapter>` (shared singleton with per-`scheme://authority` store cache); `pub fn glob_split(pattern: &str) -> (String, Option<globset::GlobMatcher>)`. `resolve("s3://…")`, `resolve("http://…")`, `resolve("https://…")` now return the native adapter. End-to-end http coverage lands in Task 10 (needs the Python layer).

- [ ] **Step 1: Write the failing tests**

Create `src/native.rs` with only:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_split_finds_prefix_and_matcher() {
        let (prefix, m) = glob_split("s3://bucket/data/shard-*.parquet");
        assert_eq!(prefix, "s3://bucket/data/");
        let m = m.unwrap();
        assert!(m.is_match("s3://bucket/data/shard-1.parquet"));
        assert!(!m.is_match("s3://bucket/data/deeper/shard-1.parquet")); // * must not cross '/'
        assert!(!m.is_match("s3://bucket/data/other.parquet"));
    }

    #[test]
    fn glob_split_without_magic_returns_input() {
        let (prefix, m) = glob_split("s3://bucket/data/shard.parquet");
        assert_eq!(prefix, "s3://bucket/data/shard.parquet");
        assert!(m.is_none());
    }

    #[test]
    fn http_glob_is_rejected() {
        let a = NativeAdapter::new();
        let err = a.list("http://example.com/*.parquet").unwrap_err();
        assert!(err.to_string().contains("glob"));
    }
}
```
In `src/adapter.rs` tests, extend `scheme_parsing_and_resolution`:
```rust
        assert!(resolve("s3://bucket/key").is_ok());
        assert!(resolve("https://example.com/x.parquet").is_ok());
```
Add `pub mod native;` to `src/lib.rs`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test`
Expected: `native` module compilation errors; `scheme_parsing_and_resolution` fails on `resolve("s3://...")`.

- [ ] **Step 3: Write the implementation**

Prepend to `src/native.rs`:
```rust
use bytes::Bytes;
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::adapter::{FsError, RangeReader};

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("failed to start tokio runtime")
});

static NATIVE: Lazy<Arc<NativeAdapter>> = Lazy::new(|| Arc::new(NativeAdapter::new()));

pub fn native() -> Arc<NativeAdapter> {
    NATIVE.clone()
}

/// Split a glob pattern into (non-glob URL prefix ending at a '/', matcher).
/// Returns (input, None) when the pattern has no glob metacharacters.
pub fn glob_split(pattern: &str) -> (String, Option<globset::GlobMatcher>) {
    match pattern.find(['*', '?', '[']) {
        None => (pattern.to_string(), None),
        Some(i) => {
            let cut = pattern[..i].rfind('/').map(|j| j + 1).unwrap_or(0);
            let matcher = globset::GlobBuilder::new(pattern)
                .literal_separator(true)
                .build()
                .ok()
                .map(|g| g.compile_matcher());
            (pattern[..cut].to_string(), matcher)
        }
    }
}

pub struct NativeAdapter {
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl Default for NativeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl NativeAdapter {
    pub fn new() -> Self {
        Self {
            stores: Mutex::new(HashMap::new()),
        }
    }

    fn adapter_err(url: &str, msg: impl ToString) -> FsError {
        FsError::Adapter {
            url: url.into(),
            msg: msg.to_string(),
        }
    }

    fn parse(url: &str) -> Result<url::Url, FsError> {
        url::Url::parse(url).map_err(|e| Self::adapter_err(url, e))
    }

    fn store_for(
        &self,
        u: &url::Url,
    ) -> Result<(Arc<dyn ObjectStore>, object_store::path::Path), FsError> {
        let key = format!("{}://{}", u.scheme(), u.authority());
        let mut stores = self.stores.lock().unwrap();
        let store = if let Some(s) = stores.get(&key) {
            s.clone()
        } else {
            let s: Arc<dyn ObjectStore> = match u.scheme() {
                "s3" => {
                    let bucket = u
                        .host_str()
                        .ok_or_else(|| Self::adapter_err(u.as_str(), "s3 url missing bucket"))?;
                    Arc::new(
                        object_store::aws::AmazonS3Builder::from_env()
                            .with_bucket_name(bucket)
                            .build()
                            .map_err(|e| Self::adapter_err(u.as_str(), e))?,
                    )
                }
                "http" | "https" => Arc::new(
                    object_store::http::HttpBuilder::new()
                        .with_url(key.clone())
                        .build()
                        .map_err(|e| Self::adapter_err(u.as_str(), e))?,
                ),
                s => {
                    return Err(FsError::UnknownScheme {
                        scheme: s.into(),
                        url: u.to_string(),
                    })
                }
            };
            stores.insert(key, s.clone());
            s
        };
        let path = object_store::path::Path::from(u.path().trim_start_matches('/'));
        Ok((store, path))
    }
}

impl RangeReader for NativeAdapter {
    fn size(&self, url: &str) -> Result<u64, FsError> {
        let u = Self::parse(url)?;
        let (store, path) = self.store_for(&u)?;
        let meta = RUNTIME
            .block_on(store.head(&path))
            .map_err(|e| Self::adapter_err(url, e))?;
        Ok(meta.size as u64)
    }

    fn read_range(&self, url: &str, offset: u64, length: u64) -> Result<Bytes, FsError> {
        let u = Self::parse(url)?;
        let (store, path) = self.store_for(&u)?;
        // object_store 0.12 uses Range<usize>; 0.13+ uses Range<u64>. Follow the compiler.
        let range = (offset as usize)..((offset + length) as usize);
        RUNTIME
            .block_on(store.get_range(&path, range))
            .map_err(|e| Self::adapter_err(url, e))
    }

    fn list(&self, pattern: &str) -> Result<Vec<String>, FsError> {
        let (prefix, matcher) = glob_split(pattern);
        let Some(matcher) = matcher else {
            return Ok(vec![prefix]);
        };
        let u = Self::parse(&prefix)?;
        if u.scheme() != "s3" {
            return Err(Self::adapter_err(
                pattern,
                "glob patterns are only supported for s3:// and local paths; \
                 pass concrete URLs for http(s)",
            ));
        }
        let (store, path) = self.store_for(&u)?;
        let metas = RUNTIME
            .block_on(async {
                use futures::TryStreamExt;
                store.list(Some(&path)).try_collect::<Vec<_>>().await
            })
            .map_err(|e| Self::adapter_err(pattern, e))?;
        let bucket = u.host_str().unwrap_or_default();
        let mut out: Vec<String> = metas
            .into_iter()
            .map(|m| format!("s3://{}/{}", bucket, m.location))
            .filter(|full| matcher.is_match(full))
            .collect();
        out.sort();
        Ok(out)
    }
}
```
In `src/adapter.rs`, replace the `resolve` match arms:
```rust
    match scheme {
        "file" => Ok(Arc::new(LocalAdapter)),
        "s3" | "http" | "https" => Ok(crate::native::native() as Arc<dyn RangeReader>),
        _ => Err(FsError::UnknownScheme {
            scheme: scheme.into(),
            url: url.into(),
        }),
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test`
Expected: all tests PASS (3 new in `native`, updated `adapter` test green).

- [ ] **Step 5: Commit**

```bash
git add src/native.rs src/adapter.rs src/lib.rs
git commit -m "feat: native s3/http adapter via object_store"
```

---

### Task 7: PyO3 bindings — `_core.Archive`, `_core.register_adapter`

**Files:**
- Create: `src/py.rs`
- Modify: `src/lib.rs` (wire module)
- Create: `tests/conftest.py`
- Test: `tests/test_core.py`

**Interfaces:**
- Consumes: Task 5's `Archive`/`InfoResult`, Task 4's `DupPolicy`/`MetaValue`/`normalize`, Task 2's `register`/`RangeReader`/`FsError`.
- Produces (Python-visible, used by Tasks 8–10):
  - `parquet_file_fs._core.Archive(sources: list[str], path_column: str | None = None, content_column: str | None = None, on_duplicate: str = "error")` with methods:
    - `ls(path: str) -> list[tuple[str, bool]]` — (name, is_dir)
    - `info(path: str) -> dict` — `{"name": str, "size": int, "type": "file"|"directory", "metadata": dict}`
    - `read(path: str) -> bytes`
    - `exists(path: str) -> bool`, `is_dir(path: str) -> bool`
    - `paths() -> list[str]`, `dirs() -> list[str]`
  - `parquet_file_fs._core.register_adapter(scheme: str, adapter: object)` — adapter must have `size(url)`, `read_range(url, offset, length)`, `glob(pattern)`.
  - Error mapping: `NotFound → FileNotFoundError`; `Schema | UnknownScheme | Duplicate → ValueError`; everything else → `OSError`.
  - pytest fixture `make_shard` in `tests/conftest.py` (used by Tasks 8–10).

- [ ] **Step 1: Write the pytest fixtures and failing tests**

`tests/conftest.py`:
```python
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def make_shard(path, rows, path_col="path", content_col="content", extra=None,
               row_group_size=None):
    """rows: list of (virtual_path, content_bytes). extra: {col_name: [values]}."""
    table = pa.table(
        {
            path_col: [r[0] for r in rows],
            content_col: [r[1] for r in rows],
            **(extra or {}),
        }
    )
    pq.write_table(table, path, row_group_size=row_group_size)


@pytest.fixture
def basic_shard(tmp_path):
    p = tmp_path / "shard.parquet"
    make_shard(
        p,
        [
            ("images/a.png", b"PNG-A"),
            ("images/b.png", b"PNG-B"),
            ("labels/a.json", b'{"route": "agentic"}'),
            ("readme.txt", b"hello"),
        ],
        extra={"route": ["a", "b", "c", "d"]},
    )
    return str(p)
```

`tests/test_core.py`:
```python
import pytest

from parquet_file_fs._core import Archive

from conftest import make_shard


def test_ls_read_exists(basic_shard):
    a = Archive([basic_shard])
    assert sorted(a.ls("")) == [
        ("images", True),
        ("labels", True),
        ("readme.txt", False),
    ]
    assert a.ls("images") == [("images/a.png", False), ("images/b.png", False)]
    assert a.read("images/a.png") == b"PNG-A"
    assert a.read("/images/a.png") == b"PNG-A"
    assert a.exists("labels") and a.is_dir("labels")
    assert not a.exists("nope")
    assert a.paths() == ["images/a.png", "images/b.png", "labels/a.json", "readme.txt"]
    assert a.dirs() == ["images", "labels"]


def test_info_metadata(basic_shard):
    a = Archive([basic_shard])
    info = a.info("labels/a.json")
    assert info["type"] == "file"
    assert info["size"] == len(b'{"route": "agentic"}')
    assert info["metadata"] == {"route": "c"}
    assert a.info("images")["type"] == "directory"


def test_errors(tmp_path, basic_shard):
    with pytest.raises(FileNotFoundError):
        Archive([basic_shard]).read("missing.txt")
    with pytest.raises(ValueError, match="register_adapter"):
        Archive(["weird://x/y.parquet"])
    p = tmp_path / "odd.parquet"
    make_shard(p, [("a", b"1")], path_col="pth", content_col="cnt")
    with pytest.raises(ValueError, match="pth"):
        Archive([str(p)])
    a = Archive([str(p)], path_column="pth", content_column="cnt")
    assert a.read("a") == b"1"


def test_on_duplicate(tmp_path):
    s1, s2 = tmp_path / "1.parquet", tmp_path / "2.parquet"
    make_shard(s1, [("x.txt", b"one")])
    make_shard(s2, [("x.txt", b"two")])
    sources = [str(s1), str(s2)]
    with pytest.raises(ValueError, match="duplicate path"):
        Archive(sources)
    assert Archive(sources, on_duplicate="first").read("x.txt") == b"one"
    assert Archive(sources, on_duplicate="last").read("x.txt") == b"two"
    with pytest.raises(ValueError):
        Archive(sources, on_duplicate="banana")


def test_multi_row_group_reads(tmp_path):
    p = tmp_path / "rg.parquet"
    rows = [(f"f/{i}.bin", f"content-{i}".encode()) for i in range(7)]
    make_shard(p, rows, row_group_size=3)
    a = Archive([str(p)])
    for i in range(7):
        assert a.read(f"f/{i}.bin") == f"content-{i}".encode()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/maturin develop
.venv/bin/pytest tests/test_core.py -q
```
Expected: FAIL — `ImportError: cannot import name 'Archive' from 'parquet_file_fs._core'`.

- [ ] **Step 3: Write the implementation**

`src/py.rs`:
```rust
use pyo3::exceptions::{PyFileNotFoundError, PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3::IntoPyObjectExt;
use std::sync::Arc;

use crate::adapter::{FsError, RangeReader};
use crate::archive::InfoResult;
use crate::index::{normalize, DupPolicy, MetaValue};

fn to_py(e: FsError) -> PyErr {
    match &e {
        FsError::NotFound(_) => PyFileNotFoundError::new_err(e.to_string()),
        FsError::Schema(_) | FsError::UnknownScheme { .. } | FsError::Duplicate { .. } => {
            PyValueError::new_err(e.to_string())
        }
        _ => PyIOError::new_err(e.to_string()),
    }
}

fn meta_to_py(py: Python<'_>, v: &MetaValue) -> PyObject {
    match v {
        MetaValue::Null => py.None(),
        MetaValue::Bool(b) => b.into_py_any(py).unwrap(),
        MetaValue::Int(i) => i.into_py_any(py).unwrap(),
        MetaValue::Float(f) => f.into_py_any(py).unwrap(),
        MetaValue::Str(s) => s.into_py_any(py).unwrap(),
        MetaValue::Bytes(b) => PyBytes::new(py, b).into_any().unbind(),
    }
}

/// Bridges a Python object with size/read_range/glob into the RangeReader trait.
pub struct PyAdapter {
    obj: Py<PyAny>,
}

impl PyAdapter {
    fn err(url: &str, e: PyErr) -> FsError {
        FsError::Adapter {
            url: url.into(),
            msg: e.to_string(),
        }
    }
}

impl RangeReader for PyAdapter {
    fn size(&self, url: &str) -> Result<u64, FsError> {
        Python::with_gil(|py| {
            self.obj
                .call_method1(py, "size", (url,))?
                .extract::<u64>(py)
        })
        .map_err(|e| Self::err(url, e))
    }

    fn read_range(&self, url: &str, offset: u64, length: u64) -> Result<bytes::Bytes, FsError> {
        Python::with_gil(|py| {
            self.obj
                .call_method1(py, "read_range", (url, offset, length))?
                .extract::<Vec<u8>>(py)
        })
        .map(bytes::Bytes::from)
        .map_err(|e| Self::err(url, e))
    }

    fn list(&self, pattern: &str) -> Result<Vec<String>, FsError> {
        Python::with_gil(|py| {
            self.obj
                .call_method1(py, "glob", (pattern,))?
                .extract::<Vec<String>>(py)
        })
        .map_err(|e| Self::err(pattern, e))
    }
}

#[pyfunction]
pub fn register_adapter(scheme: &str, adapter: Py<PyAny>) {
    crate::adapter::register(scheme, Arc::new(PyAdapter { obj: adapter }));
}

#[pyclass(frozen, name = "Archive")]
pub struct PyArchive {
    inner: crate::archive::Archive,
}

#[pymethods]
impl PyArchive {
    #[new]
    #[pyo3(signature = (sources, path_column=None, content_column=None, on_duplicate="error"))]
    fn new(
        py: Python<'_>,
        sources: Vec<String>,
        path_column: Option<String>,
        content_column: Option<String>,
        on_duplicate: &str,
    ) -> PyResult<Self> {
        let policy = DupPolicy::parse(on_duplicate).map_err(to_py)?;
        let inner = py
            .allow_threads(|| {
                crate::archive::Archive::open(
                    &sources,
                    path_column.as_deref(),
                    content_column.as_deref(),
                    policy,
                )
            })
            .map_err(to_py)?;
        Ok(Self { inner })
    }

    fn ls(&self, py: Python<'_>, path: &str) -> PyResult<Vec<(String, bool)>> {
        let entries = py.allow_threads(|| self.inner.ls(path)).map_err(to_py)?;
        Ok(entries.into_iter().map(|e| (e.name, e.is_dir)).collect())
    }

    fn info(&self, py: Python<'_>, path: &str) -> PyResult<PyObject> {
        let norm = normalize(path);
        let res = py.allow_threads(|| self.inner.info(&norm)).map_err(to_py)?;
        let d = PyDict::new(py);
        d.set_item("name", &norm)?;
        match res {
            InfoResult::Dir => {
                d.set_item("size", 0)?;
                d.set_item("type", "directory")?;
                d.set_item("metadata", PyDict::new(py))?;
            }
            InfoResult::File { size, meta } => {
                d.set_item("size", size)?;
                d.set_item("type", "file")?;
                let m = PyDict::new(py);
                for (k, v) in &meta {
                    m.set_item(k, meta_to_py(py, v))?;
                }
                d.set_item("metadata", m)?;
            }
        }
        Ok(d.into_any().unbind())
    }

    fn read<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyBytes>> {
        let data = py.allow_threads(|| self.inner.read(path)).map_err(to_py)?;
        Ok(PyBytes::new(py, &data))
    }

    fn exists(&self, py: Python<'_>, path: &str) -> bool {
        py.allow_threads(|| self.inner.exists(path))
    }

    fn is_dir(&self, py: Python<'_>, path: &str) -> bool {
        py.allow_threads(|| self.inner.is_dir(path))
    }

    fn paths(&self) -> Vec<String> {
        self.inner.paths()
    }

    fn dirs(&self) -> Vec<String> {
        self.inner.dirs()
    }
}
```
Replace `src/lib.rs` entirely:
```rust
use pyo3::prelude::*;

pub mod adapter;
pub mod archive;
pub mod chunk_reader;
pub mod index;
pub mod native;
mod py;

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<py::PyArchive>()?;
    m.add_function(wrap_pyfunction!(py::register_adapter, m)?)?;
    Ok(())
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test
.venv/bin/maturin develop
.venv/bin/pytest tests/test_core.py tests/test_smoke.py -q
```
Expected: cargo green; pytest `6 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/py.rs src/lib.rs tests/conftest.py tests/test_core.py
git commit -m "feat: PyO3 bindings for Archive and adapter registration"
```

---

### Task 8: Python adapter registry — `register_adapter` + fsspec shim

**Files:**
- Create: `python/parquet_file_fs/adapters.py`
- Test: `tests/test_adapters.py`

**Interfaces:**
- Consumes: Task 7's `_core.register_adapter` and `_core.Archive`; `tests/conftest.py::make_shard`.
- Produces:
  - `parquet_file_fs.adapters.register_adapter(scheme: str, adapter)` — accepts any `fsspec.AbstractFileSystem` (wrapped in `FsspecAdapter`) or any object with `size(url) -> int`, `read_range(url, offset, length) -> bytes`, `glob(pattern) -> list[str]`.
  - `parquet_file_fs.adapters.FsspecAdapter` — the shim class.

- [ ] **Step 1: Write the failing tests**

`tests/test_adapters.py`:
```python
import fnmatch

import pytest
from fsspec.implementations.memory import MemoryFileSystem

from parquet_file_fs._core import Archive
from parquet_file_fs.adapters import register_adapter

from conftest import make_shard


class DictAdapter:
    def __init__(self, blobs):
        self.blobs = blobs  # url -> bytes

    def size(self, url):
        return len(self.blobs[url])

    def read_range(self, url, offset, length):
        return self.blobs[url][offset : offset + length]

    def glob(self, pattern):
        if not any(c in pattern for c in "*?["):
            return [pattern]
        return sorted(u for u in self.blobs if fnmatch.fnmatch(u, pattern))


def test_duck_typed_adapter(basic_shard):
    with open(basic_shard, "rb") as f:
        blob = f.read()
    register_adapter("mem", DictAdapter({"mem://x/shard.parquet": blob}))
    a = Archive(["mem://x/*.parquet"])
    assert a.read("images/a.png") == b"PNG-A"
    assert a.info("readme.txt")["size"] == 5


def test_fsspec_filesystem_as_adapter(basic_shard):
    mfs = MemoryFileSystem()
    with open(basic_shard, "rb") as f:
        mfs.pipe_file("/data/shard.parquet", f.read())
    register_adapter("memory", mfs)
    a = Archive(["memory://data/*.parquet"])
    assert a.read("labels/a.json") == b'{"route": "agentic"}'


def test_adapter_errors_carry_url():
    class Broken:
        def size(self, url):
            raise RuntimeError("boom")

        def read_range(self, url, offset, length):
            raise RuntimeError("boom")

        def glob(self, pattern):
            return [pattern]

    register_adapter("broken", Broken())
    with pytest.raises(OSError, match="broken://x.parquet"):
        Archive(["broken://x.parquet"])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_adapters.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'parquet_file_fs.adapters'`.

- [ ] **Step 3: Write the implementation**

`python/parquet_file_fs/adapters.py`:
```python
"""Protocol adapter registry.

An adapter serves shard bytes for one URL scheme. The required interface:

    size(url) -> int
    read_range(url, offset, length) -> bytes   # exactly `length` bytes;
                                               # callers never request past EOF
    glob(pattern) -> list[str]                 # expand a glob; non-glob
                                               # patterns return [pattern]
"""

from __future__ import annotations

from fsspec import AbstractFileSystem

from parquet_file_fs._core import register_adapter as _core_register_adapter

_GLOB_CHARS = ("*", "?", "[")


class FsspecAdapter:
    """Adapts any fsspec AbstractFileSystem to the adapter interface."""

    def __init__(self, fs: AbstractFileSystem):
        self._fs = fs

    def size(self, url):
        return self._fs.info(url)["size"]

    def read_range(self, url, offset, length):
        return self._fs.cat_file(url, start=offset, end=offset + length)

    def glob(self, pattern):
        if not any(c in pattern for c in _GLOB_CHARS):
            return [pattern]
        return [self._fs.unstrip_protocol(p) for p in self._fs.glob(pattern)]


def register_adapter(scheme, adapter):
    """Register `adapter` for URLs with `scheme`.

    `adapter` is either an fsspec AbstractFileSystem or any object
    implementing size/read_range/glob (see module docstring).
    Registering a scheme again replaces the previous adapter; built-in
    schemes (file, s3, http, https) can be overridden the same way.
    """
    if isinstance(adapter, AbstractFileSystem):
        adapter = FsspecAdapter(adapter)
    _core_register_adapter(scheme, adapter)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_adapters.py -q`
Expected: `3 passed`.

- [ ] **Step 5: Commit**

```bash
git add python/parquet_file_fs/adapters.py tests/test_adapters.py
git commit -m "feat: python adapter registry with fsspec shim"
```

---

### Task 9: `ParquetFileSystem` — the fsspec filesystem

**Files:**
- Create: `python/parquet_file_fs/fs.py`
- Modify: `python/parquet_file_fs/__init__.py`
- Modify: `docs/superpowers/specs/2026-07-24-parquet-file-fs-design.md` (glob/find fast-path amendment)
- Test: `tests/test_fs.py`

**Interfaces:**
- Consumes: Task 7's `_core.Archive`, Task 8's `register_adapter`.
- Produces: `parquet_file_fs.ParquetFileSystem(sources, path_column=None, content_column=None, on_duplicate="error")`, an `fsspec.AbstractFileSystem` subclass with `protocol = "pfs"`, registered with fsspec on package import. Package exports: `ParquetFileSystem`, `register_adapter`, `__version__`.

- [ ] **Step 1: Write the failing tests**

`tests/test_fs.py`:
```python
import pytest

import fsspec
from parquet_file_fs import ParquetFileSystem

from conftest import make_shard


@pytest.fixture
def fs(basic_shard):
    return ParquetFileSystem(basic_shard)


def test_ls(fs):
    assert fs.ls("", detail=False) == ["images", "labels", "readme.txt"]
    detailed = fs.ls("images")
    assert detailed[0] == {
        "name": "images/a.png",
        "size": 5,
        "type": "file",
        "metadata": {"route": "a"},
    }
    assert fs.ls("pfs://images", detail=False) == ["images/a.png", "images/b.png"]


def test_info_exists_isdir(fs):
    assert fs.info("labels")["type"] == "directory"
    assert fs.info("readme.txt")["size"] == 5
    assert fs.exists("images/a.png") and not fs.exists("images/z.png")
    assert fs.isdir("images") and fs.isfile("readme.txt")


def test_cat_and_open(fs):
    assert fs.cat_file("images/a.png") == b"PNG-A"
    assert fs.cat_file("readme.txt", start=1, end=3) == b"el"
    with fs.open("readme.txt") as f:
        assert f.read() == b"hello"
    with fs.open("readme.txt", "r") as f:
        assert f.read() == "hello"


def test_glob_find_walk_du(fs):
    assert fs.glob("**/*.png") == ["images/a.png", "images/b.png"]
    assert fs.glob("images/*") == ["images/a.png", "images/b.png"]
    assert fs.glob("readme.txt") == ["readme.txt"]
    assert fs.glob("nope*") == []
    assert fs.find("") == [
        "images/a.png",
        "images/b.png",
        "labels/a.json",
        "readme.txt",
    ]
    assert fs.find("images") == ["images/a.png", "images/b.png"]
    walked = {root: (sorted(d), sorted(f)) for root, d, f in fs.walk("")}
    assert walked[""] == (["images", "labels"], ["readme.txt"])
    assert fs.du("images") == 10


def test_multi_shard_sources(tmp_path):
    make_shard(tmp_path / "a.parquet", [("a.txt", b"1")])
    make_shard(tmp_path / "b.parquet", [("b.txt", b"22")])
    fs = ParquetFileSystem(f"{tmp_path}/*.parquet")
    assert fs.ls("", detail=False) == ["a.txt", "b.txt"]
    fs2 = ParquetFileSystem([str(tmp_path / "a.parquet"), str(tmp_path / "b.parquet")])
    assert fs2.cat_file("b.txt") == b"22"


def test_readonly(fs):
    for method, args in [
        ("mkdir", ("d",)),
        ("makedirs", ("d",)),
        ("rmdir", ("images",)),
        ("mv", ("readme.txt", "x")),
        ("rm", ("readme.txt",)),
        ("rm_file", ("readme.txt",)),
        ("touch", ("new.txt",)),
        ("pipe_file", ("new.txt", b"x")),
        ("put_file", ("local", "remote")),
        ("cp_file", ("readme.txt", "copy.txt")),
    ]:
        with pytest.raises(NotImplementedError, match="read-only"):
            getattr(fs, method)(*args)
    with pytest.raises(NotImplementedError, match="read-only"):
        fs.open("new.txt", "wb")


def test_fsspec_registration(basic_shard):
    fs = fsspec.filesystem("pfs", sources=basic_shard)
    assert fs.cat_file("readme.txt") == b"hello"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_fs.py -q`
Expected: FAIL — `ImportError: cannot import name 'ParquetFileSystem'`.

- [ ] **Step 3: Write the implementation**

`python/parquet_file_fs/fs.py`:
```python
from __future__ import annotations

import io
import re

from fsspec import AbstractFileSystem
from fsspec.utils import glob_translate

from parquet_file_fs._core import Archive

_GLOB_CHARS = ("*", "?", "[")


def _readonly(self, *args, **kwargs):
    raise NotImplementedError("read-only filesystem")


class ParquetFileSystem(AbstractFileSystem):
    """Read-only fsspec filesystem over parquet archives.

    Each parquet row is one file: a path column holds its virtual path,
    a content column holds its bytes. `sources` is a path/URL or list
    thereof; each entry may be a glob (`data/*.parquet`,
    `s3://bucket/shard-*.parquet`).
    """

    protocol = "pfs"
    cachable = False

    def __init__(self, sources, path_column=None, content_column=None,
                 on_duplicate="error", **kwargs):
        super().__init__(**kwargs)
        if isinstance(sources, (str, bytes)):
            sources = [sources]
        self._archive = Archive(
            [str(s) for s in sources], path_column, content_column, on_duplicate
        )

    @classmethod
    def _strip_protocol(cls, path):
        return super()._strip_protocol(path).lstrip("/")

    # -- listings ---------------------------------------------------------
    def ls(self, path="", detail=True, **kwargs):
        entries = sorted(self._archive.ls(self._strip_protocol(path)))
        if not detail:
            return [name for name, _ in entries]
        return [
            {"name": name, "size": 0, "type": "directory", "metadata": {}}
            if is_dir
            else self.info(name)
            for name, is_dir in entries
        ]

    def info(self, path, **kwargs):
        return self._archive.info(self._strip_protocol(path))

    def exists(self, path, **kwargs):
        return self._archive.exists(self._strip_protocol(path))

    def find(self, path="", maxdepth=None, withdirs=False, detail=False, **kwargs):
        # Index-only fast path; fsspec's generic find would decode content
        # chunks via ls(detail=True) just to learn entry types.
        path = self._strip_protocol(path)
        prefix = f"{path}/" if path else ""
        names = [p for p in self._archive.paths() if p.startswith(prefix)]
        if not names and self._archive.exists(path) and not self._archive.is_dir(path):
            names = [path]
        if withdirs:
            names += [d for d in self._archive.dirs() if d.startswith(prefix)]
        if maxdepth is not None:
            base = prefix.count("/")
            names = [p for p in names if p.count("/") - base < maxdepth]
        names = sorted(names)
        if detail:
            return {p: self.info(p) for p in names}
        return names

    def glob(self, path, maxdepth=None, detail=False, **kwargs):
        if maxdepth is not None:
            return super().glob(path, maxdepth=maxdepth, detail=detail, **kwargs)
        path = self._strip_protocol(path)
        if not any(c in path for c in _GLOB_CHARS):
            if self.exists(path):
                return {path: self.info(path)} if detail else [path]
            return {} if detail else []
        pattern = re.compile(glob_translate(path))
        names = sorted(
            p
            for p in [*self._archive.paths(), *self._archive.dirs()]
            if pattern.match(p)
        )
        if detail:
            return {p: self.info(p) for p in names}
        return names

    # -- reads ------------------------------------------------------------
    def cat_file(self, path, start=None, end=None, **kwargs):
        data = self._archive.read(self._strip_protocol(path))
        if start is not None or end is not None:
            return data[start:end]
        return data

    def _open(self, path, mode="rb", **kwargs):
        if any(c in mode for c in "wa+x"):
            raise NotImplementedError("read-only filesystem")
        return io.BytesIO(self.cat_file(path))

    # -- writes: not supported ---------------------------------------------
    mkdir = _readonly
    makedirs = _readonly
    rmdir = _readonly
    mv = _readonly
    rm = _readonly
    rm_file = _readonly
    _rm = _readonly
    touch = _readonly
    pipe_file = _readonly
    put_file = _readonly
    cp_file = _readonly
```

Replace `python/parquet_file_fs/__init__.py`:
```python
from fsspec import register_implementation

from parquet_file_fs._core import __version__
from parquet_file_fs.adapters import register_adapter
from parquet_file_fs.fs import ParquetFileSystem

register_implementation("pfs", ParquetFileSystem, clobber=True)

__all__ = ["ParquetFileSystem", "register_adapter", "__version__"]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest -q`
Expected: all tests pass (smoke, core, adapters, fs).

- [ ] **Step 5: Amend the spec for the glob/find fast paths**

In `docs/superpowers/specs/2026-07-24-parquet-file-fs-design.md`, replace the sentence
`glob\`, \`find\`, \`du\`, \`walk\` are inherited from fsspec's generic implementations.`
(inside the Python-layer bullet list) with:

> `du` and `walk` are inherited from fsspec's generic implementations. `glob`
> and `find` are overridden with index-only fast paths that preserve fsspec
> semantics — the generic versions would decode content chunks just to list
> names. `glob` with an explicit `maxdepth` falls back to the generic
> implementation.

- [ ] **Step 6: Commit**

```bash
git add python/parquet_file_fs tests/test_fs.py docs/superpowers/specs
git commit -m "feat: fsspec ParquetFileSystem with index-only glob/find"
```

---

### Task 10: HTTP end-to-end test, README, CI

**Files:**
- Test: `tests/test_http.py`
- Modify: `README.md`
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: everything prior; no new interfaces produced.

- [ ] **Step 1: Write the failing HTTP e2e test**

`tests/test_http.py`:
```python
import http.server
import os
import threading
import urllib.parse

import pytest

from parquet_file_fs import ParquetFileSystem


class RangeHandler(http.server.BaseHTTPRequestHandler):
    """Minimal static file server with HTTP Range support (stdlib's
    SimpleHTTPRequestHandler ignores Range headers)."""

    directory = None  # set per-test via subclassing

    def _file(self):
        name = urllib.parse.urlparse(self.path).path.lstrip("/")
        p = os.path.join(self.directory, name)
        return p if os.path.isfile(p) else None

    def do_HEAD(self):
        p = self._file()
        if not p:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(os.path.getsize(p)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()

    def do_GET(self):
        p = self._file()
        if not p:
            self.send_error(404)
            return
        size = os.path.getsize(p)
        rng = self.headers.get("Range")
        with open(p, "rb") as f:
            if rng and rng.startswith("bytes="):
                start_s, _, end_s = rng[len("bytes="):].partition("-")
                start = int(start_s or 0)
                end = min(int(end_s) if end_s else size - 1, size - 1)
                f.seek(start)
                data = f.read(end - start + 1)
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            else:
                data = f.read()
                self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(data)

    def log_message(self, *args):
        pass


@pytest.fixture
def http_url(basic_shard):
    handler = type(
        "Handler", (RangeHandler,), {"directory": os.path.dirname(basic_shard)}
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    yield f"http://127.0.0.1:{port}/{os.path.basename(basic_shard)}"
    server.shutdown()


def test_read_shard_over_http(http_url):
    fs = ParquetFileSystem(http_url)
    assert fs.cat_file("readme.txt") == b"hello"
    assert fs.ls("", detail=False) == ["images", "labels", "readme.txt"]


def test_http_glob_rejected(http_url):
    base = http_url.rsplit("/", 1)[0]
    with pytest.raises(OSError, match="glob"):
        ParquetFileSystem(f"{base}/*.parquet")
```

- [ ] **Step 2: Run the test**

Run: `.venv/bin/pytest tests/test_http.py -q`
Expected: PASS if Tasks 6–9 are correct. If `test_read_shard_over_http` fails inside object_store's HttpStore (e.g. it rejects the plain server), debug with the failing status code — HttpStore uses HEAD for `head()` and GET+Range for `get_range()`; PROPFIND is only used for listing, which this path never calls.

- [ ] **Step 3: Write the README**

Replace `README.md`:
````markdown
# parquet-file-fs

Read-only [fsspec](https://filesystem-spec.readthedocs.io/) filesystem over
parquet "archive" shards — parquet files where each row is one file
(a **path** column + a **content** column). Rust core (arrow/parquet +
object_store) via maturin; built for handing datasets to coding agents:
`ls`, `cat`, `open`, `glob`, `walk` over shard contents without loading
whole shards into memory.

## Install

```bash
pip install parquet-file-fs
```

## Usage

```python
from parquet_file_fs import ParquetFileSystem

fs = ParquetFileSystem("data/*.parquet")            # local glob
fs = ParquetFileSystem("s3://bucket/shard-*.parquet")  # S3 (env credentials)
fs = ParquetFileSystem(["a.parquet", "b.parquet"])  # multi-shard

fs.ls("images", detail=False)     # ['images/a.png', 'images/b.png']
fs.cat_file("labels/a.json")      # b'{"route": "agentic"}'
fs.glob("**/*.json")              # index-only, no content decoding
fs.info("images/a.png")           # {'name': ..., 'size': 5, 'type': 'file',
                                  #  'metadata': {<extra parquet columns>}}
with fs.open("readme.txt", "r") as f:
    print(f.read())
```

Column names are auto-detected (`path`/`filename`/`file_name`/`key` and
`content`/`data`/`bytes`) or set explicitly:

```python
fs = ParquetFileSystem("odd.parquet", path_column="file_name",
                       content_column="image_bytes")
```

Duplicate paths across shards raise by default; pass
`on_duplicate="first"` or `"last"` to pick a shard instead.

## Protocol adapters

Built-in: local paths / `file://`, `s3://` (credentials from the
environment), `http://`, `https://`. Any other scheme can be registered —
either an fsspec filesystem or a tiny duck-typed object:

```python
import gcsfs
from parquet_file_fs import ParquetFileSystem, register_adapter

register_adapter("gs", gcsfs.GCSFileSystem())
fs = ParquetFileSystem("gs://bucket/data/*.parquet")
```

Custom adapter interface: `size(url) -> int`,
`read_range(url, offset, length) -> bytes` (must return exactly `length`
bytes; callers never request past EOF), `glob(pattern) -> list[str]`.

## Notes

- Read-only: mutation methods raise `NotImplementedError`.
- The index is built from parquet footers + the path column only; file
  content decodes lazily per row group (small LRU cache). Exact file sizes
  are computed on first `info`/`ls(detail=True)` per row group.
- Registered with fsspec as protocol `pfs`.

## Development

```bash
python3 -m venv .venv && .venv/bin/pip install maturin pytest pyarrow fsspec
cargo test                 # Rust tests
.venv/bin/maturin develop  # build + install the extension
.venv/bin/pytest           # Python tests
```
````

- [ ] **Step 4: Write the CI workflow**

`.github/workflows/ci.yml`:
```yaml
name: CI
on:
  push:
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Rust tests
        run: cargo test
      - name: Set up venv
        run: |
          python -m venv .venv
          .venv/bin/pip install maturin pytest pyarrow fsspec
      - name: Build extension
        run: .venv/bin/maturin develop
      - name: Python tests
        run: .venv/bin/pytest -q
```

- [ ] **Step 5: Run the full suite one last time**

```bash
cargo test
.venv/bin/pytest -q
```
Expected: everything green.

- [ ] **Step 6: Commit**

```bash
git add tests/test_http.py README.md .github/workflows/ci.yml
git commit -m "feat: http e2e test, README, CI workflow"
```

---

## Plan Self-Review Notes

- Spec coverage: archive semantics (T4/T5), multi-shard + glob discovery (T4/T6), lazy sizes + LRU (T5), native file/s3/http (T2/T6), Python adapter registry + fsspec shim (T7/T8), fsspec class with read-only enforcement and `pfs` registration (T9), error messages (T2/T4, asserted in T7 tests), Rust + Python test matrix and CI (all tasks, T10). Spec's "glob/find inherited" sentence is amended in T9 Step 5 to match the index-only fast paths.
- Version drift: crate versions in T1 are minimums; `object_store` range types and `parquet` `ChunkReader` signatures may differ by version — noted inline where it matters.



