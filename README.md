<h1 align="center">parquet-file-fs</h1>

<p align="center">
  <strong>Browse parquet archives like a directory tree — a read-only fsspec filesystem with a Rust core.</strong>
</p>

<p align="center">
  <a href="https://github.com/4thel00z/parquet-file-fs/actions/workflows/ci.yaml"><img src="https://github.com/4thel00z/parquet-file-fs/actions/workflows/ci.yaml/badge.svg" alt="CI"></a>
  <a href="https://github.com/4thel00z/parquet-file-fs/actions/workflows/python-ci.yml"><img src="https://github.com/4thel00z/parquet-file-fs/actions/workflows/python-ci.yml/badge.svg" alt="python-ci"></a>
  <a href="https://pypi.org/project/parquet-file-fs/"><img src="https://img.shields.io/pypi/v/parquet-file-fs?logo=pypi&logoColor=white" alt="PyPI"></a>
  <a href="#license"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue" alt="License"></a>
</p>

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

Glob patterns are supported for local paths and `s3://`; pass concrete
URLs for `http(s)://`.

## Notes

- Read-only: mutation methods raise `NotImplementedError`.
- The index is built from parquet footers + the path column only; file
  content decodes lazily per row group (small LRU cache). Exact file sizes
  are computed on first `info`/`ls(detail=True)` per row group.
- Registered with fsspec as protocol `pfs`.

## Development

```bash
uv sync                 # dev deps from [dependency-groups]
uv run maturin develop  # build + install the extension
uv run pytest           # Python tests
cargo test              # Rust tests
cargo clippy --all-targets -- -D warnings
cargo fmt --all
```

Without uv:

```bash
python3 -m venv .venv && .venv/bin/pip install maturin pytest pyarrow pyyaml fsspec
.venv/bin/maturin develop && .venv/bin/pytest
```

## Releasing

Commits on `master` drive [release-please](https://github.com/googleapis/release-please):
it opens a release PR that bumps `pyproject.toml`, `Cargo.toml` (via the
`x-release-please-version` marker) and `Cargo.lock`. Merging that PR tags the
release and publishes wheels (linux x86_64, macOS arm64) plus an sdist to PyPI
through Trusted Publishing — no API tokens in the repo. A failed publish can be
re-run with `workflow_dispatch` on the `release-please` workflow.

## License

MIT OR Apache-2.0, at your option.
