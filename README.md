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
python3 -m venv .venv && .venv/bin/pip install maturin pytest pyarrow fsspec
cargo test                 # Rust tests
.venv/bin/maturin develop  # build + install the extension
.venv/bin/pytest           # Python tests
```
