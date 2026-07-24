import fnmatch

import pytest
from fsspec.implementations.memory import MemoryFileSystem

from parquet_file_fs._core import Archive
from parquet_file_fs.adapters import register_adapter


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
