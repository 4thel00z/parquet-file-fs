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
