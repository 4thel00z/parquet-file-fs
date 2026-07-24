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
