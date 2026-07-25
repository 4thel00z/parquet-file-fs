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
