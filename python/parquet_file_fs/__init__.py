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
