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
