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
