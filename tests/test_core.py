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
