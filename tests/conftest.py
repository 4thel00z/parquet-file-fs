import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def make_shard(path, rows, path_col="path", content_col="content", extra=None,
               row_group_size=None):
    """rows: list of (virtual_path, content_bytes). extra: {col_name: [values]}."""
    table = pa.table(
        {
            path_col: [r[0] for r in rows],
            content_col: [r[1] for r in rows],
            **(extra or {}),
        }
    )
    pq.write_table(table, path, row_group_size=row_group_size)


@pytest.fixture
def basic_shard(tmp_path):
    p = tmp_path / "shard.parquet"
    make_shard(
        p,
        [
            ("images/a.png", b"PNG-A"),
            ("images/b.png", b"PNG-B"),
            ("labels/a.json", b'{"route": "agentic"}'),
            ("readme.txt", b"hello"),
        ],
        extra={"route": ["a", "b", "c", "d"]},
    )
    return str(p)
