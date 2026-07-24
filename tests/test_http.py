import http.server
import os
import threading
import urllib.parse

import pytest

from parquet_file_fs import ParquetFileSystem


class RangeHandler(http.server.BaseHTTPRequestHandler):
    """Minimal static file server with HTTP Range support (stdlib's
    SimpleHTTPRequestHandler ignores Range headers)."""

    directory = None  # set per-test via subclassing

    def _file(self):
        name = urllib.parse.urlparse(self.path).path.lstrip("/")
        p = os.path.join(self.directory, name)
        return p if os.path.isfile(p) else None

    def do_HEAD(self):
        p = self._file()
        if not p:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(os.path.getsize(p)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()

    def do_GET(self):
        p = self._file()
        if not p:
            self.send_error(404)
            return
        size = os.path.getsize(p)
        rng = self.headers.get("Range")
        with open(p, "rb") as f:
            if rng and rng.startswith("bytes="):
                start_s, _, end_s = rng[len("bytes="):].partition("-")
                start = int(start_s or 0)
                end = min(int(end_s) if end_s else size - 1, size - 1)
                f.seek(start)
                data = f.read(end - start + 1)
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            else:
                data = f.read()
                self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(data)

    def log_message(self, *args):
        pass


@pytest.fixture
def http_url(basic_shard):
    handler = type(
        "Handler", (RangeHandler,), {"directory": os.path.dirname(basic_shard)}
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    yield f"http://127.0.0.1:{port}/{os.path.basename(basic_shard)}"
    server.shutdown()


def test_read_shard_over_http(http_url):
    fs = ParquetFileSystem(http_url)
    assert fs.cat_file("readme.txt") == b"hello"
    assert fs.ls("", detail=False) == ["images", "labels", "readme.txt"]


def test_http_glob_rejected(http_url):
    base = http_url.rsplit("/", 1)[0]
    with pytest.raises(OSError, match="glob"):
        ParquetFileSystem(f"{base}/*.parquet")
