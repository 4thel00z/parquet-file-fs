use bytes::Bytes;
use parquet::errors::ParquetError;
use parquet::file::reader::{ChunkReader, Length};
use std::sync::Arc;

use crate::adapter::RangeReader;

#[derive(Clone)]
pub struct AdapterChunkReader {
    pub adapter: Arc<dyn RangeReader>,
    pub url: Arc<String>,
    pub size: u64,
}

impl Length for AdapterChunkReader {
    fn len(&self) -> u64 {
        self.size
    }
}

pub struct AdapterRead {
    inner: AdapterChunkReader,
    pos: u64,
}

impl std::io::Read for AdapterRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.inner.size.saturating_sub(self.pos);
        let want = (buf.len() as u64).min(remaining);
        if want == 0 {
            return Ok(0);
        }
        let data = self
            .inner
            .adapter
            .read_range(&self.inner.url, self.pos, want)
            .map_err(std::io::Error::other)?;
        let n = data.len().min(buf.len());
        buf[..n].copy_from_slice(&data[..n]);
        self.pos += n as u64;
        Ok(n)
    }
}

impl ChunkReader for AdapterChunkReader {
    type T = AdapterRead;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(AdapterRead {
            inner: self.clone(),
            pos: start,
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let data = self
            .adapter
            .read_range(&self.url, start, length as u64)
            .map_err(|e| ParquetError::External(Box::new(e)))?;
        if data.len() != length {
            return Err(ParquetError::General(format!(
                "adapter returned {} bytes for {}, expected {}",
                data.len(),
                self.url,
                length
            )));
        }
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::LocalAdapter;
    use parquet::file::reader::{ChunkReader, Length};
    use std::io::{Read, Write};
    use std::sync::Arc;

    fn fixture() -> (tempfile::TempDir, AdapterChunkReader) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f.bin");
        std::fs::File::create(&p)
            .unwrap()
            .write_all(b"0123456789")
            .unwrap();
        let url = p.to_str().unwrap().to_string();
        let r = AdapterChunkReader {
            adapter: Arc::new(LocalAdapter),
            url: Arc::new(url),
            size: 10,
        };
        (dir, r)
    }

    #[test]
    fn get_bytes_reads_exact_range() {
        let (_d, r) = fixture();
        assert_eq!(r.len(), 10);
        assert_eq!(&r.get_bytes(2, 3).unwrap()[..], b"234");
    }

    #[test]
    fn get_read_streams_from_offset_and_stops_at_eof() {
        let (_d, r) = fixture();
        let mut buf = Vec::new();
        r.get_read(7).unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"789");
    }
}
