use bytes::Bytes;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(thiserror::Error, Debug)]
pub enum FsError {
    #[error("adapter error for {url}: {msg}")]
    Adapter { url: String, msg: String },
    #[error("unknown URL scheme '{scheme}' for {url}; register one with parquet_file_fs.register_adapter('{scheme}', adapter)")]
    UnknownScheme { scheme: String, url: String },
    #[error("path not found: {0}")]
    NotFound(String),
    #[error("duplicate path '{path}' in {shard_a} and {shard_b}; pass on_duplicate='first' or 'last' to resolve")]
    Duplicate {
        path: String,
        shard_a: String,
        shard_b: String,
    },
    #[error("{0}")]
    Schema(String),
    #[error("parquet error for {url}: {source}")]
    Parquet {
        url: String,
        source: parquet::errors::ParquetError,
    },
    #[error("io error for {url}: {source}")]
    Io {
        url: String,
        source: std::io::Error,
    },
}

pub trait RangeReader: Send + Sync {
    fn size(&self, url: &str) -> Result<u64, FsError>;
    fn read_range(&self, url: &str, offset: u64, length: u64) -> Result<Bytes, FsError>;
    /// Expand a glob pattern into concrete URLs. Non-glob inputs return themselves.
    fn list(&self, pattern: &str) -> Result<Vec<String>, FsError>;
}

pub struct LocalAdapter;

fn strip_file_scheme(url: &str) -> &str {
    url.strip_prefix("file://").unwrap_or(url)
}

impl RangeReader for LocalAdapter {
    fn size(&self, url: &str) -> Result<u64, FsError> {
        std::fs::metadata(strip_file_scheme(url))
            .map(|m| m.len())
            .map_err(|e| FsError::Io {
                url: url.into(),
                source: e,
            })
    }

    fn read_range(&self, url: &str, offset: u64, length: u64) -> Result<Bytes, FsError> {
        use std::io::{Read, Seek, SeekFrom};
        let wrap = |e: std::io::Error| FsError::Io {
            url: url.into(),
            source: e,
        };
        let mut f = std::fs::File::open(strip_file_scheme(url)).map_err(wrap)?;
        f.seek(SeekFrom::Start(offset)).map_err(wrap)?;
        let mut buf = vec![0u8; length as usize];
        f.read_exact(&mut buf).map_err(wrap)?;
        Ok(buf.into())
    }

    fn list(&self, pattern: &str) -> Result<Vec<String>, FsError> {
        let p = strip_file_scheme(pattern);
        if !p.contains(['*', '?', '[']) {
            return Ok(vec![p.to_string()]);
        }
        let entries = glob::glob(p).map_err(|e| FsError::Adapter {
            url: pattern.into(),
            msg: e.to_string(),
        })?;
        let mut out: Vec<String> = entries
            .filter_map(|e| e.ok())
            .map(|pb| pb.to_string_lossy().into_owned())
            .collect();
        out.sort();
        Ok(out)
    }
}

static REGISTRY: Lazy<RwLock<HashMap<String, Arc<dyn RangeReader>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub fn register(scheme: &str, adapter: Arc<dyn RangeReader>) {
    REGISTRY.write().unwrap().insert(scheme.to_string(), adapter);
}

pub fn scheme_of(url: &str) -> &str {
    url.split_once("://").map(|(s, _)| s).unwrap_or("file")
}

pub fn resolve(url: &str) -> Result<Arc<dyn RangeReader>, FsError> {
    let scheme = scheme_of(url);
    if let Some(a) = REGISTRY.read().unwrap().get(scheme) {
        return Ok(a.clone());
    }
    match scheme {
        "file" => Ok(Arc::new(LocalAdapter)),
        // Task 6 rewires s3/http/https to NativeAdapter.
        _ => Err(FsError::UnknownScheme {
            scheme: scheme.into(),
            url: url.into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn local_size_and_read_range() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::File::create(&p)
            .unwrap()
            .write_all(b"hello world")
            .unwrap();
        let a = LocalAdapter;
        let url = p.to_str().unwrap();
        assert_eq!(a.size(url).unwrap(), 11);
        assert_eq!(&a.read_range(url, 6, 5).unwrap()[..], b"world");
        // file:// prefix works too
        let furl = format!("file://{url}");
        assert_eq!(a.size(&furl).unwrap(), 11);
    }

    #[test]
    fn local_list_glob() {
        let dir = tempfile::tempdir().unwrap();
        for n in ["a.parquet", "b.parquet", "c.txt"] {
            std::fs::File::create(dir.path().join(n)).unwrap();
        }
        let a = LocalAdapter;
        let pat = format!("{}/*.parquet", dir.path().to_str().unwrap());
        let got = a.list(&pat).unwrap();
        assert_eq!(got.len(), 2);
        assert!(got[0].ends_with("a.parquet") && got[1].ends_with("b.parquet"));
        // non-glob returns itself
        assert_eq!(a.list("/tmp/x.parquet").unwrap(), vec!["/tmp/x.parquet"]);
    }

    #[test]
    fn scheme_parsing_and_resolution() {
        assert_eq!(scheme_of("/tmp/a"), "file");
        assert_eq!(scheme_of("s3://b/k"), "s3");
        assert!(resolve("/tmp/a").is_ok());
        // `.err().unwrap()`, not `.unwrap_err()`: Arc<dyn RangeReader> isn't Debug.
        let err = resolve("weird://x").err().unwrap();
        let msg = err.to_string();
        assert!(msg.contains("weird") && msg.contains("register_adapter"));
    }

    #[test]
    fn registry_overrides() {
        struct Fake;
        impl RangeReader for Fake {
            fn size(&self, _: &str) -> Result<u64, FsError> {
                Ok(42)
            }
            fn read_range(&self, _: &str, _: u64, _: u64) -> Result<bytes::Bytes, FsError> {
                Ok(bytes::Bytes::new())
            }
            fn list(&self, p: &str) -> Result<Vec<String>, FsError> {
                Ok(vec![p.to_string()])
            }
        }
        register("fake", std::sync::Arc::new(Fake));
        assert_eq!(resolve("fake://x").unwrap().size("fake://x").unwrap(), 42);
    }
}
