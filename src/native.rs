use bytes::Bytes;
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::adapter::{FsError, RangeReader};

static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("failed to start tokio runtime")
});

static NATIVE: Lazy<Arc<NativeAdapter>> = Lazy::new(|| Arc::new(NativeAdapter::new()));

pub fn native() -> Arc<NativeAdapter> {
    NATIVE.clone()
}

/// Split a glob pattern into (non-glob URL prefix ending at a '/', matcher).
/// Returns (input, None) when the pattern has no glob metacharacters.
pub fn glob_split(pattern: &str) -> (String, Option<globset::GlobMatcher>) {
    match pattern.find(['*', '?', '[']) {
        None => (pattern.to_string(), None),
        Some(i) => {
            let cut = pattern[..i].rfind('/').map(|j| j + 1).unwrap_or(0);
            let matcher = globset::GlobBuilder::new(pattern)
                .literal_separator(true)
                .build()
                .ok()
                .map(|g| g.compile_matcher());
            (pattern[..cut].to_string(), matcher)
        }
    }
}

pub struct NativeAdapter {
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl Default for NativeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl NativeAdapter {
    pub fn new() -> Self {
        Self {
            stores: Mutex::new(HashMap::new()),
        }
    }

    fn adapter_err(url: &str, msg: impl ToString) -> FsError {
        FsError::Adapter {
            url: url.into(),
            msg: msg.to_string(),
        }
    }

    fn parse(url: &str) -> Result<url::Url, FsError> {
        url::Url::parse(url).map_err(|e| Self::adapter_err(url, e))
    }

    fn store_for(
        &self,
        u: &url::Url,
    ) -> Result<(Arc<dyn ObjectStore>, object_store::path::Path), FsError> {
        let key = format!("{}://{}", u.scheme(), u.authority());
        let mut stores = self.stores.lock().unwrap();
        let store = if let Some(s) = stores.get(&key) {
            s.clone()
        } else {
            let s: Arc<dyn ObjectStore> = match u.scheme() {
                "s3" => {
                    let bucket = u
                        .host_str()
                        .ok_or_else(|| Self::adapter_err(u.as_str(), "s3 url missing bucket"))?;
                    Arc::new(
                        object_store::aws::AmazonS3Builder::from_env()
                            .with_bucket_name(bucket)
                            .build()
                            .map_err(|e| Self::adapter_err(u.as_str(), e))?,
                    )
                }
                // object_store rejects cleartext http unless allow_http is set.
                // Only granted when the caller asked for http:// explicitly, so
                // an https:// URL still can't be downgraded by a redirect.
                "http" | "https" => {
                    let opts = object_store::ClientOptions::new()
                        .with_allow_http(u.scheme() == "http");
                    Arc::new(
                        object_store::http::HttpBuilder::new()
                            .with_url(key.clone())
                            .with_client_options(opts)
                            .build()
                            .map_err(|e| Self::adapter_err(u.as_str(), e))?,
                    )
                }
                s => {
                    return Err(FsError::UnknownScheme {
                        scheme: s.into(),
                        url: u.to_string(),
                    })
                }
            };
            stores.insert(key, s.clone());
            s
        };
        let path = object_store::path::Path::from(u.path().trim_start_matches('/'));
        Ok((store, path))
    }
}

impl RangeReader for NativeAdapter {
    fn size(&self, url: &str) -> Result<u64, FsError> {
        let u = Self::parse(url)?;
        let (store, path) = self.store_for(&u)?;
        let meta = RUNTIME
            .block_on(store.head(&path))
            .map_err(|e| Self::adapter_err(url, e))?;
        Ok(meta.size)
    }

    fn read_range(&self, url: &str, offset: u64, length: u64) -> Result<Bytes, FsError> {
        let u = Self::parse(url)?;
        let (store, path) = self.store_for(&u)?;
        RUNTIME
            .block_on(store.get_range(&path, offset..(offset + length)))
            .map_err(|e| Self::adapter_err(url, e))
    }

    fn list(&self, pattern: &str) -> Result<Vec<String>, FsError> {
        let (prefix, matcher) = glob_split(pattern);
        let Some(matcher) = matcher else {
            return Ok(vec![prefix]);
        };
        let u = Self::parse(&prefix)?;
        if u.scheme() != "s3" {
            return Err(Self::adapter_err(
                pattern,
                "glob patterns are only supported for s3:// and local paths; \
                 pass concrete URLs for http(s)",
            ));
        }
        let (store, path) = self.store_for(&u)?;
        let metas = RUNTIME
            .block_on(async {
                use futures::TryStreamExt;
                store.list(Some(&path)).try_collect::<Vec<_>>().await
            })
            .map_err(|e| Self::adapter_err(pattern, e))?;
        let bucket = u.host_str().unwrap_or_default();
        let mut out: Vec<String> = metas
            .into_iter()
            .map(|m| format!("s3://{}/{}", bucket, m.location))
            .filter(|full| matcher.is_match(full))
            .collect();
        out.sort();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_split_finds_prefix_and_matcher() {
        let (prefix, m) = glob_split("s3://bucket/data/shard-*.parquet");
        assert_eq!(prefix, "s3://bucket/data/");
        let m = m.unwrap();
        assert!(m.is_match("s3://bucket/data/shard-1.parquet"));
        assert!(!m.is_match("s3://bucket/data/deeper/shard-1.parquet")); // * must not cross '/'
        assert!(!m.is_match("s3://bucket/data/other.parquet"));
    }

    #[test]
    fn glob_split_without_magic_returns_input() {
        let (prefix, m) = glob_split("s3://bucket/data/shard.parquet");
        assert_eq!(prefix, "s3://bucket/data/shard.parquet");
        assert!(m.is_none());
    }

    #[test]
    fn http_glob_is_rejected() {
        let a = NativeAdapter::new();
        let err = a.list("http://example.com/*.parquet").err().unwrap();
        assert!(err.to_string().contains("glob"));
    }
}
