use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use lru::LruCache;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use crate::adapter::FsError;
use crate::index::{build_index, normalize, DirEntry, DupPolicy, Index, MetaValue};

pub enum InfoResult {
    File {
        size: u64,
        meta: Vec<(String, MetaValue)>,
    },
    Dir,
}

struct RgDetail {
    sizes: Vec<u64>,
    metas: Vec<Vec<(String, MetaValue)>>,
}

pub struct Archive {
    index: Index,
    chunks: Mutex<LruCache<(usize, usize), ArrayRef>>,
    details: Mutex<HashMap<(usize, usize), Arc<RgDetail>>>,
}

fn binary_value(col: &ArrayRef, i: usize) -> Result<&[u8], FsError> {
    use arrow_array::*;
    match col.data_type() {
        DataType::Binary => Ok(col.as_any().downcast_ref::<BinaryArray>().unwrap().value(i)),
        DataType::LargeBinary => Ok(col
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(i)),
        DataType::BinaryView => Ok(col
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap()
            .value(i)),
        DataType::Utf8 => Ok(col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(i)
            .as_bytes()),
        DataType::LargeUtf8 => Ok(col
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(i)
            .as_bytes()),
        DataType::Utf8View => Ok(col
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(i)
            .as_bytes()),
        dt => Err(FsError::Schema(format!(
            "content column must be binary or string, got {dt}"
        ))),
    }
}

fn meta_value(col: &ArrayRef, i: usize) -> MetaValue {
    use arrow_array::*;
    if col.is_null(i) {
        return MetaValue::Null;
    }
    macro_rules! prim {
        ($t:ty, $variant:ident, $conv:expr) => {{
            let a = col.as_any().downcast_ref::<$t>().unwrap();
            #[allow(clippy::redundant_closure_call)]
            MetaValue::$variant($conv(a.value(i)))
        }};
    }
    match col.data_type() {
        DataType::Boolean => prim!(BooleanArray, Bool, |v| v),
        DataType::Int8 => prim!(Int8Array, Int, |v| v as i64),
        DataType::Int16 => prim!(Int16Array, Int, |v| v as i64),
        DataType::Int32 => prim!(Int32Array, Int, |v| v as i64),
        DataType::Int64 => prim!(Int64Array, Int, |v| v),
        DataType::UInt8 => prim!(UInt8Array, Int, |v| v as i64),
        DataType::UInt16 => prim!(UInt16Array, Int, |v| v as i64),
        DataType::UInt32 => prim!(UInt32Array, Int, |v| v as i64),
        DataType::UInt64 => prim!(UInt64Array, Int, |v| v as i64),
        DataType::Float32 => prim!(Float32Array, Float, |v| v as f64),
        DataType::Float64 => prim!(Float64Array, Float, |v| v),
        DataType::Utf8 => prim!(StringArray, Str, |v: &str| v.to_string()),
        DataType::LargeUtf8 => prim!(LargeStringArray, Str, |v: &str| v.to_string()),
        DataType::Utf8View => prim!(StringViewArray, Str, |v: &str| v.to_string()),
        DataType::Binary => prim!(BinaryArray, Bytes, |v: &[u8]| v.to_vec()),
        DataType::LargeBinary => prim!(LargeBinaryArray, Bytes, |v: &[u8]| v.to_vec()),
        DataType::BinaryView => prim!(BinaryViewArray, Bytes, |v: &[u8]| v.to_vec()),
        _ => MetaValue::Null, // non-scalar / unsupported types
    }
}

impl Archive {
    pub fn open(
        sources: &[String],
        path_column: Option<&str>,
        content_column: Option<&str>,
        on_duplicate: DupPolicy,
    ) -> Result<Self, FsError> {
        let index = build_index(sources, path_column, content_column, on_duplicate)?;
        Ok(Self {
            index,
            chunks: Mutex::new(LruCache::new(NonZeroUsize::new(4).unwrap())),
            details: Mutex::new(HashMap::new()),
        })
    }

    /// Decode the given columns for a single row group.
    fn read_rg_columns(
        &self,
        shard_id: usize,
        rg: usize,
        cols: &[usize],
    ) -> Result<Vec<ArrayRef>, FsError> {
        let shard = &self.index.shards[shard_id];
        let wrap = |e: parquet::errors::ParquetError| FsError::Parquet {
            url: shard.url.clone(),
            source: e,
        };
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(shard.reader.clone()).map_err(wrap)?;
        let rows = builder.metadata().row_group(rg).num_rows() as usize;
        let mask = ProjectionMask::roots(
            builder.metadata().file_metadata().schema_descr(),
            cols.iter().copied(),
        );
        let mut it = builder
            .with_projection(mask)
            .with_row_groups(vec![rg])
            .with_batch_size(rows.max(1))
            .build()
            .map_err(wrap)?;
        let batch = it
            .next()
            .ok_or_else(|| FsError::Schema(format!("empty row group {rg} in {}", shard.url)))?
            .map_err(|e| FsError::Parquet {
                url: shard.url.clone(),
                source: e.into(),
            })?;
        Ok(batch.columns().to_vec())
    }

    fn load_content_chunk(&self, shard_id: usize, rg: usize) -> Result<ArrayRef, FsError> {
        if let Some(a) = self.chunks.lock().unwrap().get(&(shard_id, rg)) {
            return Ok(a.clone());
        }
        let content_col = self.index.shards[shard_id].content_col;
        let col = self.read_rg_columns(shard_id, rg, &[content_col])?.remove(0);
        self.chunks.lock().unwrap().put((shard_id, rg), col.clone());
        Ok(col)
    }

    fn load_detail(&self, shard_id: usize, rg: usize) -> Result<Arc<RgDetail>, FsError> {
        if let Some(d) = self.details.lock().unwrap().get(&(shard_id, rg)) {
            return Ok(d.clone());
        }
        let content = self.load_content_chunk(shard_id, rg)?;
        let sizes: Vec<u64> = (0..content.len())
            .map(|i| binary_value(&content, i).map(|b| b.len() as u64))
            .collect::<Result<_, _>>()?;
        let shard = &self.index.shards[shard_id];
        let metas: Vec<Vec<(String, MetaValue)>> = if shard.extra_cols.is_empty() {
            vec![Vec::new(); content.len()]
        } else {
            let cols = self.read_rg_columns(shard_id, rg, &shard.extra_cols)?;
            (0..content.len())
                .map(|i| {
                    shard
                        .extra_names
                        .iter()
                        .zip(cols.iter())
                        .map(|(n, c)| (n.clone(), meta_value(c, i)))
                        .collect()
                })
                .collect()
        };
        let d = Arc::new(RgDetail { sizes, metas });
        self.details
            .lock()
            .unwrap()
            .insert((shard_id, rg), d.clone());
        Ok(d)
    }

    pub fn read(&self, path: &str) -> Result<Vec<u8>, FsError> {
        let norm = normalize(path);
        let entry = self.index.files.get(&norm).ok_or(FsError::NotFound(norm))?;
        let col = self.load_content_chunk(entry.loc.shard, entry.loc.row_group)?;
        binary_value(&col, entry.loc.row).map(|b| b.to_vec())
    }

    pub fn info(&self, path: &str) -> Result<InfoResult, FsError> {
        let norm = normalize(path);
        if let Some(entry) = self.index.files.get(&norm) {
            let d = self.load_detail(entry.loc.shard, entry.loc.row_group)?;
            return Ok(InfoResult::File {
                size: d.sizes[entry.loc.row],
                meta: d.metas[entry.loc.row].clone(),
            });
        }
        if self.index.is_dir(&norm) {
            return Ok(InfoResult::Dir);
        }
        Err(FsError::NotFound(norm))
    }

    pub fn ls(&self, path: &str) -> Result<Vec<DirEntry>, FsError> {
        self.index.ls(path)
    }

    pub fn exists(&self, path: &str) -> bool {
        let norm = normalize(path);
        self.index.files.contains_key(&norm) || self.index.is_dir(&norm)
    }

    pub fn is_dir(&self, path: &str) -> bool {
        self.index.is_dir(&normalize(path))
    }

    pub fn paths(&self) -> Vec<String> {
        self.index.files.keys().cloned().collect()
    }

    pub fn dirs(&self) -> Vec<String> {
        self.index.dirs.iter().cloned().collect()
    }
}
