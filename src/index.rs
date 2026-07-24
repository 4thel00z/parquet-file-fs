use std::collections::btree_map::Entry as MapEntry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, LargeStringArray, StringArray};
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use crate::adapter::{resolve, FsError};
use crate::chunk_reader::AdapterChunkReader;

/// Canonical form of a virtual path: no leading/trailing separators and no
/// empty segments, so `a//b`, `/a/b` and `a/b/` all name the same file.
pub fn normalize(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for seg in path.split('/').filter(|s| !s.is_empty()) {
        if !out.is_empty() {
            out.push('/');
        }
        out.push_str(seg);
    }
    out
}

/// Map a global row index to (row_group, row_within_group) given cumulative offsets.
pub fn locate(offsets: &[usize], global: usize) -> (usize, usize) {
    let rg = offsets.partition_point(|&o| o <= global) - 1;
    (rg, global - offsets[rg])
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DupPolicy {
    Error,
    First,
    Last,
}

impl DupPolicy {
    pub fn parse(s: &str) -> Result<Self, FsError> {
        match s {
            "error" => Ok(Self::Error),
            "first" => Ok(Self::First),
            "last" => Ok(Self::Last),
            other => Err(FsError::Schema(format!(
                "on_duplicate must be 'error', 'first' or 'last', got '{other}'"
            ))),
        }
    }
}

#[derive(Clone, Debug)]
pub enum MetaValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
}

#[derive(Clone, Copy, Debug)]
pub struct RowLoc {
    pub shard: usize,
    pub row_group: usize,
    pub row: usize,
}

#[derive(Debug)]
pub struct FileEntry {
    pub loc: RowLoc,
}

pub struct Shard {
    pub url: String,
    pub reader: AdapterChunkReader,
    pub content_col: usize,
    pub extra_cols: Vec<usize>,
    pub extra_names: Vec<String>,
    pub row_group_offsets: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

pub struct Index {
    pub shards: Vec<Shard>,
    pub files: BTreeMap<String, FileEntry>,
    pub dirs: BTreeSet<String>,
}

impl Index {
    pub fn is_dir(&self, norm: &str) -> bool {
        norm.is_empty() || self.dirs.contains(norm)
    }

    pub fn ls(&self, path: &str) -> Result<Vec<DirEntry>, FsError> {
        let prefix = normalize(path);
        if !prefix.is_empty() && self.files.contains_key(&prefix) {
            return Ok(vec![DirEntry {
                name: prefix,
                is_dir: false,
            }]);
        }
        if !self.is_dir(&prefix) {
            return Err(FsError::NotFound(prefix));
        }
        let prefix_slash = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        let mut out = Vec::new();
        let mut seen_dirs = BTreeSet::new();
        for k in self.files.range(prefix_slash.clone()..).map(|(k, _)| k) {
            if !k.starts_with(&prefix_slash) {
                break;
            }
            let rest = &k[prefix_slash.len()..];
            match rest.split_once('/') {
                None => out.push(DirEntry {
                    name: k.clone(),
                    is_dir: false,
                }),
                Some((d, _)) => {
                    let full = format!("{prefix_slash}{d}");
                    // A path can be both a file and a directory prefix
                    // ("a" plus "a/b"). List the name once, as the file, so
                    // ls agrees with info/read; the children stay reachable
                    // through find/glob.
                    if seen_dirs.insert(d.to_string()) && !self.files.contains_key(&full) {
                        out.push(DirEntry {
                            name: full,
                            is_dir: true,
                        });
                    }
                }
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }
}

const PATH_NAMES: &[&str] = &["path", "filename", "file_name", "key"];
const CONTENT_NAMES: &[&str] = &["content", "data", "bytes"];

fn detect_column(
    fields: &[String],
    candidates: &[&str],
    explicit: Option<&str>,
    kind: &str,
    url: &str,
) -> Result<usize, FsError> {
    if let Some(name) = explicit {
        return fields.iter().position(|f| f == name).ok_or_else(|| {
            FsError::Schema(format!(
                "column '{name}' not found in {url}; available columns: {fields:?}"
            ))
        });
    }
    candidates
        .iter()
        .find_map(|c| fields.iter().position(|f| f.eq_ignore_ascii_case(c)))
        .ok_or_else(|| {
            FsError::Schema(format!(
                "could not detect {kind} column in {url}; available columns: {fields:?}; \
                 pass path_column=/content_column= explicitly"
            ))
        })
}

fn path_strings(col: &ArrayRef, url: &str) -> Result<Vec<String>, FsError> {
    let bad_null = || FsError::Schema(format!("path column contains a null value in {url}"));
    macro_rules! collect_strings {
        ($ty:ty) => {{
            let a = col.as_any().downcast_ref::<$ty>().unwrap();
            (0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        Err(bad_null())
                    } else {
                        Ok(a.value(i).to_string())
                    }
                })
                .collect()
        }};
    }
    match col.data_type() {
        DataType::Utf8 => collect_strings!(StringArray),
        DataType::LargeUtf8 => collect_strings!(LargeStringArray),
        DataType::Utf8View => collect_strings!(arrow_array::StringViewArray),
        dt => Err(FsError::Schema(format!(
            "path column in {url} must be a string type, got {dt}"
        ))),
    }
}

pub fn build_index(
    sources: &[String],
    path_column: Option<&str>,
    content_column: Option<&str>,
    on_duplicate: DupPolicy,
) -> Result<Index, FsError> {
    let mut urls = Vec::new();
    for s in sources {
        let adapter = resolve(s)?;
        let expanded = adapter.list(s)?;
        if expanded.is_empty() {
            return Err(FsError::Schema(format!("no shards matched '{s}'")));
        }
        urls.extend(expanded);
    }

    let mut index = Index {
        shards: Vec::new(),
        files: BTreeMap::new(),
        dirs: BTreeSet::new(),
    };

    for url in urls {
        let adapter = resolve(&url)?;
        let size = adapter.size(&url)?;
        let reader = AdapterChunkReader {
            adapter: adapter.clone(),
            url: Arc::new(url.clone()),
            size,
        };
        let builder = ParquetRecordBatchReaderBuilder::try_new(reader.clone()).map_err(|e| {
            FsError::Parquet {
                url: url.clone(),
                source: e,
            }
        })?;
        let md = builder.metadata().clone();
        let fields: Vec<String> = builder
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let path_idx = detect_column(&fields, PATH_NAMES, path_column, "path", &url)?;
        let content_idx = detect_column(&fields, CONTENT_NAMES, content_column, "content", &url)?;
        if path_idx == content_idx {
            return Err(FsError::Schema(format!(
                "path and content columns are the same ('{}') in {url}",
                fields[path_idx]
            )));
        }
        let extra_cols: Vec<usize> = (0..fields.len())
            .filter(|i| *i != path_idx && *i != content_idx)
            .collect();
        let extra_names: Vec<String> = extra_cols.iter().map(|&i| fields[i].clone()).collect();

        let mut row_group_offsets = Vec::new();
        let mut acc = 0usize;
        for rg in md.row_groups() {
            row_group_offsets.push(acc);
            acc += rg.num_rows() as usize;
        }

        let shard_id = index.shards.len();
        index.shards.push(Shard {
            url: url.clone(),
            reader,
            content_col: content_idx,
            extra_cols,
            extra_names,
            row_group_offsets: row_group_offsets.clone(),
        });

        let mask = ProjectionMask::roots(md.file_metadata().schema_descr(), [path_idx]);
        let batches = builder
            .with_projection(mask)
            .build()
            .map_err(|e| FsError::Parquet {
                url: url.clone(),
                source: e,
            })?;

        let mut global = 0usize;
        for batch in batches {
            let batch = batch.map_err(|e| FsError::Parquet {
                url: url.clone(),
                source: e.into(),
            })?;
            for raw in path_strings(batch.column(0), &url)? {
                let norm = normalize(&raw);
                let (row_group, row) = locate(&row_group_offsets, global);
                global += 1;
                if norm.is_empty() {
                    return Err(FsError::Schema(format!(
                        "row {} of {url} has an empty path ('{raw}'); every row must \
                         name a file",
                        global - 1
                    )));
                }
                let entry = FileEntry {
                    loc: RowLoc {
                        shard: shard_id,
                        row_group,
                        row,
                    },
                };
                match index.files.entry(norm) {
                    MapEntry::Vacant(v) => {
                        v.insert(entry);
                    }
                    MapEntry::Occupied(mut o) => match on_duplicate {
                        DupPolicy::Error => {
                            return Err(FsError::Duplicate {
                                path: o.key().clone(),
                                shard_a: index.shards[o.get().loc.shard].url.clone(),
                                shard_b: url.clone(),
                            })
                        }
                        DupPolicy::First => {}
                        DupPolicy::Last => {
                            o.insert(entry);
                        }
                    },
                }
            }
        }
    }

    for k in index.files.keys() {
        let mut p = k.as_str();
        while let Some(i) = p.rfind('/') {
            p = &p[..i];
            if !index.dirs.insert(p.to_string()) {
                break;
            }
        }
    }

    Ok(index)
}
