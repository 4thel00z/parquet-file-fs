//! Write parquet archive shards: one row per file (path + content).

use std::collections::HashSet;
use std::fs::File;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use arrow_array::builder::{ArrayBuilder, LargeBinaryBuilder, StringBuilder};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::adapter::FsError;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PackCompression {
    Zstd,
    Snappy,
    None,
}

impl PackCompression {
    pub fn parse(s: &str) -> Result<Self, FsError> {
        match s {
            "zstd" => Ok(Self::Zstd),
            "snappy" => Ok(Self::Snappy),
            "none" => Ok(Self::None),
            other => Err(FsError::Pack(format!(
                "compression must be 'zstd', 'snappy' or 'none', got '{other}'"
            ))),
        }
    }

    fn to_parquet(self) -> Compression {
        match self {
            Self::Zstd => Compression::ZSTD(ZstdLevel::default()),
            Self::Snappy => Compression::SNAPPY,
            Self::None => Compression::UNCOMPRESSED,
        }
    }
}

pub struct PackOptions {
    pub path_column: String,
    pub content_column: String,
    pub compression: PackCompression,
    /// Flush a row group once buffered content reaches this many bytes.
    /// Small groups keep the reader's lazy per-row-group decode cheap.
    pub max_row_group_bytes: usize,
}

impl Default for PackOptions {
    fn default() -> Self {
        Self {
            path_column: "path".into(),
            content_column: "content".into(),
            compression: PackCompression::Zstd,
            max_row_group_bytes: 32 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PackSummary {
    pub files: u64,
    pub bytes: u64,
}

fn io_err(path: &Path, e: std::io::Error) -> FsError {
    FsError::Io {
        url: path.display().to_string(),
        source: e,
    }
}

fn pq_err(path: &Path, e: parquet::errors::ParquetError) -> FsError {
    FsError::Parquet {
        url: path.display().to_string(),
        source: e,
    }
}

pub(crate) struct PackWriter {
    out: PathBuf,
    tmp: PathBuf,
    writer: Option<ArrowWriter<File>>,
    schema: Arc<Schema>,
    paths: StringBuilder,
    contents: LargeBinaryBuilder,
    pending: usize,
    max_pending: usize,
    seen: HashSet<String>,
    files: u64,
    bytes: u64,
}

impl PackWriter {
    pub(crate) fn create(out: &Path, opts: &PackOptions) -> Result<Self, FsError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(&opts.path_column, DataType::Utf8, false),
            Field::new(&opts.content_column, DataType::LargeBinary, false),
        ]));
        let tmp = PathBuf::from(format!("{}.tmp", out.display()));
        let file = File::create(&tmp).map_err(|e| io_err(&tmp, e))?;
        let props = WriterProperties::builder()
            .set_compression(opts.compression.to_parquet())
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            pq_err(out, e)
        })?;
        Ok(Self {
            out: out.to_path_buf(),
            tmp,
            writer: Some(writer),
            schema,
            paths: StringBuilder::new(),
            contents: LargeBinaryBuilder::new(),
            pending: 0,
            max_pending: opts.max_row_group_bytes.max(1),
            seen: HashSet::new(),
            files: 0,
            bytes: 0,
        })
    }

    pub(crate) fn append(&mut self, stored: String, content: &[u8]) -> Result<(), FsError> {
        if !self.seen.insert(stored.clone()) {
            return Err(FsError::Pack(format!("duplicate path '{stored}' in input")));
        }
        self.paths.append_value(&stored);
        self.contents.append_value(content);
        self.pending += content.len();
        self.files += 1;
        self.bytes += content.len() as u64;
        if self.pending >= self.max_pending {
            self.flush_row_group()?;
        }
        Ok(())
    }

    fn flush_row_group(&mut self) -> Result<(), FsError> {
        if ArrayBuilder::len(&self.paths) == 0 {
            return Ok(());
        }
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.paths.finish()),
                Arc::new(self.contents.finish()),
            ],
        )
        .map_err(|e| FsError::Schema(e.to_string()))?;
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| FsError::Pack("writer already closed".into()))?;
        writer.write(&batch).map_err(|e| pq_err(&self.out, e))?;
        // ArrowWriter::flush closes the in-progress row group.
        writer.flush().map_err(|e| pq_err(&self.out, e))?;
        self.pending = 0;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<PackSummary, FsError> {
        self.flush_row_group()?;
        let writer = self
            .writer
            .take()
            .ok_or_else(|| FsError::Pack("writer already closed".into()))?;
        writer.close().map_err(|e| pq_err(&self.out, e))?;
        std::fs::rename(&self.tmp, &self.out).map_err(|e| io_err(&self.out, e))?;
        Ok(PackSummary {
            files: self.files,
            bytes: self.bytes,
        })
    }
}

impl Drop for PackWriter {
    fn drop(&mut self) {
        // Clean up temp file if not yet renamed. If finish() succeeded, writer is None
        // and temp file was renamed to out, so this becomes a best-effort cleanup.
        let _ = std::fs::remove_file(&self.tmp);
    }
}

/// Strip `.` components so textual prefix-matching works for roots like `.`.
fn clean(p: &Path) -> PathBuf {
    p.components()
        .filter(|c| !matches!(c, Component::CurDir))
        .collect()
}

/// Virtual path for `file` relative to `root`, `/`-separated.
pub(crate) fn stored_path(file: &Path, root: &Path) -> Result<String, FsError> {
    let cf = clean(file);
    let cr = clean(root);
    let rel = cf.strip_prefix(&cr).map_err(|_| {
        FsError::Pack(format!(
            "file '{}' is outside root '{}'",
            file.display(),
            root.display()
        ))
    })?;
    let mut segs = Vec::new();
    for c in rel.components() {
        match c {
            Component::Normal(s) => segs.push(s.to_string_lossy().into_owned()),
            _ => {
                return Err(FsError::Pack(format!(
                    "path '{}' escapes the pack root",
                    file.display()
                )))
            }
        }
    }
    if segs.is_empty() {
        return Err(FsError::Pack(format!(
            "'{}' yields an empty stored path",
            file.display()
        )));
    }
    Ok(segs.join("/"))
}

/// Stream sorted (stored_path, source_file) pairs into `out`.
/// Uses a temporary file (`out.tmp`) and atomic rename; on any error before `finish()`,
/// the temp file is cleaned up and `out` is left untouched, preserving existing content.
pub(crate) fn write_pairs(
    mut pairs: Vec<(String, PathBuf)>,
    out: &Path,
    opts: &PackOptions,
) -> Result<PackSummary, FsError> {
    pairs.sort();
    let mut w = PackWriter::create(out, opts)?;
    for (stored, src) in pairs {
        let data = std::fs::read(&src).map_err(|e| io_err(&src, e))?;
        w.append(stored, &data)?;
    }
    w.finish()
}

pub fn pack_files(
    paths: &[PathBuf],
    root: &Path,
    out: &Path,
    opts: &PackOptions,
) -> Result<PackSummary, FsError> {
    if paths.is_empty() {
        return Err(FsError::Pack("no files to pack".into()));
    }
    let mut pairs = Vec::with_capacity(paths.len());
    for p in paths {
        let md = std::fs::metadata(p).map_err(|e| io_err(p, e))?;
        if !md.is_file() {
            return Err(FsError::Pack(format!(
                "'{}' is not a regular file",
                p.display()
            )));
        }
        pairs.push((stored_path(p, root)?, p.clone()));
    }
    write_pairs(pairs, out, opts)
}

/// Longest wildcard-free directory prefix of a glob pattern; the last
/// segment never counts (a concrete filename is not its own root).
fn glob_fixed_prefix(pattern: &str) -> PathBuf {
    let segs: Vec<&str> = pattern.split('/').collect();
    let mut prefix = PathBuf::new();
    for (i, seg) in segs.iter().enumerate() {
        if i + 1 == segs.len() || seg.contains(['*', '?', '[']) {
            break;
        }
        if seg.is_empty() {
            prefix.push("/"); // leading empty segment of an absolute pattern
            continue;
        }
        prefix.push(seg);
    }
    prefix
}

pub fn pack_glob(
    pattern: &str,
    root: Option<&Path>,
    out: &Path,
    opts: &PackOptions,
) -> Result<PackSummary, FsError> {
    let (pattern, default_root) = if Path::new(pattern).is_dir() {
        let dir = pattern.trim_end_matches('/');
        (format!("{dir}/**/*"), PathBuf::from(dir))
    } else {
        (pattern.to_string(), glob_fixed_prefix(pattern))
    };
    let root = root.map(Path::to_path_buf).unwrap_or(default_root);
    let mut pairs = Vec::new();
    for entry in glob::glob(&pattern)
        .map_err(|e| FsError::Pack(format!("bad glob pattern '{pattern}': {e}")))?
    {
        let p = entry.map_err(|e| {
            let path = e.path().display().to_string();
            FsError::Io {
                url: path,
                source: e.into(),
            }
        })?;
        if p.is_file() {
            pairs.push((stored_path(&p, &root)?, p));
        }
    }
    if pairs.is_empty() {
        return Err(FsError::Pack(format!("no files matched '{pattern}'")));
    }
    write_pairs(pairs, out, opts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_prefix_of_patterns() {
        assert_eq!(
            glob_fixed_prefix("data/images/**/*.png"),
            PathBuf::from("data/images")
        );
        assert_eq!(glob_fixed_prefix("*.txt"), PathBuf::new());
        assert_eq!(glob_fixed_prefix("a/b/c.txt"), PathBuf::from("a/b"));
        assert_eq!(
            glob_fixed_prefix("/abs/dir/*.bin"),
            PathBuf::from("/abs/dir")
        );
    }
}
