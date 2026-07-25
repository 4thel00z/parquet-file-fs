//! Write parquet archive shards: one row per file (path + content).

use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ArchiveFormat {
    Zip,
    Tar,
    TarGz,
    TarBz2,
    TarXz,
    TarZst,
    Rar,
    SevenZ,
}

impl ArchiveFormat {
    pub fn parse(s: &str) -> Result<Self, FsError> {
        match s {
            "zip" => Ok(Self::Zip),
            "tar" => Ok(Self::Tar),
            "tar.gz" | "tgz" => Ok(Self::TarGz),
            "tar.bz2" | "tbz2" => Ok(Self::TarBz2),
            "tar.xz" | "txz" => Ok(Self::TarXz),
            "tar.zst" | "tzst" => Ok(Self::TarZst),
            "rar" => Ok(Self::Rar),
            "7z" => Ok(Self::SevenZ),
            other => Err(FsError::Pack(format!(
                "unknown archive format '{other}'; expected zip, tar, tar.gz, \
                 tar.bz2, tar.xz, tar.zst, rar or 7z"
            ))),
        }
    }
}

/// Magic-byte detection with the file extension as a fallback tie-breaker.
fn sniff_format(archive: &Path) -> Result<ArchiveFormat, FsError> {
    let f = File::open(archive).map_err(|e| io_err(archive, e))?;
    let mut head = Vec::with_capacity(262);
    f.take(262)
        .read_to_end(&mut head)
        .map_err(|e| io_err(archive, e))?;
    let starts = |m: &[u8]| head.starts_with(m);
    if starts(&[0x50, 0x4B, 0x03, 0x04]) || starts(&[0x50, 0x4B, 0x05, 0x06]) {
        return Ok(ArchiveFormat::Zip);
    }
    if starts(&[0x1F, 0x8B]) {
        return Ok(ArchiveFormat::TarGz);
    }
    if starts(b"BZh") {
        return Ok(ArchiveFormat::TarBz2);
    }
    if starts(&[0xFD, b'7', b'z', b'X', b'Z', 0x00]) {
        return Ok(ArchiveFormat::TarXz);
    }
    if starts(&[0x28, 0xB5, 0x2F, 0xFD]) {
        return Ok(ArchiveFormat::TarZst);
    }
    if starts(b"Rar!\x1A\x07") {
        return Ok(ArchiveFormat::Rar);
    }
    if starts(&[0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C]) {
        return Ok(ArchiveFormat::SevenZ);
    }
    if head.len() >= 262 && &head[257..262] == b"ustar" {
        return Ok(ArchiveFormat::Tar);
    }
    let name = archive
        .file_name()
        .map(|n| n.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    for (suffix, fmt) in [
        (".zip", ArchiveFormat::Zip),
        (".tar.gz", ArchiveFormat::TarGz),
        (".tgz", ArchiveFormat::TarGz),
        (".tar.bz2", ArchiveFormat::TarBz2),
        (".tbz2", ArchiveFormat::TarBz2),
        (".tar.xz", ArchiveFormat::TarXz),
        (".txz", ArchiveFormat::TarXz),
        (".tar.zst", ArchiveFormat::TarZst),
        (".tzst", ArchiveFormat::TarZst),
        (".tar", ArchiveFormat::Tar),
        (".rar", ArchiveFormat::Rar),
        (".7z", ArchiveFormat::SevenZ),
    ] {
        if name.ends_with(suffix) {
            return Ok(fmt);
        }
    }
    Err(FsError::Pack(format!(
        "could not detect archive format of '{}'; pass the format explicitly",
        archive.display()
    )))
}

/// Normalize an archive entry name into a stored path: `/`-separators,
/// no leading `/`, no `.`/empty segments; `..` is rejected outright.
fn entry_stored_path(raw: &str) -> Result<String, FsError> {
    let mut segs = Vec::new();
    for seg in raw.replace('\\', "/").split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                return Err(FsError::Pack(format!(
                    "archive entry '{raw}' contains a '..' component"
                )))
            }
            s => segs.push(s.to_string()),
        }
    }
    if segs.is_empty() {
        return Err(FsError::Pack(format!(
            "archive entry '{raw}' normalizes to an empty path"
        )));
    }
    Ok(segs.join("/"))
}

fn pack_zip_entries(archive: &Path, w: &mut PackWriter) -> Result<(), FsError> {
    let f = File::open(archive).map_err(|e| io_err(archive, e))?;
    let mut z = zip::ZipArchive::new(f)
        .map_err(|e| FsError::Pack(format!("failed to open zip '{}': {e}", archive.display())))?;
    for i in 0..z.len() {
        let mut entry = z.by_index(i).map_err(|e| {
            FsError::Pack(format!(
                "failed to read zip entry {i} in '{}': {e}",
                archive.display()
            ))
        })?;
        if !entry.is_file() {
            continue;
        }
        let stored = entry_stored_path(entry.name())?;
        let mut data = Vec::with_capacity(entry.size() as usize);
        entry
            .read_to_end(&mut data)
            .map_err(|e| io_err(archive, e))?;
        w.append(stored, &data)?;
    }
    Ok(())
}

fn pack_tar_entries<R: Read>(reader: R, archive: &Path, w: &mut PackWriter) -> Result<(), FsError> {
    let bad = |e: std::io::Error| {
        FsError::Pack(format!(
            "failed to read tar stream from '{}': {e}; is this a (compressed) tar archive?",
            archive.display()
        ))
    };
    let mut a = tar::Archive::new(reader);
    for entry in a.entries().map_err(bad)? {
        let mut entry = entry.map_err(bad)?;
        if !entry.header().entry_type().is_file() {
            continue; // directories, symlinks, hardlinks, devices
        }
        let raw = entry.path().map_err(bad)?.to_string_lossy().into_owned();
        let stored = entry_stored_path(&raw)?;
        let mut data = Vec::new();
        entry.read_to_end(&mut data).map_err(bad)?;
        w.append(stored, &data)?;
    }
    Ok(())
}

fn pack_7z_entries(archive: &Path, w: &mut PackWriter) -> Result<(), FsError> {
    let bad = |e: String| FsError::Pack(format!("failed to read 7z '{}': {e}", archive.display()));
    let mut r = sevenz_rust2::ArchiveReader::open(archive, sevenz_rust2::Password::empty())
        .map_err(|e| bad(e.to_string()))?;
    let mut inner: Result<(), FsError> = Ok(());
    let result = r.for_each_entries(
        &mut |entry: &sevenz_rust2::ArchiveEntry, reader: &mut dyn Read| {
            if entry.is_directory() {
                std::io::copy(reader, &mut std::io::sink())?;
                return Ok(true);
            }
            let mut data = Vec::new();
            reader.read_to_end(&mut data)?;
            match entry_stored_path(entry.name()).and_then(|p| w.append(p, &data)) {
                Ok(()) => Ok(true),
                Err(e) => {
                    inner = Err(e);
                    // `Ok(false)` only halts iteration within the current block: the
                    // crate's archive-level loop iterates blocks with
                    // `folder_dec.for_each_entries(&mut each)?;`, discarding the `bool`
                    // between blocks, so a later block would still run and could
                    // overwrite `inner` with a second, unrelated error. Returning a
                    // genuine `Err` here makes both the block- and archive-level `?`
                    // halt immediately; it is just a carrier; the real error is `inner`,
                    // checked first below.
                    Err(std::io::Error::other("stopping after entry error").into())
                }
            }
        },
    );
    // Check `inner` first: if the closure detected an error, `result` is just the
    // carrier `Err` used to halt iteration, and must not mask the real (first) error.
    match inner {
        Err(e) => Err(e),
        Ok(()) => result.map_err(|e| bad(e.to_string())),
    }
}

#[cfg(feature = "rar")]
fn pack_rar_entries(archive: &Path, w: &mut PackWriter) -> Result<(), FsError> {
    let bad = |e: String| FsError::Pack(format!("failed to read rar '{}': {e}", archive.display()));
    let mut ar = unrar::Archive::new(archive)
        .open_for_processing()
        .map_err(|e| bad(e.to_string()))?;
    while let Some(header) = ar.read_header().map_err(|e| bad(e.to_string()))? {
        let is_file = header.entry().is_file();
        let raw = header.entry().filename.to_string_lossy().into_owned();
        ar = if is_file {
            let (data, next) = header.read().map_err(|e| bad(e.to_string()))?;
            w.append(entry_stored_path(&raw)?, &data)?;
            next
        } else {
            header.skip().map_err(|e| bad(e.to_string()))?
        };
    }
    Ok(())
}

#[cfg(not(feature = "rar"))]
fn pack_rar_entries(_archive: &Path, _w: &mut PackWriter) -> Result<(), FsError> {
    Err(FsError::Pack(
        "rar support not compiled in; rebuild with the 'rar' feature or \
         install the CLI (cargo install parquet-file-fs-cli)"
            .into(),
    ))
}

pub fn pack_archive(
    archive: &Path,
    format: Option<ArchiveFormat>,
    out: &Path,
    opts: &PackOptions,
) -> Result<PackSummary, FsError> {
    let fmt = match format {
        Some(f) => f,
        None => sniff_format(archive)?,
    };
    let mut w = PackWriter::create(out, opts)?;
    (|| {
        let open = || File::open(archive).map_err(|e| io_err(archive, e));
        match fmt {
            ArchiveFormat::Zip => pack_zip_entries(archive, &mut w)?,
            ArchiveFormat::Tar => pack_tar_entries(open()?, archive, &mut w)?,
            ArchiveFormat::TarGz => {
                pack_tar_entries(flate2::read::GzDecoder::new(open()?), archive, &mut w)?
            }
            ArchiveFormat::TarBz2 => {
                pack_tar_entries(bzip2::read::BzDecoder::new(open()?), archive, &mut w)?
            }
            ArchiveFormat::TarXz => {
                pack_tar_entries(liblzma::read::XzDecoder::new(open()?), archive, &mut w)?
            }
            ArchiveFormat::TarZst => pack_tar_entries(
                zstd::stream::read::Decoder::new(open()?).map_err(|e| io_err(archive, e))?,
                archive,
                &mut w,
            )?,
            ArchiveFormat::Rar => pack_rar_entries(archive, &mut w)?,
            ArchiveFormat::SevenZ => pack_7z_entries(archive, &mut w)?,
        }
        // Check before finish(): finish() renames tmp -> out, so once it succeeds
        // `out` has been overwritten. An empty archive must fail without ever
        // touching a pre-existing `out`; dropping the unfinished `w` here cleans
        // up only the (still-sibling) tmp file.
        if w.files == 0 {
            return Err(FsError::Pack(format!(
                "archive '{}' contains no files",
                archive.display()
            )));
        }
        w.finish()
    })()
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

    #[test]
    fn entry_paths_normalize_and_reject() {
        assert_eq!(entry_stored_path("/a//b/./c.txt").unwrap(), "a/b/c.txt");
        assert_eq!(entry_stored_path("w\\x.txt").unwrap(), "w/x.txt");
        assert!(entry_stored_path("../up.txt").is_err());
        assert!(entry_stored_path("./").is_err());
    }

    #[test]
    fn format_strings_parse() {
        for s in [
            "zip", "tar", "tar.gz", "tgz", "tar.bz2", "tar.xz", "tar.zst", "rar", "7z",
        ] {
            assert!(ArchiveFormat::parse(s).is_ok(), "{s}");
        }
    }
}
