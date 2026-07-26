use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use parquet_file_fs::adapter::FsError;
use parquet_file_fs::pack::{
    pack_archive, pack_glob, ArchiveFormat, PackCompression, PackOptions, PackSummary,
};

#[derive(Parser)]
#[command(
    name = "pfs",
    version,
    about = "Create parquet archive shards readable by parquet-file-fs"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Pack files matched by a glob (or under a directory) into a parquet archive.
    ///
    /// Matched archive files (zip, tar, ...) are stored as plain bytes,
    /// never expanded — use `pack-archive` for that.
    Pack {
        /// Glob pattern (quote it!) or directory.
        source: String,
        /// Output parquet file.
        out: PathBuf,
        /// Directory stored paths are made relative to
        /// (default: the pattern's wildcard-free prefix).
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long, default_value = "path")]
        path_column: String,
        #[arg(long, default_value = "content")]
        content_column: String,
        /// zstd, snappy or none.
        #[arg(long, default_value = "zstd")]
        compression: String,
    },
    /// Expand one archive (zip, tar, tar.gz/bz2/xz/zst, rar, 7z) into a parquet archive.
    PackArchive {
        archive: PathBuf,
        out: PathBuf,
        /// zip|tar|tar.gz|tar.bz2|tar.xz|tar.zst|rar|7z (default: detect by magic bytes).
        #[arg(long)]
        format: Option<String>,
        #[arg(long, default_value = "path")]
        path_column: String,
        #[arg(long, default_value = "content")]
        content_column: String,
        /// zstd, snappy or none.
        #[arg(long, default_value = "zstd")]
        compression: String,
    },
}

fn build_opts(
    path_column: String,
    content_column: String,
    compression: &str,
) -> Result<PackOptions, FsError> {
    Ok(PackOptions {
        path_column,
        content_column,
        compression: PackCompression::parse(compression)?,
        ..PackOptions::default()
    })
}

fn run(cmd: Cmd) -> Result<(PackSummary, PathBuf), FsError> {
    match cmd {
        Cmd::Pack {
            source,
            out,
            root,
            path_column,
            content_column,
            compression,
        } => {
            let opts = build_opts(path_column, content_column, &compression)?;
            let s = pack_glob(&source, root.as_deref(), &out, &opts)?;
            Ok((s, out))
        }
        Cmd::PackArchive {
            archive,
            out,
            format,
            path_column,
            content_column,
            compression,
        } => {
            let fmt = format.as_deref().map(ArchiveFormat::parse).transpose()?;
            let opts = build_opts(path_column, content_column, &compression)?;
            let s = pack_archive(&archive, fmt, &out, &opts)?;
            Ok((s, out))
        }
    }
}

fn main() -> ExitCode {
    match run(Cli::parse().cmd) {
        Ok((s, out)) => {
            println!(
                "packed {} files ({} bytes) -> {}",
                s.files,
                s.bytes,
                out.display()
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_consistent() {
        Cli::command().debug_assert();
    }
}
