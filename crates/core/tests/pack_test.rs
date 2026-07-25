use std::path::{Path, PathBuf};

use parquet_file_fs::archive::Archive;
use parquet_file_fs::index::DupPolicy;
use parquet_file_fs::pack::{pack_files, pack_glob, PackCompression, PackOptions};

fn open(out: &Path) -> Archive {
    Archive::open(
        &[out.to_str().unwrap().to_string()],
        None,
        None,
        DupPolicy::Error,
    )
    .unwrap()
}

fn tree(dir: &Path) -> Vec<PathBuf> {
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    std::fs::write(dir.join("a.txt"), b"alpha").unwrap();
    std::fs::write(dir.join("sub/b.bin"), b"beta").unwrap();
    vec![dir.join("a.txt"), dir.join("sub/b.bin")]
}

#[test]
fn pack_files_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let files = tree(tmp.path());
    let out = tmp.path().join("out.parquet");
    let s = pack_files(&files, tmp.path(), &out, &PackOptions::default()).unwrap();
    assert_eq!((s.files, s.bytes), (2, 9));
    let a = open(&out);
    assert_eq!(a.read("a.txt").unwrap(), b"alpha");
    assert_eq!(a.read("sub/b.bin").unwrap(), b"beta");
    assert_eq!(
        a.paths(),
        vec!["a.txt".to_string(), "sub/b.bin".to_string()]
    );
}

#[test]
fn pack_files_rejects_duplicates() {
    let tmp = tempfile::tempdir().unwrap();
    let files = tree(tmp.path());
    let dup = vec![files[0].clone(), files[0].clone()];
    let out = tmp.path().join("out.parquet");
    let err = pack_files(&dup, tmp.path(), &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("duplicate path 'a.txt'"), "{err}");
    assert!(!out.exists(), "failed pack must not leave output behind");
}

#[test]
fn pack_files_rejects_out_of_root_and_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let files = tree(tmp.path());
    let out = tmp.path().join("out.parquet");
    let err = pack_files(
        &files,
        &tmp.path().join("sub"),
        &out,
        &PackOptions::default(),
    )
    .err()
    .unwrap();
    assert!(err.to_string().contains("outside root"), "{err}");
    let err = pack_files(&[], tmp.path(), &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("no files"), "{err}");
}

#[test]
fn row_groups_flush_at_threshold() {
    let tmp = tempfile::tempdir().unwrap();
    let mut files = Vec::new();
    for i in 0..4 {
        let p = tmp.path().join(format!("f{i}.bin"));
        std::fs::write(&p, vec![b'x'; 10]).unwrap();
        files.push(p);
    }
    let out = tmp.path().join("out.parquet");
    let opts = PackOptions {
        max_row_group_bytes: 20,
        ..PackOptions::default()
    };
    pack_files(&files, tmp.path(), &out, &opts).unwrap();
    let f = std::fs::File::open(&out).unwrap();
    let reader = parquet::file::reader::SerializedFileReader::new(f).unwrap();
    use parquet::file::reader::FileReader;
    assert_eq!(reader.metadata().num_row_groups(), 2); // 2 files per 20-byte group
    let a = open(&out);
    assert_eq!(a.read("f3.bin").unwrap(), vec![b'x'; 10]);
}

#[test]
fn compression_variants_are_readable() {
    let tmp = tempfile::tempdir().unwrap();
    let files = tree(tmp.path());
    for comp in ["zstd", "snappy", "none"] {
        let out = tmp.path().join(format!("out-{comp}.parquet"));
        let opts = PackOptions {
            compression: PackCompression::parse(comp).unwrap(),
            ..PackOptions::default()
        };
        pack_files(&files, tmp.path(), &out, &opts).unwrap();
        assert_eq!(open(&out).read("a.txt").unwrap(), b"alpha");
    }
    assert!(PackCompression::parse("brotli").is_err());
}

#[test]
fn custom_column_names() {
    let tmp = tempfile::tempdir().unwrap();
    let files = tree(tmp.path());
    let out = tmp.path().join("out.parquet");
    let opts = PackOptions {
        path_column: "file_name".into(),
        content_column: "image_bytes".into(),
        ..PackOptions::default()
    };
    pack_files(&files, tmp.path(), &out, &opts).unwrap();
    let a = Archive::open(
        &[out.to_str().unwrap().to_string()],
        Some("file_name"),
        Some("image_bytes"),
        DupPolicy::Error,
    )
    .unwrap();
    assert_eq!(a.read("a.txt").unwrap(), b"alpha");
}

#[test]
fn failed_pack_preserves_existing_output() {
    let tmp = tempfile::tempdir().unwrap();
    let files = tree(tmp.path());
    let out = tmp.path().join("out.parquet");

    // Write a valid pack first
    pack_files(&files, tmp.path(), &out, &PackOptions::default()).unwrap();
    let original = open(&out);
    assert_eq!(original.read("a.txt").unwrap(), b"alpha");

    // Attempt to pack with duplicates into the same path
    let dup = vec![files[0].clone(), files[0].clone()];
    let err = pack_files(&dup, tmp.path(), &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("duplicate path"), "{err}");

    // Verify original file is preserved
    let restored = open(&out);
    assert_eq!(restored.read("a.txt").unwrap(), b"alpha");
    assert_eq!(restored.read("sub/b.bin").unwrap(), b"beta");

    // Verify no .tmp file is left behind
    let tmp_file = tmp.path().join("out.parquet.tmp");
    assert!(!tmp_file.exists(), "temp file should be cleaned up");
}

#[test]
fn successful_repack_overwrites() {
    let tmp = tempfile::tempdir().unwrap();
    let files1 = tree(tmp.path());
    let out = tmp.path().join("out.parquet");

    // Pack first set
    pack_files(&files1, tmp.path(), &out, &PackOptions::default()).unwrap();
    let first = open(&out);
    assert_eq!(first.read("a.txt").unwrap(), b"alpha");

    // Create different content and repack into same location
    std::fs::remove_file(tmp.path().join("a.txt")).unwrap();
    std::fs::remove_file(tmp.path().join("sub/b.bin")).unwrap();
    std::fs::write(tmp.path().join("c.txt"), b"charlie").unwrap();
    std::fs::write(tmp.path().join("d.txt"), b"delta").unwrap();
    let files2 = vec![tmp.path().join("c.txt"), tmp.path().join("d.txt")];

    // Repack
    pack_files(&files2, tmp.path(), &out, &PackOptions::default()).unwrap();
    let second = open(&out);
    assert_eq!(second.read("c.txt").unwrap(), b"charlie");
    assert_eq!(second.read("d.txt").unwrap(), b"delta");
    // Original files should no longer be accessible
    assert!(second.read("a.txt").is_err());
    assert!(second.read("sub/b.bin").is_err());
}

#[test]
fn pack_glob_roundtrip_with_inferred_root() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    tree(&data);
    let out = tmp.path().join("out.parquet");
    let pattern = format!("{}/**/*", data.display());
    let s = pack_glob(&pattern, None, &out, &PackOptions::default()).unwrap();
    assert_eq!(s.files, 2);
    let a = open(&out);
    // root inferred as `<tmp>/data`, so paths are relative to it
    assert_eq!(
        a.paths(),
        vec!["a.txt".to_string(), "sub/b.bin".to_string()]
    );
}

#[test]
fn pack_glob_explicit_root() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    tree(&data);
    let out = tmp.path().join("out.parquet");
    let pattern = format!("{}/**/*", data.display());
    pack_glob(&pattern, Some(tmp.path()), &out, &PackOptions::default()).unwrap();
    let a = open(&out);
    assert_eq!(
        a.paths(),
        vec!["data/a.txt".to_string(), "data/sub/b.bin".to_string()]
    );
}

#[test]
fn pack_glob_directory_shorthand() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    tree(&data);
    let out = tmp.path().join("out.parquet");
    let s = pack_glob(data.to_str().unwrap(), None, &out, &PackOptions::default()).unwrap();
    assert_eq!(s.files, 2);
    assert_eq!(open(&out).read("sub/b.bin").unwrap(), b"beta");
}

#[test]
fn pack_glob_no_match_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("out.parquet");
    let pattern = format!("{}/nope/**/*", tmp.path().display());
    let err = pack_glob(&pattern, None, &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("no files matched"), "{err}");
    assert!(!out.exists());
}

#[test]
fn pack_glob_stores_matched_archives_as_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    // minimal zip built with the zip crate (a dependency from Task 4 — for
    // Task 3, write literal bytes instead: an empty-zip magic is enough)
    std::fs::write(data.join("bundle.zip"), b"PK\x05\x06 not really a full zip").unwrap();
    let out = tmp.path().join("out.parquet");
    pack_glob(
        &format!("{}/**/*", data.display()),
        None,
        &out,
        &PackOptions::default(),
    )
    .unwrap();
    let a = open(&out);
    assert_eq!(a.paths(), vec!["bundle.zip".to_string()]);
    assert!(a.read("bundle.zip").unwrap().starts_with(b"PK"));
}

#[cfg(unix)]
#[test]
fn non_utf8_output_path_roundtrips() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let tmp = tempfile::tempdir().unwrap();
    let files = tree(tmp.path());
    // "out-<0xFF>.parquet" — invalid UTF-8, so a display()-built temp path
    // would be lossy and land the .tmp sibling under a different name.
    let out = tmp.path().join(OsStr::from_bytes(b"out-\xff.parquet"));
    match pack_files(&files, tmp.path(), &out, &PackOptions::default()) {
        // APFS refuses to create non-UTF-8 filenames at all (EILSEQ), so the
        // scenario is only constructible on linux (ext4 & friends).
        Err(_) if cfg!(target_os = "macos") => return,
        r => r.unwrap(),
    };
    // Archive::open takes string URLs, so validate via the parquet reader.
    use parquet::file::reader::FileReader;
    let reader =
        parquet::file::reader::SerializedFileReader::new(std::fs::File::open(&out).unwrap())
            .unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 2);
    let stray: Vec<_> = std::fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().as_encoded_bytes().ends_with(b".tmp"))
        .collect();
    assert!(stray.is_empty(), "leftover temp files: {stray:?}");
}
