use std::path::{Path, PathBuf};

use parquet_file_fs::archive::Archive;
use parquet_file_fs::index::DupPolicy;
use parquet_file_fs::pack::{pack_files, PackCompression, PackOptions};

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
