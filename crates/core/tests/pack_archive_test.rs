use std::io::Write;
use std::path::Path;

use parquet_file_fs::archive::Archive;
use parquet_file_fs::index::DupPolicy;
use parquet_file_fs::pack::{pack_archive, ArchiveFormat, PackOptions};

fn open(out: &Path) -> Archive {
    Archive::open(
        &[out.to_str().unwrap().to_string()],
        None,
        None,
        DupPolicy::Error,
    )
    .unwrap()
}

fn assert_roundtrip(out: &Path) {
    let a = open(out);
    assert_eq!(a.read("a.txt").unwrap(), b"alpha");
    assert_eq!(a.read("sub/b.bin").unwrap(), b"beta");
    assert_eq!(a.paths().len(), 2);
}

fn make_zip(path: &Path) {
    let f = std::fs::File::create(path).unwrap();
    let mut z = zip::ZipWriter::new(f);
    let o: zip::write::SimpleFileOptions = Default::default();
    z.add_directory("sub/", o).unwrap();
    z.start_file("a.txt", o).unwrap();
    z.write_all(b"alpha").unwrap();
    z.start_file("sub/b.bin", o).unwrap();
    z.write_all(b"beta").unwrap();
    z.finish().unwrap();
}

fn tar_bytes() -> Vec<u8> {
    let mut b = tar::Builder::new(Vec::new());
    let mut h = tar::Header::new_gnu();
    h.set_size(5);
    h.set_mode(0o644);
    h.set_cksum();
    b.append_data(&mut h, "a.txt", &b"alpha"[..]).unwrap();
    let mut h = tar::Header::new_gnu();
    h.set_size(4);
    h.set_mode(0o644);
    h.set_cksum();
    b.append_data(&mut h, "sub/b.bin", &b"beta"[..]).unwrap();
    b.into_inner().unwrap()
}

#[test]
fn zip_roundtrip_detected_by_magic() {
    let tmp = tempfile::tempdir().unwrap();
    let ar = tmp.path().join("weird-name.bin"); // detection must not need the extension
    make_zip(&ar);
    let out = tmp.path().join("out.parquet");
    let s = pack_archive(&ar, None, &out, &PackOptions::default()).unwrap();
    assert_eq!(s.files, 2); // the directory entry is skipped
    assert_roundtrip(&out);
}

#[test]
fn tar_and_compressed_tar_roundtrips() {
    let tmp = tempfile::tempdir().unwrap();
    let raw = tar_bytes();

    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("t.tar", raw.clone()),
        ("t.tar.gz", {
            let mut e = flate2::write::GzEncoder::new(Vec::new(), Default::default());
            e.write_all(&raw).unwrap();
            e.finish().unwrap()
        }),
        ("t.tar.bz2", {
            let mut e = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::default());
            e.write_all(&raw).unwrap();
            e.finish().unwrap()
        }),
        ("t.tar.xz", {
            let mut e = liblzma::write::XzEncoder::new(Vec::new(), 6);
            e.write_all(&raw).unwrap();
            e.finish().unwrap()
        }),
        ("t.tar.zst", zstd::stream::encode_all(&raw[..], 0).unwrap()),
    ];
    for (name, bytes) in cases {
        let ar = tmp.path().join(name);
        std::fs::write(&ar, bytes).unwrap();
        let out = tmp.path().join(format!("{name}.parquet"));
        pack_archive(&ar, None, &out, &PackOptions::default()).unwrap();
        assert_roundtrip(&out);
    }
}

#[test]
fn format_override_beats_detection() {
    let tmp = tempfile::tempdir().unwrap();
    let ar = tmp.path().join("mislabeled.zip"); // says zip, is tar
    std::fs::write(&ar, tar_bytes()).unwrap();
    let out = tmp.path().join("out.parquet");
    // magic sniffing sees ustar and wins over the extension
    pack_archive(&ar, None, &out, &PackOptions::default()).unwrap();
    assert_roundtrip(&out);
    // explicit override forces the wrong reader -> clear error
    let err = pack_archive(&ar, Some(ArchiveFormat::Zip), &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("zip"), "{err}");
}

#[test]
fn zip_slip_and_empty_archive_are_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let evil = tmp.path().join("evil.zip");
    let f = std::fs::File::create(&evil).unwrap();
    let mut z = zip::ZipWriter::new(f);
    let o: zip::write::SimpleFileOptions = Default::default();
    z.start_file("../escape.txt", o).unwrap();
    z.write_all(b"nope").unwrap();
    z.finish().unwrap();
    let out = tmp.path().join("out.parquet");
    let err = pack_archive(&evil, None, &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains(".."), "{err}");
    assert!(!out.exists());

    let empty = tmp.path().join("empty.zip");
    let zf = std::fs::File::create(&empty).unwrap();
    zip::ZipWriter::new(zf).finish().unwrap();
    let err = pack_archive(&empty, None, &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("contains no files"), "{err}");
}

#[test]
fn bare_gz_without_tar_gives_clear_error() {
    let tmp = tempfile::tempdir().unwrap();
    let ar = tmp.path().join("plain.gz");
    let mut e = flate2::write::GzEncoder::new(Vec::new(), Default::default());
    e.write_all(b"just text, no tar").unwrap();
    std::fs::write(&ar, e.finish().unwrap()).unwrap();
    let out = tmp.path().join("out.parquet");
    let err = pack_archive(&ar, None, &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("tar"), "{err}");
}

#[test]
fn unknown_format_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let ar = tmp.path().join("mystery.dat");
    std::fs::write(&ar, b"not an archive at all").unwrap();
    let out = tmp.path().join("out.parquet");
    let err = pack_archive(&ar, None, &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(err.to_string().contains("could not detect"), "{err}");
    assert!(ArchiveFormat::parse("tar.lol").is_err());
}

#[test]
fn sevenz_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir_all(src.join("sub")).unwrap();
    std::fs::write(src.join("a.txt"), b"alpha").unwrap();
    std::fs::write(src.join("sub/b.bin"), b"beta").unwrap();
    let ar = tmp.path().join("simple.7z");
    sevenz_rust2::compress_to_path(&src, &ar).unwrap();
    let out = tmp.path().join("out.parquet");
    pack_archive(&ar, None, &out, &PackOptions::default()).unwrap();
    assert_roundtrip(&out);
}

#[cfg(feature = "rar")]
#[test]
#[ignore = "requires fixtures/simple.rar (see fixtures/README.md)"]
fn rar_roundtrip_from_fixture() {
    let ar = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/simple.rar");
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("out.parquet");
    pack_archive(&ar, None, &out, &PackOptions::default()).unwrap();
    assert_roundtrip(&out);
}

#[cfg(not(feature = "rar"))]
#[test]
fn rar_without_feature_gives_clear_error() {
    let tmp = tempfile::tempdir().unwrap();
    let ar = tmp.path().join("x.rar");
    std::fs::write(&ar, b"Rar!\x1a\x07\x01\x00rest").unwrap();
    let out = tmp.path().join("out.parquet");
    let err = pack_archive(&ar, None, &out, &PackOptions::default())
        .err()
        .unwrap();
    assert!(
        err.to_string().contains("rar support not compiled in"),
        "{err}"
    );
}
