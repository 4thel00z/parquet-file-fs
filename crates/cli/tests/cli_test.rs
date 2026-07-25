use std::io::Write;
use std::path::Path;
use std::process::Command;

use parquet_file_fs::archive::Archive;
use parquet_file_fs::index::DupPolicy;

fn pfs() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pfs"))
}

fn open(out: &Path) -> Archive {
    Archive::open(
        &[out.to_str().unwrap().to_string()],
        None,
        None,
        DupPolicy::Error,
    )
    .unwrap()
}

fn tree(dir: &Path) {
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    std::fs::write(dir.join("a.txt"), b"alpha").unwrap();
    std::fs::write(dir.join("sub/b.bin"), b"beta").unwrap();
}

#[test]
fn pack_glob_happy_path() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    tree(&data);
    let out = tmp.path().join("out.parquet");
    let o = pfs()
        .args([
            "pack",
            &format!("{}/**/*", data.display()),
            out.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(o.status.success(), "{o:?}");
    let stdout = String::from_utf8_lossy(&o.stdout);
    assert!(stdout.contains("packed 2 files"), "{stdout}");
    assert_eq!(open(&out).read("a.txt").unwrap(), b"alpha");
}

#[test]
fn pack_directory_shorthand_and_flags() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("data");
    tree(&data);
    let out = tmp.path().join("out.parquet");
    let o = pfs()
        .args([
            "pack",
            data.to_str().unwrap(),
            out.to_str().unwrap(),
            "--compression",
            "snappy",
            "--path-column",
            "file_name",
            "--content-column",
            "data",
        ])
        .output()
        .unwrap();
    assert!(o.status.success(), "{o:?}");
    let a = Archive::open(
        &[out.to_str().unwrap().to_string()],
        Some("file_name"),
        Some("data"),
        DupPolicy::Error,
    )
    .unwrap();
    assert_eq!(a.read("sub/b.bin").unwrap(), b"beta");
}

#[test]
fn pack_archive_zip() {
    let tmp = tempfile::tempdir().unwrap();
    let ar = tmp.path().join("bundle.zip");
    let f = std::fs::File::create(&ar).unwrap();
    let mut z = zip::ZipWriter::new(f);
    let o: zip::write::SimpleFileOptions = Default::default();
    z.start_file("inner.txt", o).unwrap();
    z.write_all(b"inner").unwrap();
    z.finish().unwrap();
    let out = tmp.path().join("out.parquet");
    let o = pfs()
        .args(["pack-archive", ar.to_str().unwrap(), out.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(o.status.success(), "{o:?}");
    assert_eq!(open(&out).read("inner.txt").unwrap(), b"inner");
}

#[test]
fn errors_exit_nonzero_with_stderr() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("out.parquet");
    let o = pfs()
        .args([
            "pack",
            &format!("{}/none/**/*", tmp.path().display()),
            out.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("no files matched"));

    let o = pfs()
        .args([
            "pack-archive",
            "/nonexistent.zip",
            out.to_str().unwrap(),
            "--format",
            "sit",
        ])
        .output()
        .unwrap();
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("unknown archive format"));
}
