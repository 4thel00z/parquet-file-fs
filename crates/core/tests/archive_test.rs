mod common;

use common::{write_shard, write_shard_custom};
use parquet_file_fs::archive::{Archive, InfoResult};
use parquet_file_fs::index::{DupPolicy, MetaValue};

fn open(paths: &[&std::path::Path]) -> Archive {
    let sources: Vec<String> = paths
        .iter()
        .map(|p| p.to_str().unwrap().to_string())
        .collect();
    Archive::open(&sources, None, None, DupPolicy::Error).unwrap()
}

#[test]
fn reads_content_across_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s.parquet");
    let rows: Vec<(String, Vec<u8>)> = (0..7)
        .map(|i| (format!("f/{i}.bin"), format!("content-{i}").into_bytes()))
        .collect();
    let rows_ref: Vec<(&str, &[u8])> = rows
        .iter()
        .map(|(p, c)| (p.as_str(), c.as_slice()))
        .collect();
    write_shard(&p, &rows_ref, 3); // 3 row groups: 3+3+1
    let a = open(&[&p]);
    for i in 0..7 {
        assert_eq!(
            a.read(&format!("f/{i}.bin")).unwrap(),
            format!("content-{i}").into_bytes()
        );
    }
    // leading slash tolerated
    assert_eq!(a.read("/f/0.bin").unwrap(), b"content-0");
    assert!(matches!(a.read("f/99.bin"), Err(e) if e.to_string().contains("not found")));
}

#[test]
fn info_sizes_and_extra_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s.parquet");
    write_shard_custom(
        &p,
        "path",
        "content",
        &[("a.txt", b"12345"), ("b.txt", b"12")],
        Some(&[10, 20]),
        1,
    );
    let a = open(&[&p]);
    match a.info("a.txt").unwrap() {
        InfoResult::File { size, meta } => {
            assert_eq!(size, 5);
            assert!(matches!(
                meta.iter().find(|(k, _)| k == "num"),
                Some((_, MetaValue::Int(10)))
            ));
        }
        InfoResult::Dir => panic!("expected file"),
    }
    assert!(matches!(a.info("").unwrap(), InfoResult::Dir));
    assert!(a.info("zzz").is_err());
}

#[test]
fn exists_paths_dirs() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s.parquet");
    write_shard(&p, &[("x/y.txt", b"1"), ("z.txt", b"2")], 100);
    let a = open(&[&p]);
    assert!(a.exists("x/y.txt") && a.exists("x") && a.exists(""));
    assert!(!a.exists("nope"));
    assert!(a.is_dir("x") && !a.is_dir("z.txt"));
    assert_eq!(a.paths(), vec!["x/y.txt".to_string(), "z.txt".to_string()]);
    assert_eq!(a.dirs(), vec!["x".to_string()]);
}

#[test]
fn string_content_column_works() {
    // pyarrow-style shards sometimes store text content as Utf8
    use arrow_array::{ArrayRef, RecordBatch, StringArray};
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s.parquet");
    let batch = RecordBatch::try_from_iter(vec![
        (
            "path",
            Arc::new(StringArray::from(vec!["a.txt"])) as ArrayRef,
        ),
        (
            "content",
            Arc::new(StringArray::from(vec!["hello"])) as ArrayRef,
        ),
    ])
    .unwrap();
    let f = std::fs::File::create(&p).unwrap();
    let mut w = ArrowWriter::try_new(f, batch.schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();

    let a = open(&[&p]);
    assert_eq!(a.read("a.txt").unwrap(), b"hello");
}
