mod common;

use common::{write_shard, write_shard_custom};
use parquet_file_fs::index::{build_index, locate, normalize, DupPolicy};

const ROWS: &[(&str, &[u8])] = &[
    ("images/a.png", b"PNG-A"),
    ("images/b.png", b"PNG-B"),
    ("labels/a.json", b"{}"),
    ("readme.txt", b"hi"),
    ("images/sub/c.png", b"PNG-C"),
];

#[test]
fn normalize_strips_slashes() {
    assert_eq!(normalize("/a/b/"), "a/b");
    assert_eq!(normalize(""), "");
    assert_eq!(normalize("/"), "");
}

#[test]
fn locate_maps_global_rows() {
    let offsets = vec![0, 2, 4];
    assert_eq!(locate(&offsets, 0), (0, 0));
    assert_eq!(locate(&offsets, 1), (0, 1));
    assert_eq!(locate(&offsets, 2), (1, 0));
    assert_eq!(locate(&offsets, 4), (2, 0));
}

#[test]
fn builds_index_with_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s.parquet");
    write_shard(&p, ROWS, 2);
    let idx = build_index(
        &[p.to_str().unwrap().to_string()],
        None,
        None,
        DupPolicy::Error,
    )
    .unwrap();
    assert_eq!(idx.files.len(), 5);
    assert_eq!(idx.shards.len(), 1);
    let e = &idx.files["images/sub/c.png"];
    assert_eq!((e.loc.row_group, e.loc.row), (2, 0)); // row 4 with rows_per_group=2
    assert!(idx.dirs.contains("images") && idx.dirs.contains("images/sub"));
}

#[test]
fn ls_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s.parquet");
    write_shard(&p, ROWS, 100);
    let idx = build_index(
        &[p.to_str().unwrap().to_string()],
        None,
        None,
        DupPolicy::Error,
    )
    .unwrap();

    let root: Vec<String> = idx.ls("").unwrap().into_iter().map(|e| e.name).collect();
    assert!(root.contains(&"images".to_string()) && root.contains(&"readme.txt".to_string()));

    let images = idx.ls("/images/").unwrap();
    let names: Vec<&str> = images.iter().map(|e| e.name.as_str()).collect();
    assert_eq!(names, vec!["images/a.png", "images/b.png", "images/sub"]);
    assert!(images.iter().find(|e| e.name == "images/sub").unwrap().is_dir);

    // ls of a file returns itself
    let f = idx.ls("readme.txt").unwrap();
    assert_eq!(f.len(), 1);
    assert!(!f[0].is_dir);

    assert!(idx.ls("nope").is_err());
    assert!(idx.is_dir("images") && !idx.is_dir("readme.txt"));
}

#[test]
fn duplicate_policies() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a.parquet");
    let b = dir.path().join("b.parquet");
    write_shard(&a, &[("x.txt", b"from-a")], 100);
    write_shard(&b, &[("x.txt", b"from-b")], 100);
    let sources = vec![
        a.to_str().unwrap().to_string(),
        b.to_str().unwrap().to_string(),
    ];

    let err = build_index(&sources, None, None, DupPolicy::Error)
        .err()
        .unwrap();
    assert!(err.to_string().contains("duplicate path 'x.txt'"));

    let first = build_index(&sources, None, None, DupPolicy::First).unwrap();
    assert_eq!(first.files["x.txt"].loc.shard, 0);
    let last = build_index(&sources, None, None, DupPolicy::Last).unwrap();
    assert_eq!(last.files["x.txt"].loc.shard, 1);
}

#[test]
fn column_detection_and_override() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("odd.parquet");
    write_shard_custom(&p, "file_name", "image_bytes", &[("a", b"1")], None, 100);
    let src = vec![p.to_str().unwrap().to_string()];

    // auto-detect finds file_name but not image_bytes
    let err = build_index(&src, None, None, DupPolicy::Error).err().unwrap();
    let msg = err.to_string();
    assert!(msg.contains("content") && msg.contains("image_bytes"));

    let idx = build_index(&src, None, Some("image_bytes"), DupPolicy::Error).unwrap();
    assert_eq!(idx.files.len(), 1);

    // explicit missing column names available ones
    let err = build_index(&src, Some("nope"), Some("image_bytes"), DupPolicy::Error)
        .err()
        .unwrap();
    assert!(err.to_string().contains("file_name"));
}

#[test]
fn multi_shard_glob_and_empty_match() {
    let dir = tempfile::tempdir().unwrap();
    write_shard(&dir.path().join("s1.parquet"), &[("a.txt", b"1")], 100);
    write_shard(&dir.path().join("s2.parquet"), &[("b.txt", b"2")], 100);
    let pat = format!("{}/*.parquet", dir.path().to_str().unwrap());
    let idx = build_index(&[pat], None, None, DupPolicy::Error).unwrap();
    assert_eq!(idx.files.len(), 2);
    assert_eq!(idx.shards.len(), 2);

    let none = format!("{}/zzz-*.parquet", dir.path().to_str().unwrap());
    assert!(build_index(&[none], None, None, DupPolicy::Error).is_err());
}
