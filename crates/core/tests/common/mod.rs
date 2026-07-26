use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

pub fn write_shard_custom(
    path: &std::path::Path,
    path_col: &str,
    content_col: &str,
    rows: &[(&str, &[u8])],
    extra_num: Option<&[i64]>,
    rows_per_group: usize,
) {
    let paths = StringArray::from_iter_values(rows.iter().map(|(p, _)| *p));
    let contents = BinaryArray::from_iter_values(rows.iter().map(|(_, c)| *c));
    let mut cols: Vec<(&str, ArrayRef)> = vec![
        (path_col, Arc::new(paths) as ArrayRef),
        (content_col, Arc::new(contents) as ArrayRef),
    ];
    if let Some(nums) = extra_num {
        cols.push(("num", Arc::new(Int64Array::from(nums.to_vec())) as ArrayRef));
    }
    let batch = RecordBatch::try_from_iter(cols).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(rows_per_group)
        .build();
    let file = std::fs::File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

pub fn write_shard(path: &std::path::Path, rows: &[(&str, &[u8])], rows_per_group: usize) {
    write_shard_custom(path, "path", "content", rows, None, rows_per_group);
}
