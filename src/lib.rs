use pyo3::prelude::*;

pub mod adapter;
pub mod archive;
pub mod chunk_reader;
pub mod index;

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
