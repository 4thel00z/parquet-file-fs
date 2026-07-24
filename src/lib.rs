use pyo3::prelude::*;

pub mod adapter;
pub mod archive;
pub mod chunk_reader;
pub mod index;
pub mod native;
mod py;

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<py::PyArchive>()?;
    m.add_function(wrap_pyfunction!(py::register_adapter, m)?)?;
    Ok(())
}
