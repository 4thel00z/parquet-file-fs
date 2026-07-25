use pyo3::exceptions::{PyFileNotFoundError, PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3::IntoPyObjectExt;
use std::sync::Arc;

use parquet_file_fs::adapter::{FsError, RangeReader};
use parquet_file_fs::archive::InfoResult;
use parquet_file_fs::index::{normalize, DupPolicy, MetaValue};

fn to_py(e: FsError) -> PyErr {
    match &e {
        FsError::NotFound(_) => PyFileNotFoundError::new_err(e.to_string()),
        FsError::Schema(_) | FsError::UnknownScheme { .. } | FsError::Duplicate { .. } => {
            PyValueError::new_err(e.to_string())
        }
        _ => PyIOError::new_err(e.to_string()),
    }
}

fn meta_to_py(py: Python<'_>, v: &MetaValue) -> PyObject {
    match v {
        MetaValue::Null => py.None(),
        MetaValue::Bool(b) => b.into_py_any(py).unwrap(),
        MetaValue::Int(i) => i.into_py_any(py).unwrap(),
        MetaValue::Float(f) => f.into_py_any(py).unwrap(),
        MetaValue::Str(s) => s.into_py_any(py).unwrap(),
        MetaValue::Bytes(b) => PyBytes::new(py, b).into_any().unbind(),
    }
}

/// Bridges a Python object with size/read_range/glob into the RangeReader trait.
pub struct PyAdapter {
    obj: Py<PyAny>,
}

impl PyAdapter {
    fn err(url: &str, e: PyErr) -> FsError {
        FsError::Adapter {
            url: url.into(),
            msg: e.to_string(),
        }
    }
}

impl RangeReader for PyAdapter {
    fn size(&self, url: &str) -> Result<u64, FsError> {
        Python::with_gil(|py| {
            self.obj
                .call_method1(py, "size", (url,))?
                .extract::<u64>(py)
        })
        .map_err(|e| Self::err(url, e))
    }

    fn read_range(&self, url: &str, offset: u64, length: u64) -> Result<bytes::Bytes, FsError> {
        Python::with_gil(|py| {
            self.obj
                .call_method1(py, "read_range", (url, offset, length))?
                .extract::<Vec<u8>>(py)
        })
        .map(bytes::Bytes::from)
        .map_err(|e| Self::err(url, e))
    }

    fn list(&self, pattern: &str) -> Result<Vec<String>, FsError> {
        Python::with_gil(|py| {
            self.obj
                .call_method1(py, "glob", (pattern,))?
                .extract::<Vec<String>>(py)
        })
        .map_err(|e| Self::err(pattern, e))
    }
}

#[pyfunction]
pub fn register_adapter(scheme: &str, adapter: Py<PyAny>) {
    parquet_file_fs::adapter::register(scheme, Arc::new(PyAdapter { obj: adapter }));
}

#[pyclass(frozen, name = "Archive")]
pub struct PyArchive {
    inner: parquet_file_fs::archive::Archive,
}

#[pymethods]
impl PyArchive {
    #[new]
    #[pyo3(signature = (sources, path_column=None, content_column=None, on_duplicate="error"))]
    fn new(
        py: Python<'_>,
        sources: Vec<String>,
        path_column: Option<String>,
        content_column: Option<String>,
        on_duplicate: &str,
    ) -> PyResult<Self> {
        let policy = DupPolicy::parse(on_duplicate).map_err(to_py)?;
        let inner = py
            .allow_threads(|| {
                parquet_file_fs::archive::Archive::open(
                    &sources,
                    path_column.as_deref(),
                    content_column.as_deref(),
                    policy,
                )
            })
            .map_err(to_py)?;
        Ok(Self { inner })
    }

    fn ls(&self, py: Python<'_>, path: &str) -> PyResult<Vec<(String, bool)>> {
        let entries = py.allow_threads(|| self.inner.ls(path)).map_err(to_py)?;
        Ok(entries.into_iter().map(|e| (e.name, e.is_dir)).collect())
    }

    fn info(&self, py: Python<'_>, path: &str) -> PyResult<PyObject> {
        let norm = normalize(path);
        let res = py.allow_threads(|| self.inner.info(&norm)).map_err(to_py)?;
        let d = PyDict::new(py);
        d.set_item("name", &norm)?;
        match res {
            InfoResult::Dir => {
                d.set_item("size", 0)?;
                d.set_item("type", "directory")?;
                d.set_item("metadata", PyDict::new(py))?;
            }
            InfoResult::File { size, meta } => {
                d.set_item("size", size)?;
                d.set_item("type", "file")?;
                let m = PyDict::new(py);
                for (k, v) in &meta {
                    m.set_item(k, meta_to_py(py, v))?;
                }
                d.set_item("metadata", m)?;
            }
        }
        Ok(d.into_any().unbind())
    }

    fn read<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyBytes>> {
        let data = py.allow_threads(|| self.inner.read(path)).map_err(to_py)?;
        Ok(PyBytes::new(py, &data))
    }

    fn exists(&self, py: Python<'_>, path: &str) -> bool {
        py.allow_threads(|| self.inner.exists(path))
    }

    fn is_dir(&self, py: Python<'_>, path: &str) -> bool {
        py.allow_threads(|| self.inner.is_dir(path))
    }

    fn paths(&self) -> Vec<String> {
        self.inner.paths()
    }

    fn dirs(&self) -> Vec<String> {
        self.inner.dirs()
    }
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<PyArchive>()?;
    m.add_function(wrap_pyfunction!(register_adapter, m)?)?;
    Ok(())
}
