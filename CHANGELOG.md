# Changelog

## [0.2.0](https://github.com/4thel00z/parquet-file-fs/compare/v0.1.0...v0.2.0) (2026-07-24)


### Features

* archive content reads with LRU cache and lazy sizes/metadata ([80411e4](https://github.com/4thel00z/parquet-file-fs/commit/80411e407df748e7fcd9270f3e66cfcac1623b21))
* fsspec ParquetFileSystem with index-only glob/find ([9d7aae3](https://github.com/4thel00z/parquet-file-fs/commit/9d7aae37451c180f92912a8dda0f3d70c56d327d))
* http e2e test, README, CI workflow ([39d1026](https://github.com/4thel00z/parquet-file-fs/commit/39d1026724b1262938ec3720ea5d01a382586bbf))
* native s3/http adapter via object_store ([00c79df](https://github.com/4thel00z/parquet-file-fs/commit/00c79df367a4596ab0659ddb6c5f737658273dbe))
* parquet ChunkReader over RangeReader adapters ([5a2a522](https://github.com/4thel00z/parquet-file-fs/commit/5a2a522a2da40fda6bc2ae6f7c7d5f93f01500c2))
* PyO3 bindings for Archive and adapter registration ([3b00ada](https://github.com/4thel00z/parquet-file-fs/commit/3b00ada17ae38b024c8652c896749755a56a4e65))
* python adapter registry with fsspec shim ([effd8b3](https://github.com/4thel00z/parquet-file-fs/commit/effd8b390447e6f05e0a77345644aed596cc8c11))
* RangeReader trait, LocalAdapter, adapter registry ([6aeee51](https://github.com/4thel00z/parquet-file-fs/commit/6aeee51b7997ceee2a4b6c83e31c5e4dd1e93a20))
* scaffold maturin mixed project ([446aeb9](https://github.com/4thel00z/parquet-file-fs/commit/446aeb91ad58ab1439a7e08d96c92f7eed9d41a7))
* shard index with column detection, dir tree, duplicate policies ([b85e7ab](https://github.com/4thel00z/parquet-file-fs/commit/b85e7ab854772d2b090a1090777059a06dab429e))


### Bug Fixes

* canonicalize virtual paths and dedupe file/dir name clashes in listings ([aaac958](https://github.com/4thel00z/parquet-file-fs/commit/aaac95892cc469d04963de9327a5cbc89cbe97e4))
* **ci:** let manual dispatch publish when the release PR gate fails ([bd5894a](https://github.com/4thel00z/parquet-file-fs/commit/bd5894a98d7d1e595c9838eae1cfc7dfaf1e74a3))
* surface temporal/nested extra columns in metadata instead of dropping them ([ea61d62](https://github.com/4thel00z/parquet-file-fs/commit/ea61d62c3a364efce3819fbf5b425e21a510f8c8))


### Documentation

* add logo, sharpen description and keywords ([4ce603a](https://github.com/4thel00z/parquet-file-fs/commit/4ce603aba32ac6c4d738abf4e0958fc545addcd0))
* add parquet-file-fs implementation plan ([9e7f6b7](https://github.com/4thel00z/parquet-file-fs/commit/9e7f6b74eb6ff66e2a6b3ced06f1bb9a4a050c3c))
