# Changelog

## [0.3.0](https://github.com/4thel00z/parquet-file-fs/compare/v0.2.0...v0.3.0) (2026-07-26)


### Features

* 7z and rar archive readers (rar behind a default-on cargo feature) ([d247cde](https://github.com/4thel00z/parquet-file-fs/commit/d247cded8a53c6bf048ba6e8ec77151fb7b91050))
* core pack writer and pack_files ([33161b0](https://github.com/4thel00z/parquet-file-fs/commit/33161b07ea7a5c126c344019f17a5c08499d1d24))
* create parquet archive shards from globs and archive files (pfs pack / pack_archive) ([8ef5d91](https://github.com/4thel00z/parquet-file-fs/commit/8ef5d91b1ecdacd7788ace9cdbf13d1ac2f65476))
* pack_archive with magic-byte detection for zip and tar family ([001e179](https://github.com/4thel00z/parquet-file-fs/commit/001e17980c11288ac8f04a5280e955ed41137690))
* pack_glob with root inference and directory shorthand ([bcc2766](https://github.com/4thel00z/parquet-file-fs/commit/bcc2766fcf1224c59fc91e5b3fc0fc16f7838c8c))
* pfs CLI with pack and pack-archive subcommands ([bc200d1](https://github.com/4thel00z/parquet-file-fs/commit/bc200d138d647acf5294951f4e25d831f7e7f6b2))
* python pack and pack_archive API ([5543fbc](https://github.com/4thel00z/parquet-file-fs/commit/5543fbcbef242a6e053d4c8ec9257e62e69bb742))


### Bug Fixes

* address PR review — non-lossy temp path, close handle before unlink, document 7z ordering ([c441bea](https://github.com/4thel00z/parquet-file-fs/commit/c441bea65542421c29e4850de48f56ec71d9bc1e))
* never touch existing output when archive has no files ([001396c](https://github.com/4thel00z/parquet-file-fs/commit/001396c81014aebb1c3e40427cc89858d7526b71))
* propagate first 7z entry error and halt iteration ([3811bcf](https://github.com/4thel00z/parquet-file-fs/commit/3811bcffd12374ce77dc149a128330da7c02e6a5))
* write pack output via temp file + atomic rename ([7758ee8](https://github.com/4thel00z/parquet-file-fs/commit/7758ee8bebb4ea542d48cf223c5971caeb452c97))


### Documentation

* add 7z as a first-class pack_archive format ([4a0eb1e](https://github.com/4thel00z/parquet-file-fs/commit/4a0eb1e380571b1d2e5347abce8c71625fd27261))
* add design spec for pack (create archives from glob/zip) ([fb09642](https://github.com/4thel00z/parquet-file-fs/commit/fb0964228a3acab8c138b7045f00c7e731bf6994))
* correct CLI install command to git install ([c0b892e](https://github.com/4thel00z/parquet-file-fs/commit/c0b892e8261d283a22238db376c0384ad5395ce2))
* creating archives with pfs pack / pack_archive ([e89d20e](https://github.com/4thel00z/parquet-file-fs/commit/e89d20e750d58f87079a95578e4fe6037d473a08))
* generalize pack spec to multi-format pack_archive, drop source-type magic ([1e63d00](https://github.com/4thel00z/parquet-file-fs/commit/1e63d005bcfc41043ff22a8c3fd5f25fd6ce7d7b))
* implementation plan for pack/pack_archive ([9c46364](https://github.com/4thel00z/parquet-file-fs/commit/9c46364ce037a558ce6d62a73a56dabe6a4d0bfd))
* note 7z empty-file ordering in pack_7z_entries ([46ab977](https://github.com/4thel00z/parquet-file-fs/commit/46ab977f2f429eb2c7374e65b8cdb739a77a004d))

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
