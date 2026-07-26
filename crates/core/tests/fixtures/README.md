# Test fixtures

## `simple.rar`

Not checked in: this development environment could install the `rar` CLI
(`brew install --cask rar`) but could not *run* it — every invocation (even
bare `rar --version` with stdin redirected from `/dev/null`) hung
indefinitely. `spctl -a -v` reported the freshly installed binary as
`rejected`; removing the `com.apple.quarantine` xattr and re-signing it
ad hoc (`codesign --force --sign -`) did not help, and sampling the hung
process (`sample <pid>`) showed it parked in `_dyld_start`, i.e. it never
even reached `main`. This reproduced with the sandboxed bash tool disabled
too, so it is specific to this machine/OS combination
(macOS 26.4 on this host), not the agent's sandboxing.

Because RAR's compressor is proprietary (no open-source or `p7zip`
alternative can *write* `.rar`), the fixture could not be generated here.
The round-trip test that depends on it is marked
`#[ignore = "requires fixtures/simple.rar (see fixtures/README.md)"]`.

To generate the fixture on a machine with a working `rar` binary:

```bash
command -v rar || brew install rar   # rarlab cask; metalbrew works too
cd "$(mktemp -d)"
printf 'alpha' > a.txt
mkdir sub && printf 'beta' > sub/b.bin
rar a simple.rar a.txt sub/b.bin
cp simple.rar <repo>/crates/core/tests/fixtures/
```

Then remove the `#[ignore]` attribute from `rar_roundtrip_from_fixture` in
`crates/core/tests/pack_archive_test.rs`.
