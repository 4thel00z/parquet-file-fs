"""Regression tests for the release automation metadata.

Guards against the release pipeline shipping a tag whose Cargo.lock still
pins the previous version: from such a commit every ``cargo build/test
--locked`` hard-fails, and unlocked builds silently diverge from the
committed lockfile.

These tests only parse committed files; they do not require the built
extension module.
"""

import json
import re
import sys
from pathlib import Path

import pytest
import yaml

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - exercised on the 3.9/3.10 CI legs
    tomllib = pytest.importorskip(
        "tomli", reason="TOML parsing needs tomllib (3.11+) or tomli"
    )

REPO = Path(__file__).resolve().parent.parent
WORKFLOW = REPO / ".github" / "workflows" / "release-please.yaml"


def _package_version() -> str:
    manifest = tomllib.loads((REPO / "Cargo.toml").read_text())
    return manifest["package"]["version"]


class TestVersionSync:
    def test_lockfile_pins_current_version(self) -> None:
        """Cargo.lock matches Cargo.toml.

        A mismatch is exactly the state a release tag ends up in when the
        version bump lands without a lockfile regeneration.
        """
        version = _package_version()
        lock = tomllib.loads((REPO / "Cargo.lock").read_text())
        pinned = [
            pkg["version"] for pkg in lock["package"] if pkg["name"] == "parquet-file-fs"
        ]
        assert pinned, "parquet-file-fs not found in Cargo.lock"
        assert pinned == [version], (
            f"Cargo.lock pins {pinned} but Cargo.toml is at {version}; "
            "run `cargo update --workspace` and commit the lockfile"
        )

    def test_python_and_rust_versions_agree(self) -> None:
        """The wheel version and the crate version are one release."""
        pyproject = tomllib.loads((REPO / "pyproject.toml").read_text())
        assert pyproject["project"]["version"] == _package_version()

    def test_manifest_tracks_current_version(self) -> None:
        manifest = json.loads((REPO / ".release-please-manifest.json").read_text())
        assert manifest["."] == _package_version()

    def test_version_marker_wiring(self) -> None:
        """extra-files bumps Cargo.toml via the inline marker."""
        config = json.loads((REPO / "release-please-config.json").read_text())
        assert "Cargo.toml" in config["packages"]["."]["extra-files"]
        assert re.search(
            r'^version = "[^"]+" # x-release-please-version$',
            (REPO / "Cargo.toml").read_text(),
            flags=re.MULTILINE,
        ), "Cargo.toml lost its x-release-please-version marker"


class TestReleaseWorkflow:
    def test_workflow_syncs_lockfile_on_release_pr(self) -> None:
        """release-please's updater only rewrites Cargo.toml; without this job
        the tagged release commit keeps the stale lockfile."""
        job = yaml.safe_load(WORKFLOW.read_text())["jobs"]["sync-lockfile"]
        assert "prs_created" in job.get("if", ""), (
            "the lockfile sync job must be gated on the release PR"
        )
        job_text = yaml.dump(job)
        assert "cargo update --workspace" in job_text
        assert "Cargo.lock" in job_text

    def test_publish_job_uses_trusted_publishing(self) -> None:
        job = yaml.safe_load(WORKFLOW.read_text())["jobs"]["publish-pypi"]
        assert job.get("permissions", {}).get("id-token") == "write", (
            "publish-pypi is missing `permissions: id-token: write`, "
            "required for PyPI Trusted Publishing"
        )
        steps = job["steps"]
        assert any("pypa/gh-action-pypi-publish" in s.get("uses", "") for s in steps)
        # An API-token fallback would defeat the point of OIDC publishing.
        assert "password" not in yaml.dump(steps)

    def test_manual_dispatch_survives_a_failed_gate(self) -> None:
        """workflow_dispatch is the documented recovery path, so the publish
        jobs must not be skipped just because the release-please job failed
        (e.g. PR creation blocked by repo policy). Without `always()`, `needs`
        short-circuits and the escape hatch escapes nothing."""
        jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
        for name in ("pypi-wheels", "pypi-sdist", "publish-pypi"):
            condition = jobs[name]["if"]
            assert "always()" in condition, f"{name} would be skipped by a failed gate"
            assert "workflow_dispatch" in condition, name

    def test_publish_still_requires_successful_builds(self) -> None:
        """`always()` must not let a partial dist reach PyPI: a failed wheel or
        sdist build has to block the upload."""
        condition = yaml.safe_load(WORKFLOW.read_text())["jobs"]["publish-pypi"]["if"]
        assert "needs.pypi-wheels.result == 'success'" in condition
        assert "needs.pypi-sdist.result == 'success'" in condition

    def test_publish_waits_for_wheels_and_sdist(self) -> None:
        """PyPI rejects a project whose first upload lacks an sdist, and a
        partial publish cannot be re-run cleanly — so both must be built
        before the publish job starts."""
        jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
        assert set(jobs["publish-pypi"]["needs"]) >= {"pypi-wheels", "pypi-sdist"}
        assert jobs["pypi-sdist"]["steps"][-2]["with"]["command"] == "sdist"
