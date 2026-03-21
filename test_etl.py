"""Tests for etl pipeline."""

import os
import stat
import subprocess
import textwrap
import types
from pathlib import Path


ETL = str(Path(__file__).parent / "etl.py")


def load_etl_module() -> types.ModuleType:
    import importlib.util

    spec = importlib.util.spec_from_file_location("etl_mod", ETL)
    assert spec is not None
    assert spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def write_script(path: Path, content: str) -> None:
    path.write_text(textwrap.dedent(content))
    path.chmod(path.stat().st_mode | stat.S_IEXEC)


# === Unit tests for core ===


def test_sanitize_id() -> None:
    mod = load_etl_module()

    assert mod.sanitize_id("msg-abc@123!") == "msg-abc123"
    assert mod.sanitize_id("simple123") == "simple123"
    assert mod.sanitize_id("my_item") == "my_item"
    assert mod.sanitize_id("with-dash") == "with-dash"
    assert mod.sanitize_id("mixed_id-123") == "mixed_id-123"
    assert mod.sanitize_id("@@@") == ""
    assert mod.sanitize_id("A.B.C") == "ABC"


def test_parse_extractor_line() -> None:
    mod = load_etl_module()

    assert mod.parse_extractor_line("id123 some data here") == (
        "id123",
        "some data here",
    )
    assert mod.parse_extractor_line("id123\tdata") == ("id123", "data")
    assert mod.parse_extractor_line("idonly") == ("idonly", "")

    assert mod.parse_extractor_line(" test") is None
    assert mod.parse_extractor_line(" data with several words and no id") is None
    assert mod.parse_extractor_line("\tdata no id") is None
    assert mod.parse_extractor_line("") is None


# === Integration tests ===


class TestRunPipeline:
    def test_full_pipeline(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 hello world"
            echo "item2 goodbye world"
            """,
        )
        write_script(
            tmp_path / "transform.sh",
            """\
            #!/usr/bin/env bash
            data=$(cat)
            echo "TRANSFORMED: $data"
            """,
        )
        write_script(
            tmp_path / "loader.sh",
            """\
            #!/usr/bin/env bash
            cat "$1"
            echo "LOADED"
            """,
        )

        result = subprocess.run([ETL, str(tmp_path)], capture_output=True, text=True)
        assert result.returncode == 0

        # Check state was created
        state = tmp_path / "state"
        assert state.is_dir()

        # item1 → shard "it"
        item1_dir = state / "it" / "item1"
        assert item1_dir.is_dir()
        assert (item1_dir / "item1.data").read_text() == "TRANSFORMED: hello world\n"
        assert (item1_dir / "item1.run.0").exists()

        # item2 → shard "it"
        item2_dir = state / "it" / "item2"
        assert item2_dir.is_dir()
        assert (item2_dir / "item2.data").read_text() == "TRANSFORMED: goodbye world\n"
        assert (item2_dir / "item2.run.0").exists()

    def test_dedup_skips_existing(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data"
            """,
        )

        # Pre-create the run.0 marker
        item_dir = tmp_path / "state" / "it" / "item1"
        item_dir.mkdir(parents=True)
        (item_dir / "item1.run.0").write_text("already done")

        # No loader — if dedup works, nothing happens
        result = subprocess.run([ETL, str(tmp_path)], capture_output=True, text=True)
        assert result.returncode == 0
        # run.0 should still have original content
        assert (item_dir / "item1.run.0").read_text() == "already done"
        assert not (item_dir / "item1.data").exists(), "Should not create data entry"

    def test_id_only_extraction(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "alertid"
            """,
        )

        result = subprocess.run([ETL, str(tmp_path)], capture_output=True, text=True)
        assert result.returncode == 0

        item_dir = tmp_path / "state" / "al" / "alertid"
        assert (item_dir / "alertid.data").read_text() == ""
        assert (item_dir / "alertid.run.0").exists()

    def test_no_transformer_no_loader(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "abc123 raw data here"
            """,
        )

        result = subprocess.run([ETL, str(tmp_path)], capture_output=True, text=True)
        assert result.returncode == 0

        item_dir = tmp_path / "state" / "ab" / "abc123"
        assert (item_dir / "abc123.data").read_text() == "raw data here"
        assert (item_dir / "abc123.run.0").exists()

    def test_transformer_filter(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "keep1 good"
            echo "skip1 bad"
            """,
        )
        write_script(
            tmp_path / "transform.sh",
            """\
            #!/usr/bin/env bash
            data=$(cat)
            if [ "$data" = "bad" ]; then exit 1; fi
            echo "$data"
            """,
        )

        result = subprocess.run([ETL, str(tmp_path)], capture_output=True, text=True)
        assert result.returncode == 0

        # keep1 should be processed
        assert (tmp_path / "state" / "ke" / "keep1" / "keep1.run.0").exists()

        # skip1 should have no run file (filtered by transformer)
        skip_dir = tmp_path / "state" / "sk" / "skip1"
        assert not skip_dir.exists(), "Should not have created directory"
        assert not (skip_dir / "skip1.run.0").exists()

    def test_loader_failure(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "fail1 data"
            """,
        )
        write_script(
            tmp_path / "loader.sh",
            """\
            #!/usr/bin/env bash
            echo "error happened" >&2
            exit 42
            """,
        )

        result = subprocess.run([ETL, str(tmp_path)], capture_output=True, text=True)
        assert result.returncode == 1

        item_dir = tmp_path / "state" / "fa" / "fail1"
        assert not (item_dir / "fail1.run.0").exists()
        assert (item_dir / "fail1.run.42").exists()
        assert "error happened" in (item_dir / "fail1.run.42").read_text()

    def test_id_sanitization(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "msg-abc@123! some data"
            """,
        )

        result = subprocess.run([ETL, str(tmp_path)], capture_output=True, text=True)
        assert result.returncode == 0

        item_dir = tmp_path / "state" / "ms" / "msg-abc123"
        assert item_dir.is_dir()
        assert (item_dir / "msg-abc123.run.0").exists()

    def test_no_extractor_fails(self, tmp_path: Path) -> None:
        result = subprocess.run([ETL, str(tmp_path)], capture_output=True, text=True)
        assert result.returncode != 0

    def test_multiple_extractors_fails(self, tmp_path: Path) -> None:
        write_script(tmp_path / "extract.sh", "#!/bin/bash\n")
        write_script(tmp_path / "email.extract.sh", "#!/bin/bash\n")

        result = subprocess.run([ETL, str(tmp_path)], capture_output=True, text=True)
        assert result.returncode != 0

    def test_named_extract_script(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "email.extract.sh",
            """\
            #!/usr/bin/env bash
            echo "e1 email data"
            """,
        )

        result = subprocess.run([ETL, str(tmp_path)], capture_output=True, text=True)
        assert result.returncode == 0
        assert (tmp_path / "state" / "e1" / "e1" / "e1.run.0").exists()

    def test_retry_failed_item(self, tmp_path: Path) -> None:
        """Failed items (run.<non-zero>) should be retried on next run."""
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "retry1 data"
            """,
        )

        # Pre-create a failed run marker
        item_dir = tmp_path / "state" / "re" / "retry1"
        item_dir.mkdir(parents=True)
        (item_dir / "retry1.run.1").write_text("previous failure")

        # No run.0 exists, so item should be retried
        result = subprocess.run([ETL, str(tmp_path)], capture_output=True, text=True)
        assert result.returncode == 0
        # Now run.0 should exist
        assert (item_dir / "retry1.run.0").exists()

    def test_clean_gt_days_removes_old_state(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "new1 data"
            """,
        )

        # Pre-create old state that should be cleaned
        old_dir = tmp_path / "state" / "ol" / "old1"
        old_dir.mkdir(parents=True)
        run_file = old_dir / "old1.run.0"
        run_file.write_text("")
        old_time = run_file.stat().st_mtime - 60 * 86400
        os.utime(str(run_file), (old_time, old_time))

        result = subprocess.run(
            [ETL, str(tmp_path), "--clean-gt-days", "30"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        # New item should be processed
        assert (tmp_path / "state" / "ne" / "new1" / "new1.run.0").exists()
        # Old item should be cleaned
        assert not old_dir.exists()

    def test_clean_gt_days_runs_after_loader_failure(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "fail1 data"
            """,
        )
        write_script(
            tmp_path / "loader.sh",
            """\
            #!/usr/bin/env bash
            exit 1
            """,
        )

        # Pre-create old state
        old_dir = tmp_path / "state" / "ol" / "old1"
        old_dir.mkdir(parents=True)
        run_file = old_dir / "old1.run.0"
        run_file.write_text("")
        old_time = run_file.stat().st_mtime - 60 * 86400
        os.utime(str(run_file), (old_time, old_time))

        result = subprocess.run(
            [ETL, str(tmp_path), "--clean-gt-days", "30"],
            capture_output=True,
            text=True,
        )
        # Pipeline failed but clean should still have run
        assert result.returncode == 1
        assert not old_dir.exists()


class TestClean:
    def test_clean_old_items(self, tmp_path: Path) -> None:
        poller = tmp_path / "mypoller"
        poller.mkdir()
        state = poller / "state" / "ab" / "abc123"
        state.mkdir(parents=True)

        data_file = state / "abc123.data"
        run_file = state / "abc123.run.0"
        data_file.write_text("data")
        run_file.write_text("")

        # Set mtime to 60 days ago
        old_time = os.path.getmtime(str(data_file)) - 60 * 86400
        os.utime(str(data_file), (old_time, old_time))
        os.utime(str(run_file), (old_time, old_time))

        result = subprocess.run(
            [ETL, "clean", str(poller), "--days", "30"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert not state.exists()
        # Shard dir should still exist
        assert not (poller / "state" / "ab").exists()

    def test_clean_keeps_recent(self, tmp_path: Path) -> None:
        state = tmp_path / "state" / "ab" / "abc123"
        state.mkdir(parents=True)
        (state / "abc123.data").write_text("data")
        (state / "abc123.run.0").write_text("")

        result = subprocess.run(
            [ETL, "clean", str(tmp_path), "--days", "30"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert state.exists()

    def test_clean_recursive(self, tmp_path: Path) -> None:
        """Clean walks recursively and finds all state/ dirs."""
        for name in ["poller1", "poller2"]:
            state = tmp_path / name / "state" / "ab" / "abc"
            state.mkdir(parents=True)
            f = state / "abc.run.0"
            f.write_text("")
            old_time = f.stat().st_mtime - 60 * 86400
            os.utime(str(f), (old_time, old_time))

        result = subprocess.run(
            [ETL, "clean", str(tmp_path), "--days", "30"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert not (tmp_path / "poller1" / "state" / "ab" / "abc").exists()
        assert not (tmp_path / "poller2" / "state" / "ab" / "abc").exists()
