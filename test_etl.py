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

        result = subprocess.run(
            [
                ETL,
                str(tmp_path),
                "-e",
                "extract.sh",
                "-t",
                "transform.sh",
                "-l",
                "loader.sh",
            ],
            capture_output=True,
            text=True,
        )
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

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
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

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh"],
            capture_output=True,
            text=True,
        )
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

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh"],
            capture_output=True,
            text=True,
        )
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

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "-t", "transform.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

        assert (tmp_path / "state" / "ke" / "keep1" / "keep1.run.0").exists()

        skip_dir = tmp_path / "state" / "sk" / "skip1"
        assert not skip_dir.exists(), "Should not have created directory"

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

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "-l", "loader.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 42

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

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

        item_dir = tmp_path / "state" / "ms" / "msg-abc123"
        assert item_dir.is_dir()
        assert (item_dir / "msg-abc123.run.0").exists()

    def test_duplicate_id_uses_first_occurrence(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "dup1 first"
            echo "dup1 second"
            echo "dup1 third"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

        item_dir = tmp_path / "state" / "du" / "dup1"
        assert (item_dir / "dup1.data").read_text() == "first"
        assert (item_dir / "dup1.run.0").exists()

    def test_custom_state_dir(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "--state-dir", "mystate"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "mystate" / "it" / "item1" / "item1.run.0").exists()
        assert not (tmp_path / "state").exists()

    def test_stdin_extractor(self, tmp_path: Path) -> None:
        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "-"],
            input="item1 hello\nitem2 world\n",
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()
        assert (tmp_path / "state" / "it" / "item2" / "item2.run.0").exists()

    def test_default_stdin_with_warning(self, tmp_path: Path) -> None:
        """No -e flag defaults to stdin with a warning."""
        result = subprocess.run(
            [ETL, str(tmp_path)],
            input="item1 data\n",
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "No extractor command passed" in result.stderr
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()

    def test_command_with_pipes(self, tmp_path: Path) -> None:
        """Commands can use shell features like pipes."""
        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "echo 'item1 hello' | cat"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()

    def test_command_finds_script_via_path(self, tmp_path: Path) -> None:
        """Bare script name resolves via PATH=directory:$PATH."""
        write_script(
            tmp_path / "my-extract",
            """\
            #!/usr/bin/env bash
            echo "item1 data"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "my-extract"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()

    def test_null_delimited_extractor(self, tmp_path: Path) -> None:
        """Items with newlines in data using -0."""
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            printf 'item1 line1\\nline2\\0item2 data2\\0'
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "-0"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "it" / "item1" / "item1.data").read_text() == "line1\nline2"
        assert (tmp_path / "state" / "it" / "item2" / "item2.data").read_text() == "data2"

    def test_null_delimited_stdin(self, tmp_path: Path) -> None:
        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "-", "-0"],
            input="item1 hello\0item2 world\0",
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()
        assert (tmp_path / "state" / "it" / "item2" / "item2.run.0").exists()

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

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
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
            [ETL, str(tmp_path), "-e", "extract.sh", "--clean-gt-days", "30"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "ne" / "new1" / "new1.run.0").exists()
        assert not old_dir.exists()

    def test_clean_gt_days_protects_extractor_ids(self, tmp_path: Path) -> None:
        """IDs seen in extractor output should not be cleaned even if old."""
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "old1 data"
            """,
        )

        # Pre-create old state for old1 (same ID the extractor will output)
        old_dir = tmp_path / "state" / "ol" / "old1"
        old_dir.mkdir(parents=True)
        run_file = old_dir / "old1.run.0"
        run_file.write_text("")
        old_time = run_file.stat().st_mtime - 60 * 86400
        os.utime(str(run_file), (old_time, old_time))

        # Pre-create old state for old2 (NOT in extractor output)
        old_dir2 = tmp_path / "state" / "ol" / "old2"
        old_dir2.mkdir(parents=True)
        run_file2 = old_dir2 / "old2.run.0"
        run_file2.write_text("")
        os.utime(str(run_file2), (old_time, old_time))

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "--clean-gt-days", "30"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        # old1 should be protected (extractor mentioned it)
        assert old_dir.exists()
        # old2 should be cleaned (not in extractor output)
        assert not old_dir2.exists()

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
            [
                ETL,
                str(tmp_path),
                "-e",
                "extract.sh",
                "-l",
                "loader.sh",
                "--clean-gt-days",
                "30",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        assert not old_dir.exists()


class TestStreaming:
    def test_streaming_loads_while_extracting(self, tmp_path: Path) -> None:
        """Items are loaded while the extractor is still running."""
        marker = tmp_path / "load_happened"
        write_script(
            tmp_path / "extract.sh",
            f"""\
            #!/usr/bin/env bash
            echo "item1 data1"
            # Give loader time to start
            sleep 0.5
            # Check that loader ran for item1 while we're still extracting
            if [ -f "{tmp_path}/state/it/item1/item1.run.0" ]; then
                touch {marker}
            fi
            echo "item2 data2"
            """,
        )
        write_script(
            tmp_path / "loader.sh",
            """\
            #!/usr/bin/env bash
            cat "$1"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "-l", "loader.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()
        assert (tmp_path / "state" / "it" / "item2" / "item2.run.0").exists()
        assert marker.exists(), "Loader should have run while extractor was still running"


class TestErrorHandling:
    def test_fail_empty_default(self, tmp_path: Path) -> None:
        """Default --fail-empty: error if extractor produces zero valid lines."""
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo ""
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        assert "zero valid lines" in result.stderr

    def test_no_fail_empty(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo ""
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "--no-fail-empty"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

    def test_extractor_partial_output(self, tmp_path: Path) -> None:
        """Non-zero exit still processes complete lines."""
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data1"
            echo "item2 data2"
            exit 3
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh"],
            capture_output=True,
            text=True,
        )
        # Exit code should be the extractor's exit code
        assert result.returncode == 3
        # But items should still be processed
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()
        assert (tmp_path / "state" / "it" / "item2" / "item2.run.0").exists()

    def test_extractor_incomplete_line_discarded(self, tmp_path: Path) -> None:
        """Incomplete trailing line discarded on non-zero exit."""
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            printf "item1 data1\nincomplete"
            exit 1
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()
        assert not (tmp_path / "state" / "in" / "incomplete").exists()

    def test_extractor_incomplete_line_kept_on_success(self, tmp_path: Path) -> None:
        """Incomplete trailing line kept on exit 0."""
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            printf "item1 data1\nitem2 data2"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()
        assert (tmp_path / "state" / "it" / "item2" / "item2.run.0").exists()

    def test_filter_exits_specific_codes(self, tmp_path: Path) -> None:
        """--filter-exits makes only listed codes silent filters."""
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "keep1 data"
            echo "filter1 data"
            echo "error1 data"
            """,
        )
        write_script(
            tmp_path / "transform.sh",
            """\
            #!/usr/bin/env bash
            data=$(cat)
            if [ "$ETL_ID" = "filter1" ]; then exit 99; fi
            if [ "$ETL_ID" = "error1" ]; then exit 42; fi
            echo "$data"
            """,
        )

        result = subprocess.run(
            [
                ETL,
                str(tmp_path),
                "-e",
                "extract.sh",
                "-t",
                "transform.sh",
                "--filter-exits",
                "99",
            ],
            capture_output=True,
            text=True,
        )
        # error1 causes exit 42 which is not in filter list → mirrors that exit code
        assert result.returncode == 42
        # keep1 should be processed
        assert (tmp_path / "state" / "ke" / "keep1" / "keep1.run.0").exists()
        # filter1 silently filtered
        assert not (tmp_path / "state" / "fi" / "filter1").exists()
        # error1 not processed (transformer errored)
        assert not (tmp_path / "state" / "er" / "error1").exists()

    def test_fail_empty_warning_on_nonzero_exit(self, tmp_path: Path) -> None:
        """If extractor exits non-zero with zero lines, it's a warning not double-error."""
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            exit 5
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh"],
            capture_output=True,
            text=True,
        )
        # Should exit with extractor's code, not 1 from fail-empty
        assert result.returncode == 5
        assert "zero valid lines" in result.stderr
        assert "already failed" in result.stderr


class TestDryRun:
    def test_dry_run_no_state_written(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data1"
            echo "item2 data2"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "--dry-run"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert not (tmp_path / "state").exists()

    def test_dry_run_shows_new_entries(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data1"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "--dry-run"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "*NEW ENTRY*" in result.stdout
        assert "item1" in result.stdout

    def test_dry_run_shows_already_exists(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data"
            """,
        )
        item_dir = tmp_path / "state" / "it" / "item1"
        item_dir.mkdir(parents=True)
        (item_dir / "item1.run.0").write_text("")

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "--dry-run", "-v"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "*ALREADY EXISTS*" in result.stdout

    def test_dry_run_shows_filtered(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data"
            """,
        )
        write_script(
            tmp_path / "transform.sh",
            """\
            #!/usr/bin/env bash
            exit 1
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "-t", "transform.sh", "--dry-run"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "*FILTERED OUT*" in result.stdout
        assert not (tmp_path / "state").exists()

    def test_dry_run_shows_would_load(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "-l", "myloader.sh", "--dry-run"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "*WOULD LOAD*" in result.stdout
        assert "myloader.sh" in result.stdout

    def test_dry_run_summary_to_stderr(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data"
            echo "item2 data"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "--dry-run"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "Would load: 2" in result.stderr

    def test_dry_run_does_not_run_collector(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data"
            """,
        )
        marker = tmp_path / "collector_ran"
        write_script(
            tmp_path / "collect.sh",
            f"""\
            #!/usr/bin/env bash
            touch {marker}
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "-c", "collect.sh", "--dry-run"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert not marker.exists()


class TestCollect:
    def test_collector_receives_run_files(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data1"
            echo "item2 data2"
            """,
        )
        write_script(
            tmp_path / "collect.sh",
            """\
            #!/usr/bin/env bash
            echo "$@" > /tmp/etl_collect_test_args
            echo "$ETL_IDS" > /tmp/etl_collect_test_ids
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "-c", "collect.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

        args_file = Path("/tmp/etl_collect_test_args")
        args = args_file.read_text().strip()
        assert "item1.run.0" in args
        assert "item2.run.0" in args

        ids_file = Path("/tmp/etl_collect_test_ids")
        ids = ids_file.read_text().strip().split()
        assert "item1" in ids
        assert "item2" in ids

        args_file.unlink()
        ids_file.unlink()

    def test_collector_not_called_when_no_items(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data"
            """,
        )

        # Pre-create done marker so no new items
        item_dir = tmp_path / "state" / "it" / "item1"
        item_dir.mkdir(parents=True)
        (item_dir / "item1.run.0").write_text("")

        marker = tmp_path / "collector_ran"
        write_script(
            tmp_path / "collect.sh",
            f"""\
            #!/usr/bin/env bash
            touch {marker}
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "-c", "collect.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert not marker.exists(), "Collector should not run when no new items"

    def test_collector_failure(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data"
            """,
        )
        write_script(
            tmp_path / "collect.sh",
            """\
            #!/usr/bin/env bash
            exit 1
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "-c", "collect.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1


class TestParsers:
    def test_json_parser(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo '{"_id": "item1", "name": "foo"}'
            echo '{"_id": "item2", "name": "bar"}'
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "--parser", "._id"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        data1 = (tmp_path / "state" / "it" / "item1" / "item1.data").read_text()
        assert '"_id": "item1"' in data1
        assert (tmp_path / "state" / "it" / "item2" / "item2.run.0").exists()

    def test_json_parser_missing_key(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo '{"name": "foo"}'
            echo '{"_id": "item2", "name": "bar"}'
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "--parser", "._id"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        # First line should be skipped (no _id key)
        assert not (tmp_path / "state" / "na").exists()
        assert (tmp_path / "state" / "it" / "item2" / "item2.run.0").exists()

    def test_json_parser_invalid_json(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo 'not json'
            echo '{"_id": "item1"}'
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "--parser", "._id"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()

    def test_auto_parser_json(self, tmp_path: Path) -> None:
        """Auto parser detects JSON lines by leading { and uses _id."""
        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "-"],
            input='{"_id": "item1", "val": "x"}\n',
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()

    def test_auto_parser_id_data(self, tmp_path: Path) -> None:
        """Auto parser falls back to pop-first-col for non-JSON lines."""
        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "-"],
            input="item1 some data\n",
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "it" / "item1" / "item1.data").read_text() == "some data"

    def test_auto_parser_mixed(self, tmp_path: Path) -> None:
        """Auto parser handles mixed JSON and id-data lines."""
        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "-"],
            input='item1 plain\n{"_id": "item2", "val": "json"}\n',
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()
        assert (tmp_path / "state" / "it" / "item2" / "item2.run.0").exists()


class TestEnvVars:
    def test_transformer_gets_etl_id(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "myid data"
            """,
        )
        write_script(
            tmp_path / "transform.sh",
            """\
            #!/usr/bin/env bash
            echo "id=$ETL_ID"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "-t", "transform.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "my" / "myid" / "myid.data").read_text() == "id=myid\n"

    def test_loader_gets_etl_id(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "myid data"
            """,
        )
        write_script(
            tmp_path / "loader.sh",
            """\
            #!/usr/bin/env bash
            echo "id=$ETL_ID"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh", "-l", "loader.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        run_file = tmp_path / "state" / "my" / "myid" / "myid.run.0"
        assert "id=myid" in run_file.read_text()

    def test_commands_get_etl_state_dir(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "myid $ETL_STATE_DIR"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-e", "extract.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        state_dir = str((tmp_path / "state").resolve())
        data = (tmp_path / "state" / "my" / "myid" / "myid.data").read_text()
        assert data == state_dir


class TestAutoDiscover:
    def test_auto_discover_full_pipeline(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data"
            """,
        )
        write_script(
            tmp_path / "transform.sh",
            """\
            #!/usr/bin/env bash
            cat
            """,
        )
        write_script(
            tmp_path / "loader.sh",
            """\
            #!/usr/bin/env bash
            cat "$1"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-a"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()

    def test_auto_discover_named_scripts(self, tmp_path: Path) -> None:
        write_script(
            tmp_path / "rss.extract.sh",
            """\
            #!/usr/bin/env bash
            echo "e1 email data"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-a"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "e1" / "e1" / "e1.run.0").exists()

    def test_auto_discover_no_extractor_fails(self, tmp_path: Path) -> None:
        result = subprocess.run(
            [ETL, str(tmp_path), "-a"],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0

    def test_auto_discover_multiple_extractors_fails(self, tmp_path: Path) -> None:
        write_script(tmp_path / "extract.sh", "#!/bin/bash\necho 'a b'\n")
        write_script(tmp_path / "email.extract.sh", "#!/bin/bash\necho 'a b'\n")

        result = subprocess.run(
            [ETL, str(tmp_path), "-a"],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0

    def test_auto_discover_explicit_override(self, tmp_path: Path) -> None:
        """Explicit -e overrides discovered extractor."""
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "discovered data"
            """,
        )
        write_script(
            tmp_path / "custom.sh",
            """\
            #!/usr/bin/env bash
            echo "custom1 custom data"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-a", "-e", "custom.sh"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state" / "cu" / "custom1" / "custom1.run.0").exists()

    def test_auto_discover_skip_transformer(self, tmp_path: Path) -> None:
        """+t excludes transformer from auto-discovery."""
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data"
            """,
        )
        write_script(
            tmp_path / "transform.sh",
            """\
            #!/usr/bin/env bash
            echo "TRANSFORMED: $(cat)"
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-a", "+t"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        # Data should be raw, not transformed
        assert (tmp_path / "state" / "it" / "item1" / "item1.data").read_text() == "data"

    def test_auto_discover_skip_loader(self, tmp_path: Path) -> None:
        """+l excludes loader from auto-discovery."""
        write_script(
            tmp_path / "extract.sh",
            """\
            #!/usr/bin/env bash
            echo "item1 data"
            """,
        )
        write_script(
            tmp_path / "loader.sh",
            """\
            #!/usr/bin/env bash
            exit 42
            """,
        )

        result = subprocess.run(
            [ETL, str(tmp_path), "-a", "+l"],
            capture_output=True,
            text=True,
        )
        # Without loader, should succeed (loader would have failed with 42)
        assert result.returncode == 0
        assert (tmp_path / "state" / "it" / "item1" / "item1.run.0").exists()


class TestStatus:
    def test_status_shows_items(self, tmp_path: Path) -> None:
        # Create some state
        for name, code in [("item1", 0), ("item2", 0), ("item3", 1)]:
            shard = name[:2]
            d = tmp_path / "state" / shard / name
            d.mkdir(parents=True)
            (d / f"{name}.run.{code}").write_text("")

        result = subprocess.run(
            [ETL, "status", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "exit 0: 2 item(s)" in result.stdout
        assert "exit 1: 1 item(s)" in result.stdout

    def test_status_success_rate(self, tmp_path: Path) -> None:
        for name, code in [("item1", 0), ("item2", 0), ("item3", 1), ("item4", 0)]:
            shard = name[:2]
            d = tmp_path / "state" / shard / name
            d.mkdir(parents=True)
            (d / f"{name}.run.{code}").write_text("")

        result = subprocess.run(
            [ETL, "status", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        # 3/4 = 75%
        assert "75.0%" in result.stdout

    def test_status_empty_state(self, tmp_path: Path) -> None:
        (tmp_path / "state").mkdir()

        result = subprocess.run(
            [ETL, "status", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "No items" in result.stdout

    def test_status_no_state_dir(self, tmp_path: Path) -> None:
        result = subprocess.run(
            [ETL, "status", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0

    def test_status_custom_state_dir(self, tmp_path: Path) -> None:
        d = tmp_path / "custom" / "it" / "item1"
        d.mkdir(parents=True)
        (d / "item1.run.0").write_text("")

        result = subprocess.run(
            [ETL, "status", str(tmp_path), "--state-dir", "custom"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "exit 0: 1 item(s)" in result.stdout


class TestList:
    def test_list_items(self, tmp_path: Path) -> None:
        for name in ["item1", "item2"]:
            shard = name[:2]
            d = tmp_path / "state" / shard / name
            d.mkdir(parents=True)
            (d / f"{name}.data").write_text("data")
            (d / f"{name}.run.0").write_text("")

        result = subprocess.run(
            [ETL, "list", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        lines = result.stdout.strip().splitlines()
        assert len(lines) == 2
        # Each line: ID DATE PATH
        for line in lines:
            parts = line.split(" ", 2)
            assert len(parts) == 3
            assert parts[0] in ("item1", "item2")
            assert parts[1].endswith("Z")
            assert ".data" in parts[2]

    def test_list_empty_state(self, tmp_path: Path) -> None:
        (tmp_path / "state").mkdir()
        result = subprocess.run(
            [ETL, "list", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert result.stdout.strip() == ""

    def test_list_custom_state_dir(self, tmp_path: Path) -> None:
        d = tmp_path / "custom" / "it" / "item1"
        d.mkdir(parents=True)
        (d / "item1.data").write_text("data")

        result = subprocess.run(
            [ETL, "list", str(tmp_path), "--state-dir", "custom"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "item1" in result.stdout

    def test_list_skips_items_without_data(self, tmp_path: Path) -> None:
        """Items with .run but no .data file are skipped."""
        d = tmp_path / "state" / "it" / "item1"
        d.mkdir(parents=True)
        (d / "item1.run.0").write_text("")

        result = subprocess.run(
            [ETL, "list", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert result.stdout.strip() == ""

    def test_list_as_extractor_input(self, tmp_path: Path) -> None:
        """etl list output can be piped as extractor input to another pipeline."""
        # Create first pipeline's state
        d = tmp_path / "state" / "it" / "item1"
        d.mkdir(parents=True)
        (d / "item1.data").write_text("hello world")
        (d / "item1.run.0").write_text("")

        # Use etl list as extractor for a second pipeline with separate state
        result = subprocess.run(
            [
                ETL,
                str(tmp_path),
                "-e",
                f"{ETL} list {tmp_path}",
                "--state-dir",
                "state2",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert (tmp_path / "state2" / "it" / "item1" / "item1.run.0").exists()


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

    def test_clean_custom_state_dir(self, tmp_path: Path) -> None:
        """Clean uses --state-dir to find state directory."""
        state = tmp_path / "mystate" / "ab" / "abc"
        state.mkdir(parents=True)
        f = state / "abc.run.0"
        f.write_text("")
        old_time = f.stat().st_mtime - 60 * 86400
        os.utime(str(f), (old_time, old_time))

        result = subprocess.run(
            [ETL, "clean", str(tmp_path), "--days", "30", "--state-dir", "mystate"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert not state.exists()

    def test_clean_dry_run(self, tmp_path: Path) -> None:
        """--dry-run shows what would be cleaned without removing."""
        state = tmp_path / "state" / "ab" / "abc123"
        state.mkdir(parents=True)
        data_file = state / "abc123.data"
        run_file = state / "abc123.run.0"
        data_file.write_text("data")
        run_file.write_text("")
        old_time = os.path.getmtime(str(data_file)) - 60 * 86400
        os.utime(str(data_file), (old_time, old_time))
        os.utime(str(run_file), (old_time, old_time))

        result = subprocess.run(
            [ETL, "clean", str(tmp_path), "--days", "30", "--dry-run"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "Would clean 1 item(s)" in result.stdout
        assert state.exists()

    def test_clean_dry_run_verbose(self, tmp_path: Path) -> None:
        """--dry-run -v shows individual paths."""
        state = tmp_path / "state" / "ab" / "abc123"
        state.mkdir(parents=True)
        f = state / "abc123.run.0"
        f.write_text("")
        old_time = f.stat().st_mtime - 60 * 86400
        os.utime(str(f), (old_time, old_time))

        result = subprocess.run(
            [ETL, "clean", str(tmp_path), "--days", "30", "--dry-run", "-v"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "abc123" in result.stdout
