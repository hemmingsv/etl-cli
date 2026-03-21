#!/usr/bin/env python3
"""etl — extract, deduplicate, transform, load pipeline."""

import argparse
import asyncio
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from glob import glob
from pathlib import Path

log = logging.getLogger("etl")

# === Flow ===


def main() -> int:
    args = parse_args()
    setup_logging(args.verbose)

    if args.command == "clean":
        return cmd_clean(Path(args.directory), args.days)

    return cmd_run(
        directory=Path(args.directory),
        extractor=args.extractor,
        transformer=args.transformer,
        loader=args.loader,
        clean_gt_days=args.clean_gt_days,
        parallel=args.parallel,
    )


def cmd_run(
    directory: Path,
    extractor: str | None,
    transformer: str | None,
    loader: str | None,
    clean_gt_days: int | None = None,
    parallel: int = 10,
) -> int:
    directory = directory.resolve()
    if not directory.is_dir():
        log.error("Not a directory: %s", directory)
        return 1

    ext = extractor or discover_script(directory, "extract.sh")
    if ext is None:
        log.error("No extractor found in %s", directory)
        return 1

    tfm = transformer or discover_script(directory, "transform.sh", required=False)
    ldr = loader or discover_script(directory, "loader.sh", required=False)

    log.info("Extracting with %s", ext)
    lines = run_extractor(ext)
    if lines is None:
        return 1

    state_dir = directory / "state"
    new_items = extract_and_dedup(lines, state_dir)

    if not new_items:
        log.warning("No new items")
        if clean_gt_days is not None:
            cmd_clean(directory, clean_gt_days)
        return 0

    log.info("Processing %d new item(s)", len(new_items))

    # Transform synchronously (may filter items), then load in parallel
    to_load: list[tuple[str, Path]] = []
    for item_id, raw_data, item_dir in new_items:
        data = transform_item(item_id, raw_data, tfm)
        if data is None:
            continue
        item_dir.mkdir(parents=True, exist_ok=True)
        data_file = item_dir / f"{item_id}.data"
        data_file.write_text(data)
        to_load.append((item_id, item_dir))

    failures = asyncio.run(load_items(to_load, ldr, max_concurrent=parallel))

    if clean_gt_days is not None:
        cmd_clean(directory, clean_gt_days)

    if failures:
        log.warning("%d item(s) failed", failures)
        return 1
    return 0


def cmd_clean(directory: Path, days: int) -> int:
    directory = directory.resolve()
    if not directory.is_dir():
        log.error("Not a directory: %s", directory)
        return 1

    threshold = time.time() - days * 86400
    removed = 0

    for state_dir in find_state_dirs(directory):
        removed += clean_state_dir(state_dir, threshold)

    log.info("Removed %d item folder(s)", removed)
    return 0


# === Pipeline ===


def run_extractor(extractor: str) -> list[str] | None:
    result = subprocess.run(
        [extractor],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        log.error(
            "Extractor failed (exit %d): %s", result.returncode, result.stderr.strip()
        )
        return None
    return result.stdout.splitlines()


def extract_and_dedup(
    lines: list[str],
    state_dir: Path,
) -> list[tuple[str, str, Path]]:
    new_items: list[tuple[str, str, Path]] = []

    for line in lines:
        parsed = parse_extractor_line(line)
        if parsed is None:
            log.warning("Skipping malformed line: %s", line[:80])
            continue

        raw_id, data = parsed
        item_id = sanitize_id(raw_id)
        if not item_id:
            log.warning("Skipping empty ID after sanitization: %s", raw_id)
            continue

        shard = item_id[:2]
        item_dir = state_dir / shard / item_id

        if is_done(item_dir, item_id):
            continue

        new_items.append((item_id, data, item_dir))

    return new_items


def transform_item(
    item_id: str,
    raw_data: str,
    transformer: str | None,
) -> str | None:
    if not transformer:
        return raw_data

    result = subprocess.run(
        [transformer],
        input=raw_data,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        log.info("Transformer rejected %s (exit %d)", item_id, result.returncode)
        return None
    return result.stdout


async def load_items(
    items: list[tuple[str, Path]],
    loader: str | None,
    max_concurrent: int = 10,
) -> int:
    if not loader:
        for item_id, item_dir in items:
            run_file = item_dir / f"{item_id}.run.0"
            run_file.write_text("")
        return 0

    semaphore = asyncio.Semaphore(max_concurrent)

    async def _load_one(item_id: str, item_dir: Path) -> bool:
        async with semaphore:
            data_file = item_dir / f"{item_id}.data"
            log.info("Loading %s", item_id)
            proc = await asyncio.create_subprocess_exec(
                loader,
                str(data_file),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            stdout, _ = await proc.communicate()
            assert proc.returncode is not None
            output = stdout.decode() if stdout else ""
            run_file = item_dir / f"{item_id}.run.{proc.returncode}"
            run_file.write_text(output)
            if proc.returncode != 0:
                log.warning("Loader failed for %s (exit %d)", item_id, proc.returncode)
                return False
            return True

    results = await asyncio.gather(*[_load_one(iid, idir) for iid, idir in items])
    failures = sum(1 for ok in results if not ok)
    return failures


# === Core ===


def parse_extractor_line(line: str) -> tuple[str, str] | None:
    if not line or line[0].isspace():
        return None
    parts = line.split(None, 1)
    if not parts:
        return None
    return parts[0], parts[1] if len(parts) > 1 else ""


def sanitize_id(raw_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_\-]", "", raw_id)


def is_done(item_dir: Path, item_id: str) -> bool:
    return (item_dir / f"{item_id}.run.0").exists()


# === Script discovery ===


def discover_script(directory: Path, pattern: str, required: bool = True) -> str | None:
    matches = glob(str(directory / f"*{pattern}"))
    matches = [m for m in matches if os.access(m, os.X_OK)]

    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise SystemExit(f"Multiple matches for *{pattern} in {directory}: {matches}")
    if required:
        log.error("No executable *%s found in %s", pattern, directory)
        return None
    return None


# === Clean ===


def find_state_dirs(directory: Path) -> list[Path]:
    result = []
    for root, dirs, _files in os.walk(directory):
        if Path(root).name == "state":
            result.append(Path(root))
            dirs.clear()  # don't recurse into state/
    return result


def clean_state_dir(state_dir: Path, threshold: float) -> int:
    removed = 0
    for shard_dir in state_dir.iterdir():
        if not shard_dir.is_dir():
            continue
        for item_dir in shard_dir.iterdir():
            if not item_dir.is_dir():
                continue
            if all_files_older_than(item_dir, threshold):
                shutil.rmtree(item_dir)
                removed += 1
        # Remove empty shard dirs
        if not any(shard_dir.iterdir()):
            shard_dir.rmdir()
    return removed


def all_files_older_than(directory: Path, threshold: float) -> bool:
    files = list(directory.iterdir())
    if not files:
        return True
    return all(f.stat().st_mtime < threshold for f in files if f.is_file())


# === CLI ===


HELP_TEXT = """\
usage: etl [directory] [options]
       etl clean [directory] --days <n>

Extract, deduplicate, transform, load pipeline.

EXAMPLES
  etl
      Run etl in the current directory. Looks for extract.sh,
      transform.sh, loader.sh automatically.

  etl /path/to/poller
      Run etl in a specific directory.

  etl --extractor ./extract.sh --loader ./loader.sh
      Override auto-discovered scripts.

  etl clean /path/to/pollrc.d --days 30
      Remove all state older than 30 days recursively.

  etl /path/to/poller --clean-gt-days 30
      Run pipeline and clean state older than 30 days.

OPTIONS
  [directory]               Operating directory (default: current directory)
  --extractor PATH          Path to extractor script
  --transformer PATH        Path to transformer script
  --loader PATH             Path to loader script
  --parallel N              Max parallel loaders (default: 10)
  --clean-gt-days N         Clean state older than N days after running
  -v, --verbose             Enable debug logging
  -h, --help                Show this help message

CONCEPTS
  etl is a pipeline with four stages: extract, deduplicate,
  transform, load. The later stages are optional, while extract and
  deduplicate are mandatory.

  The extractor outputs lines to stdout in the format:
      <id> [data]

  IDs are sanitized (only alphanumeric, underscore, and dash are
  kept) and used to create state directories for deduplication.

  The power of etl comes from separation of concern. You only have
  to worry about building an extractor, then you slap etl on top of
  it and you get deduplication (the extractor needs no logic for
  this) and if you slap cron on top of etl, you have a polling
  pipeline.

  State lives in a state/ folder inside the operating directory.
  Items are sharded by the first two characters of the sanitized ID.

  The presence of <id>.run.0 marks an item as done. Failed items
  (any <id>.run.<non-zero>) are retried on the next run.

  Clearly for this to work, you have to write extractors that have
  consistent id outputs for same item. E.g. a scraper could use the
  ids used on the websie, rather than generating uuids on the fly.

  Transformers can act as filters by exiting non-zero for items
  that should be ignored. Filtered items leave no trace and will
  be retried every run.

  Loaders run in parallel (up to 10 at a time by default).

FULL EXAMPLE
  An RSS alert poller. Only items with "ALERT:" in the title
  are kept; matching items are emailed to root.

  /home/user/pollers/rss/extract.sh:
      #!/usr/bin/env bash
      curl -s https://example.com/feed.xml \\
          | xmlstarlet sel -t -m '//item' \\
              -v 'concat(guid, " ", title)' -n

  /home/user/pollers/rss/transform.sh:
      #!/usr/bin/env bash
      # Reads the title from stdin. Only passes through
      # items that start with "ALERT:".
      TITLE=$(cat)
      echo "$TITLE" | grep -q '^ALERT:' || exit 1
      echo "$TITLE"

  /home/user/pollers/rss/loader.sh:
      #!/usr/bin/env bash
      TITLE=$(cat "$1")
      echo "$TITLE" | mail -s "$TITLE" root

  Cron entry:
      */15 * * * * etl /home/user/pollers/rss
"""


def parse_args() -> argparse.Namespace:
    # Manual parsing because argparse subparsers don't mix well with
    # an optional positional on the root command.
    argv = sys.argv[1:]

    # Pull out -v/--verbose early
    verbose = False
    if "-v" in argv:
        verbose = True
        argv.remove("-v")
    if "--verbose" in argv:
        verbose = True
        argv.remove("--verbose")

    if "-h" in argv or "--help" in argv:
        print(HELP_TEXT)
        sys.exit(0)

    if argv and argv[0] == "clean":
        argv = argv[1:]
        ns = _parse_clean(argv)
        ns.verbose = verbose
        return ns

    ns = _parse_run(argv)
    ns.verbose = verbose
    return ns


def _parse_clean(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="etl clean", add_help=False)
    parser.add_argument("directory", nargs="?", default=".")
    parser.add_argument("--days", type=int, required=True)
    ns = parser.parse_args(argv)
    ns.command = "clean"
    return ns


def _parse_run(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="etl", add_help=False)
    parser.add_argument("directory", nargs="?", default=".")
    parser.add_argument("--extractor")
    parser.add_argument("--transformer")
    parser.add_argument("--loader")
    parser.add_argument("--parallel", type=int, default=10)
    parser.add_argument("--clean-gt-days", type=int, default=None)
    ns = parser.parse_args(argv)
    ns.command = "run"
    return ns


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(levelname)s: %(message)s",
        level=level,
    )


def cli() -> None:
    sys.exit(main())


if __name__ == "__main__":
    cli()
