#!/usr/bin/env python3
"""etl — extract, deduplicate, transform, load pipeline."""

import argparse
import asyncio
import collections
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from glob import glob
from pathlib import Path

log = logging.getLogger("etl")


# === Models ===


@dataclass
class Warning:
    """A non-fatal warning message from any pipeline stage."""

    message: str


@dataclass
class ParseWarning:
    """A line that could not be parsed."""

    line_preview: str
    reason: str


@dataclass
class DedupWarning:
    """An item that was already seen or already done."""

    id: str
    path: str


@dataclass
class TransformError:
    """A transformer that exited with an unexpected non-zero code."""

    id: str
    exit_code: int
    stderr: str


@dataclass
class CollectorError:
    """A collector that exited with a non-zero code."""

    exit_code: int
    stderr: str


@dataclass
class Line:
    """A complete line from the extractor."""

    text: str


@dataclass
class Incomplete:
    """Trailing data without a terminating delimiter."""

    text: str


@dataclass
class ExtractorDone:
    """The extractor has finished."""

    exit_code: int
    stderr: str


@dataclass
class Parsed:
    """A successfully parsed line with id and data."""

    id: str
    data: str


@dataclass
class Deduped:
    """An item that passed deduplication."""

    id: str
    data: str
    item_dir: Path


@dataclass
class Transformed:
    """An item that passed transformation."""

    id: str
    data: str
    item_dir: Path


@dataclass
class Filtered:
    """An item filtered out by the transformer."""

    id: str
    exit_code: int


@dataclass
class WriteData:
    """Action: write .data file for an item."""

    id: str
    data: str
    item_dir: Path


@dataclass
class LoadResult:
    """Result of loading a single item via the loader command."""

    id: str
    path: Path
    exit_code: int
    output: str

    @property
    def ok(self) -> bool:
        """Whether the loader exited successfully."""
        return self.exit_code == 0


@dataclass
class CollectAction:
    """Action: what collector would run (dry-run only)."""

    cmd: str
    run_files: list[str]


@dataclass
class Stats:
    """Summary statistics from the pipeline."""

    extracted: int
    invalid: int
    deduped: int
    filtered: int
    loaded: int
    load_failures: int
    transform_errors: int


LineParseFn = Callable[[str], tuple[str, str] | None]

# Union of all pipeline types
PipelineItem = (
    Warning
    | ParseWarning
    | DedupWarning
    | TransformError
    | CollectorError
    | Line
    | Incomplete
    | ExtractorDone
    | Parsed
    | Deduped
    | Transformed
    | Filtered
    | WriteData
    | LoadResult
    | CollectAction
    | Stats
)


# === Colors ===

_IS_TTY = sys.stdout.isatty()


def _color(code: int, text: str) -> str:
    if not _IS_TTY:
        return text
    return f"\033[{code}m{text}\033[0m"


def _red(text: str) -> str:
    return _color(31, text)


def _green(text: str) -> str:
    return _color(32, text)


def _yellow(text: str) -> str:
    return _color(33, text)


def _grey(text: str) -> str:
    return _color(90, text)


# === Flow ===


def cli() -> None:
    """Entry point for the etl command."""
    sys.exit(main())


def main() -> int:
    """Parse args and run the appropriate subcommand."""
    args = _parse_args()
    _setup_logging(args.verbose)

    if args.command == "clean":
        return cmd_clean(
            Path(args.directory),
            args.days,
            state_dir_name=args.state_dir,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

    if args.command == "status":
        return cmd_status(Path(args.directory), state_dir_name=args.state_dir)

    if args.command == "list":
        return cmd_list(Path(args.directory), state_dir_name=args.state_dir)

    filter_exits = None
    if args.filter_exits is not None:
        filter_exits = {int(c) for c in args.filter_exits.split(",")}

    return cmd_run(
        directory=Path(args.directory),
        extractor=args.extractor,
        transformer=args.transformer,
        loader=args.loader,
        collector=args.collector,
        clean_gt_days=args.clean_gt_days,
        workers=args.workers,
        state_dir_name=args.state_dir,
        null_delimited=args.null_delimited,
        fail_empty=args.fail_empty,
        filter_exits=filter_exits,
        parser=args.parser,
        dry_run=args.dry_run,
    )


def cmd_run(
    directory: Path,
    extractor: str,
    transformer: str | None,
    loader: str | None,
    collector: str | None = None,
    clean_gt_days: int | None = None,
    workers: int = 10,
    state_dir_name: str = "state",
    null_delimited: bool = False,
    fail_empty: bool = True,
    filter_exits: set[int] | None = None,
    parser: str = "auto",
    dry_run: bool = False,
) -> int:
    """Run the streaming ETL pipeline."""
    directory, state_dir = _resolve_directory(directory, state_dir_name)
    env = _command_env(directory, state_dir)

    return asyncio.run(
        _run_pipeline(
            directory=directory,
            state_dir=state_dir,
            env=env,
            extractor=extractor,
            transformer=transformer,
            loader=loader,
            collector=collector,
            clean_gt_days=clean_gt_days,
            workers=workers,
            state_dir_name=state_dir_name,
            null_delimited=null_delimited,
            fail_empty=fail_empty,
            filter_exits=filter_exits,
            parser=parser,
            dry_run=dry_run,
        )
    )


def cmd_status(
    directory: Path,
    state_dir_name: str = "state",
) -> int:
    """Print item counts by exit code and success rates over time windows."""
    directory, state_dir = _resolve_directory(directory, state_dir_name)
    if not state_dir.is_dir():
        log.error("No state directory: %s", state_dir)
        return 1

    items = _scan_state_dir(state_dir)
    if not items:
        print("No items in state directory.")
        return 0

    now = time.time()

    # Group by exit code
    by_code: dict[int, list[tuple[str, float]]] = {}
    for item_id, exit_code, mtime in items:
        by_code.setdefault(exit_code, []).append((item_id, mtime))

    # Print items by exit code
    print("Items by exit code:")
    for code in sorted(by_code):
        entries = by_code[code]
        latest = max(mtime for _, mtime in entries)
        ago = _relative_time(now - latest)
        print(f"  exit {code}: {len(entries)} item(s), latest {ago}")

    # Success rate over time windows
    windows = [
        ("1h", 3600),
        ("12h", 43200),
        ("24h", 86400),
        ("3d", 259200),
        ("7d", 604800),
        ("30d", 2592000),
        ("90d", 7776000),
        ("365d", 31536000),
        ("all", None),
    ]

    print("\nSuccess rate:")
    for label, seconds in windows:
        if seconds is not None:
            threshold = now - seconds
            window_items = [(ec, mt) for _, ec, mt in items if mt >= threshold]
        else:
            window_items = [(ec, mt) for _, ec, mt in items]
        if not window_items:
            print(f"  {label:>4s}: {'=':>6s} (0 items)")
            continue
        successes = sum(1 for ec, _ in window_items if ec == 0)
        total = len(window_items)
        pct = successes / total * 100
        print(f"  {label:>4s}: {pct:5.1f}% ({successes}/{total})")

    return 0


def cmd_list(
    directory: Path,
    state_dir_name: str = "state",
) -> int:
    """List all items in the state directory with ID, date, and data path."""
    directory, state_dir = _resolve_directory(directory, state_dir_name)

    from datetime import UTC, datetime

    for item_id, item_dir in _iter_item_dirs(state_dir):
        data_file = item_dir / f"{item_id}.data"
        if not data_file.exists():
            continue
        mtime = data_file.stat().st_mtime
        dt = datetime.fromtimestamp(mtime, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        print(f"{item_id} {dt} {data_file}")

    return 0


def cmd_clean(
    directory: Path,
    days: int,
    state_dir_name: str = "state",
    protect_ids: set[str] | None = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> int:
    """Remove state older than the given number of days."""
    directory, state_dir = _resolve_directory(directory, state_dir_name)
    threshold = time.time() - days * 86400

    items = _cleanable_items(state_dir, threshold, protect_ids)
    if dry_run:
        _print_clean_dry_run_items(items, days, verbose)
        return 0
    removed = _remove_items(state_dir, items)
    log.info("Removed %d item folder(s)", removed)
    return 0


# === Pipeline stages ===


async def _run_pipeline(
    directory: Path,
    state_dir: Path,
    env: dict[str, str],
    extractor: str,
    transformer: str | None,
    loader: str | None,
    collector: str | None = None,
    clean_gt_days: int | None = None,
    workers: int = 10,
    state_dir_name: str = "state",
    null_delimited: bool = False,
    fail_empty: bool = True,
    filter_exits: set[int] | None = None,
    parser: str = "auto",
    dry_run: bool = False,
) -> int:
    delim = b"\0" if null_delimited else b"\n"
    rstrip = None if null_delimited else b"\r"

    # Set up streaming extractor
    if extractor == "-":
        log.info("Reading extractor output from stdin")
        line_iter = _stream_stdin(delim, rstrip)
        extractor_proc = None
    else:
        log.info("Extracting with %s", extractor)
        extractor_proc = await asyncio.create_subprocess_shell(
            extractor,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        assert extractor_proc.stdout is not None
        line_iter = _stream_reader(extractor_proc.stdout, delim, rstrip)

    # Set up the parse function
    if parser.startswith("."):
        parse_fn = _make_json_parser(parser[1:])
    elif parser == "auto":
        parse_fn = _auto_parse_line
    else:
        parse_fn = parse_extractor_line

    all_ids: set[str] = set()

    # Compose pipeline stages
    stream: AsyncIterator[PipelineItem] = _extract(line_iter, extractor_proc)
    stream = _parse(stream, parse_fn)
    stream = _dedup(stream, state_dir, all_ids)
    stream = _transform(stream, transformer, env, filter_exits)
    stream = _load(stream, loader, env, workers, dry_run)
    stream = _collect(stream, collector, env, directory, fail_empty, dry_run)

    if dry_run:
        exit_code = await _handle_dry_run(stream, loader_desc=loader)
    else:
        exit_code = await _handle_real(stream, fail_empty)

    # Clean (dry-run aware)
    if clean_gt_days is not None:
        if dry_run:
            _print_clean_dry_run(
                directory,
                clean_gt_days,
                state_dir_name,
                all_ids,
                verbose=log.isEnabledFor(logging.DEBUG),
            )
        else:
            cmd_clean(
                directory,
                clean_gt_days,
                state_dir_name=state_dir_name,
                protect_ids=all_ids,
            )

    return exit_code


async def _extract(
    upstream: AsyncIterator[tuple[str, bool]],
    extractor_proc: asyncio.subprocess.Process | None,
) -> AsyncIterator[PipelineItem]:
    pending_incomplete: str | None = None
    async for line, complete in upstream:
        if not complete:
            pending_incomplete = line
            continue
        yield Line(text=line)

    # Get extractor exit code and stderr
    exit_code = 0
    stderr_text = ""
    if extractor_proc is not None:
        assert extractor_proc.stderr is not None
        stderr_bytes = await extractor_proc.stderr.read()
        await extractor_proc.wait()
        exit_code = extractor_proc.returncode or 0
        stderr_text = stderr_bytes.decode().strip()

    if pending_incomplete is not None:
        yield Incomplete(text=pending_incomplete)

    yield ExtractorDone(exit_code=exit_code, stderr=stderr_text)


async def _parse(
    upstream: AsyncIterator[PipelineItem],
    parse_fn: LineParseFn,
) -> AsyncIterator[PipelineItem]:
    held_incomplete: str | None = None

    async for item in upstream:
        if isinstance(item, Line):
            parsed = parse_fn(item.text)
            if parsed is None:
                yield ParseWarning(line_preview=item.text[:80], reason="malformed line")
                continue
            raw_id, data = parsed
            item_id = sanitize_id(raw_id)
            if not item_id:
                yield ParseWarning(
                    line_preview=item.text[:80],
                    reason=f"empty ID after sanitization of {raw_id!r}",
                )
                continue
            yield Parsed(id=item_id, data=data)
        elif isinstance(item, Incomplete):
            held_incomplete = item.text
        elif isinstance(item, ExtractorDone):
            # Process incomplete trailing line only if extractor exited 0
            if held_incomplete is not None:
                if item.exit_code == 0:
                    parsed = parse_fn(held_incomplete)
                    if parsed is None:
                        yield ParseWarning(
                            line_preview=held_incomplete[:80],
                            reason="malformed line",
                        )
                    else:
                        raw_id, data = parsed
                        item_id = sanitize_id(raw_id)
                        if not item_id:
                            yield ParseWarning(
                                line_preview=held_incomplete[:80],
                                reason=f"empty ID after sanitization of {raw_id!r}",
                            )
                        else:
                            yield Parsed(id=item_id, data=data)
                else:
                    log.debug(
                        "Discarding incomplete trailing line: %s",
                        held_incomplete[:80],
                    )
            yield item
        else:
            yield item


async def _dedup(
    upstream: AsyncIterator[PipelineItem],
    state_dir: Path,
    all_ids: set[str],
) -> AsyncIterator[PipelineItem]:
    seen_ids: collections.OrderedDict[str, None] = collections.OrderedDict()
    dedup_limit = 10000

    async for item in upstream:
        if isinstance(item, Parsed):
            item_id = item.id
            if item_id in seen_ids:
                yield DedupWarning(id=item_id, path="duplicate in extractor output")
                continue
            seen_ids[item_id] = None
            if len(seen_ids) > dedup_limit:
                seen_ids.popitem(last=False)
            all_ids.add(item_id)
            shard = item_id[:2]
            item_dir = state_dir / shard / item_id
            if _is_done(item_dir, item_id):
                yield DedupWarning(id=item_id, path=str(item_dir))
                continue
            yield Deduped(id=item_id, data=item.data, item_dir=item_dir)
        else:
            yield item


async def _transform(
    upstream: AsyncIterator[PipelineItem],
    transformer: str | None,
    env: dict[str, str],
    filter_exits: set[int] | None,
) -> AsyncIterator[PipelineItem]:
    async for item in upstream:
        if isinstance(item, Deduped):
            if not transformer:
                yield Transformed(id=item.id, data=item.data, item_dir=item.item_dir)
                continue
            item_env = {**env, "ETL_ID": item.id}
            t_result = _run_command(transformer, item_env, stdin_data=item.data)
            if t_result.returncode != 0:
                if filter_exits is None or t_result.returncode in filter_exits:
                    yield Filtered(id=item.id, exit_code=t_result.returncode)
                else:
                    yield TransformError(
                        id=item.id,
                        exit_code=t_result.returncode,
                        stderr=t_result.stderr.strip(),
                    )
                continue
            yield Transformed(id=item.id, data=t_result.stdout, item_dir=item.item_dir)
        else:
            yield item


async def _load(
    upstream: AsyncIterator[PipelineItem],
    loader: str | None,
    env: dict[str, str],
    workers: int,
    dry_run: bool,
) -> AsyncIterator[PipelineItem]:
    if dry_run or not loader:
        # No worker pattern needed — just yield WriteData + dummy LoadResult
        async for item in upstream:
            if isinstance(item, Transformed):
                yield item  # pass through for dry-run handler
                yield WriteData(id=item.id, data=item.data, item_dir=item.item_dir)
                yield LoadResult(id=item.id, path=item.item_dir, exit_code=0, output="")
            else:
                yield item
        return

    # Worker pattern with bounded queue for real loading
    in_queue: asyncio.Queue[Transformed | None] = asyncio.Queue(maxsize=workers)
    out_queue: asyncio.Queue[PipelineItem | tuple[int, LoadResult] | None] = asyncio.Queue()
    # Track order: sequence number -> result
    pending_results: dict[int, LoadResult] = {}
    next_seq = 0  # next sequence number to assign
    next_emit = 0  # next sequence number to emit

    async def worker() -> None:
        while True:
            work_item = await in_queue.get()
            if work_item is None:
                in_queue.task_done()
                break
            seq = work_item._seq  # type: ignore[attr-defined]
            data_file = work_item.item_dir / f"{work_item.id}.data"
            log.info("Loading %s", work_item.id)
            item_env = {**env, "ETL_ID": work_item.id}
            proc = await asyncio.create_subprocess_shell(
                f"{loader} {shlex.quote(str(data_file))}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=item_env,
            )
            stdout_bytes, _ = await proc.communicate()
            assert proc.returncode is not None
            output = stdout_bytes.decode() if stdout_bytes else ""
            lr = LoadResult(
                id=work_item.id,
                path=work_item.item_dir,
                exit_code=proc.returncode,
                output=output,
            )
            await out_queue.put((seq, lr))
            in_queue.task_done()

    async def feeder() -> None:
        nonlocal next_seq
        async for item in upstream:
            if isinstance(item, Transformed):
                # Pass through Transformed and WriteData before enqueueing
                await out_queue.put(item)
                await out_queue.put(WriteData(id=item.id, data=item.data, item_dir=item.item_dir))
                item._seq = next_seq  # type: ignore[attr-defined]
                next_seq += 1
                await in_queue.put(item)
            else:
                await out_queue.put(item)
        # Signal workers to stop
        for _ in range(workers):
            await in_queue.put(None)
        await in_queue.join()
        await out_queue.put(None)  # Signal end of output

    # Start workers and feeder
    worker_tasks = [asyncio.create_task(worker()) for _ in range(workers)]
    feeder_task = asyncio.create_task(feeder())

    # Consume output queue, preserving order for LoadResults
    while True:
        out_item = await out_queue.get()
        if out_item is None:
            break
        if isinstance(out_item, tuple):
            pending_results[out_item[0]] = out_item[1]  # type: ignore[index]
            # Emit in order
            while next_emit in pending_results:
                yield pending_results.pop(next_emit)
                next_emit += 1
        else:
            yield out_item

    # Clean up
    await feeder_task
    for wt in worker_tasks:
        await wt


async def _collect(
    upstream: AsyncIterator[PipelineItem],
    collector: str | None,
    env: dict[str, str],
    directory: Path,
    fail_empty: bool,
    dry_run: bool,
) -> AsyncIterator[PipelineItem]:
    extracted = 0
    invalid = 0
    deduped = 0
    filtered = 0
    loaded = 0
    load_failures = 0
    transform_errors = 0
    load_results: list[LoadResult] = []

    async for item in upstream:
        if isinstance(item, ParseWarning):
            invalid += 1
        elif isinstance(item, DedupWarning):
            deduped += 1
        elif isinstance(item, Filtered):
            filtered += 1
        elif isinstance(item, TransformError):
            transform_errors += 1
        elif isinstance(item, LoadResult):
            loaded += 1
            if not item.ok:
                load_failures += 1
            if collector:
                load_results.append(item)
        yield item

    # extracted = everything that came through the parse stage
    extracted = invalid + deduped + filtered + transform_errors + loaded

    # Run collector or yield action
    if collector and load_results:
        run_files = [str(lr.path / f"{lr.id}.run.{lr.exit_code}") for lr in load_results]
        if dry_run:
            yield CollectAction(cmd=collector, run_files=run_files)
        else:
            loaded_ids = [lr.id for lr in load_results]
            collector_env = {
                **env,
                "ETL_DIR": str(directory),
                "ETL_IDS": " ".join(loaded_ids),
            }
            collect_result = _run_collector(collector, run_files, collector_env)
            if collect_result.stderr:
                yield Warning(message=f"Collector stderr: {collect_result.stderr}")
            if collect_result.returncode != 0:
                yield CollectorError(
                    exit_code=collect_result.returncode,
                    stderr=collect_result.stderr.strip(),
                )

    yield Stats(
        extracted=extracted,
        invalid=invalid,
        deduped=deduped,
        filtered=filtered,
        loaded=loaded,
        load_failures=load_failures,
        transform_errors=transform_errors,
    )


async def _handle_dry_run(
    upstream: AsyncIterator[PipelineItem],
    loader_desc: str | None,
) -> int:
    verbose = log.isEnabledFor(logging.DEBUG)
    extractor_exit_code = 0
    first_error_code = 0

    async for item in upstream:
        match item:
            case ParseWarning(line_preview=preview, reason=reason):
                if verbose:
                    print(_red(f"  -> skipped: {reason} ({preview})"))
            case DedupWarning(id=_, path=path):
                if verbose:
                    if path == "duplicate in extractor output":
                        print(_red("  -> *ALREADY EXISTS*: duplicate in extractor output"))
                    else:
                        print(_red(f"  -> *ALREADY EXISTS*: {path}"))
            case Transformed(id=item_id, data=data, item_dir=item_dir):
                print(_green(f"  -> *NEW ENTRY*: {item_id} ({item_dir})"))
                if verbose:
                    print(_grey(f"     data: {data}"))
            case Filtered(id=_, exit_code=ec):
                print(_red(f"  -> *FILTERED OUT*: exit code {ec}"))
            case TransformError(id=item_id, exit_code=ec, stderr=stderr):
                print(_red(f"  -> *TRANSFORM ERROR* {item_id}: exit code {ec}: {stderr}"))
                if not first_error_code:
                    first_error_code = ec
            case CollectorError(exit_code=ec, stderr=stderr):
                print(_red(f"  -> *COLLECTOR ERROR*: exit code {ec}: {stderr}"))
                if not first_error_code:
                    first_error_code = ec
            case WriteData():
                pass
            case LoadResult():
                ld = loader_desc or "(no loader)"
                data_path = item.path / f"{item.id}.data"
                print(_yellow(f"  -> *WOULD LOAD*: {ld} {data_path}"))
            case CollectAction(cmd=cmd, run_files=run_files):
                print(_yellow(f"  -> *WOULD COLLECT*: {cmd} ({len(run_files)} file(s))"))
            case Stats() as stats:
                print(
                    f"Extracted: {stats.extracted}, Invalid: {stats.invalid}, "
                    f"Deduplicated: {stats.deduped}, Filtered: {stats.filtered}, "
                    f"Would load: {stats.loaded}",
                    file=sys.stderr,
                )
            case ExtractorDone(exit_code=ec, stderr=stderr):
                extractor_exit_code = ec
                if stderr:
                    log.warning("Extractor stderr: %s", stderr)
                if ec != 0:
                    log.warning("Extractor exited with code %d", ec)

    if extractor_exit_code != 0:
        return extractor_exit_code
    return first_error_code


async def _handle_real(
    upstream: AsyncIterator[PipelineItem],
    fail_empty: bool,
) -> int:
    extractor_exit_code = 0
    first_error_code = 0
    load_failures = 0
    stats: Stats | None = None

    async for item in upstream:
        match item:
            case Warning(message=msg):
                log.warning("%s", msg)
            case ParseWarning(line_preview=preview, reason=_):
                log.warning("Skipping malformed line: %s", preview)
            case DedupWarning(id=item_id, path=_):
                log.warning("Duplicate ID in extractor output, skipping: %s", item_id)
            case TransformError(id=item_id, exit_code=ec, stderr=stderr):
                log.warning("Transformer error for %s (exit %d): %s", item_id, ec, stderr)
                if not first_error_code:
                    first_error_code = ec
            case CollectorError(exit_code=ec, stderr=stderr):
                log.warning("Collector failed (exit %d): %s", ec, stderr)
                if not first_error_code:
                    first_error_code = ec
            case Filtered(id=item_id, exit_code=ec):
                log.info("Transformer filtered %s (exit %d)", item_id, ec)
            case Transformed():
                pass
            case WriteData(id=item_id, data=data, item_dir=item_dir):
                item_dir.mkdir(parents=True, exist_ok=True)
                (item_dir / f"{item_id}.data").write_text(data)
            case LoadResult(id=item_id, path=item_dir, exit_code=ec, output=output):
                run_file = item_dir / f"{item_id}.run.{ec}"
                run_file.write_text(output)
                if ec != 0:
                    load_failures += 1
                    log.warning("Loader failed for %s (exit %d)", item_id, ec)
                    if not first_error_code:
                        first_error_code = ec
            case Stats() as s:
                stats = s
            case ExtractorDone(exit_code=ec, stderr=stderr):
                extractor_exit_code = ec
                if stderr:
                    log.warning("Extractor stderr: %s", stderr)
                if ec != 0:
                    log.warning("Extractor exited with code %d", ec)

    assert stats is not None
    valid_line_count = stats.extracted - stats.invalid

    # --fail-empty
    if fail_empty and valid_line_count == 0:
        if extractor_exit_code == 0:
            log.error("Extractor produced zero valid lines")
            return 1
        log.warning("Extractor produced zero valid lines (extractor already failed)")

    if stats.loaded == 0 and first_error_code == 0 and stats.transform_errors == 0:
        log.warning("No new items")

    if load_failures:
        log.warning("%d item(s) failed to load", load_failures)

    if extractor_exit_code != 0:
        return extractor_exit_code
    return first_error_code


# === Streaming ===


async def _stream_reader(
    reader: asyncio.StreamReader,
    delimiter: bytes = b"\n",
    rstrip: bytes | None = b"\r",
) -> AsyncIterator[tuple[str, bool]]:
    """Yield (line, complete) from an async stream.

    complete=True if the line was terminated by delimiter.
    complete=False for trailing data without a terminating delimiter.
    """
    buf = b""
    while True:
        chunk = await reader.read(8192)
        if not chunk:
            break
        buf += chunk
        while delimiter in buf:
            line, buf = buf.split(delimiter, 1)
            if rstrip:
                line = line.rstrip(rstrip)
            decoded = line.decode()
            if decoded:
                yield decoded, True
    if buf:
        # Do not rstrip an unfinished line
        yield buf.decode(), False


async def _stream_stdin(
    delimiter: bytes = b"\n",
    rstrip: bytes | None = b"\r",
) -> AsyncIterator[tuple[str, bool]]:
    """Yield (line, complete) from stdin, streaming as data arrives."""
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    transport, _ = await loop.connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(reader),
        sys.stdin.buffer,
    )
    try:
        async for item in _stream_reader(reader, delimiter, rstrip):
            yield item
    finally:
        transport.close()


# === Clean helpers ===


def _print_clean_dry_run(
    directory: Path,
    clean_gt_days: int,
    state_dir_name: str,
    all_ids: set[str],
    verbose: bool = False,
) -> None:
    state_dir = resolve_state_dir(directory, state_dir_name)
    threshold = time.time() - clean_gt_days * 86400
    items = _cleanable_items(state_dir, threshold, all_ids)
    _print_clean_dry_run_items(items, clean_gt_days, verbose)


def _print_clean_dry_run_items(
    items: list[Path],
    days: int,
    verbose: bool = False,
) -> None:
    if items:
        print(_yellow(f"Would clean {len(items)} item(s) older than {days} days"))
        if verbose:
            for item_dir in items:
                print(_yellow(f"  {item_dir}"))
    else:
        print(_grey(f"Nothing to clean (older than {days} days)"))


def _iter_item_dirs(state_dir: Path) -> Iterator[tuple[str, Path]]:
    # Synchronous directory walk, used by:
    #   cmd_list     — standalone subcommand, no async
    #   cmd_status   — standalone subcommand, no async (via _scan_state_dir)
    #   cmd_clean    — standalone subcommand, no async (via _cleanable_items)
    #   _run_pipeline — only after the async pipeline has fully drained
    #                   (via _print_clean_dry_run → _cleanable_items)
    # No async needed: all callers run after concurrent work is done.
    if not state_dir.is_dir():
        return
    for shard_dir in sorted(state_dir.iterdir()):
        if not shard_dir.is_dir():
            continue
        for item_dir in sorted(shard_dir.iterdir()):
            if not item_dir.is_dir():
                continue
            yield item_dir.name, item_dir


def _cleanable_items(
    state_dir: Path,
    threshold: float,
    protect_ids: set[str] | None = None,
) -> list[Path]:
    result: list[Path] = []
    for item_id, item_dir in _iter_item_dirs(state_dir):
        if protect_ids and item_id in protect_ids:
            continue
        if _all_files_older_than(item_dir, threshold):
            result.append(item_dir)
    return result


def _remove_items(state_dir: Path, items: list[Path]) -> int:
    removed = 0
    shard_dirs: set[Path] = set()
    for item_dir in items:
        shard_dirs.add(item_dir.parent)
        shutil.rmtree(item_dir)
        removed += 1
    for shard_dir in shard_dirs:
        if shard_dir.is_dir() and not any(shard_dir.iterdir()):
            shard_dir.rmdir()
    return removed


def _all_files_older_than(directory: Path, threshold: float) -> bool:
    files = list(directory.iterdir())
    if not files:
        return True
    return all(f.stat().st_mtime < threshold for f in files if f.is_file())


# === Status helpers ===


def _scan_state_dir(state_dir: Path) -> list[tuple[str, int, float]]:
    items: list[tuple[str, int, float]] = []
    for item_id, item_dir in _iter_item_dirs(state_dir):
        run_files = [f for f in item_dir.iterdir() if f.name.startswith(f"{item_id}.run.")]
        if not run_files:
            continue
        latest_run = max(run_files, key=lambda f: f.stat().st_mtime)
        parts = latest_run.name.rsplit(".", 1)
        try:
            exit_code = int(parts[-1])
        except ValueError:
            continue
        items.append((item_id, exit_code, latest_run.stat().st_mtime))
    return items


def _relative_time(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds / 60)}m ago"
    if seconds < 86400:
        return f"{int(seconds / 3600)}h ago"
    return f"{int(seconds / 86400)}d ago"


# === Command helpers ===


def _command_env(directory: Path, state_dir: Path | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env["PATH"] = str(directory) + os.pathsep + env.get("PATH", "")
    if state_dir is not None:
        env["ETL_STATE_DIR"] = str(state_dir)
    return env


def _run_command(
    cmd: str,
    env: dict[str, str],
    stdin_data: str | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["sh", "-c", cmd],
        input=stdin_data,
        capture_output=True,
        text=True,
        env=env,
    )


def _make_json_parser(key: str) -> LineParseFn:
    def parse_json_line(line: str) -> tuple[str, str] | None:
        line = line.strip()
        if not line:
            return None
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return None
        if not isinstance(obj, dict) or not obj:
            return None
        raw_id = str(obj.get(key, ""))
        if not raw_id:
            return None
        return raw_id, line

    return parse_json_line


_DEFAULT_JSON_PARSER = _make_json_parser("_id")


def _auto_parse_line(line: str) -> tuple[str, str] | None:
    stripped = line.lstrip()
    if stripped.startswith("{"):
        return _DEFAULT_JSON_PARSER(line)
    return parse_extractor_line(line)


def _run_collector(
    collector: str,
    run_files: list[str],
    env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    cmd = collector + " " + " ".join(shlex.quote(f) for f in run_files)
    return subprocess.run(
        ["sh", "-c", cmd],
        capture_output=True,
        text=True,
        env=env,
    )


# === Core ===


def parse_extractor_line(line: str) -> tuple[str, str] | None:
    """Parse a 'pop-first-col' line into (raw_id, data), or None if malformed."""
    if not line or line[0].isspace():
        return None
    parts = line.split(None, 1)
    if not parts:
        return None
    return parts[0], parts[1] if len(parts) > 1 else ""


def sanitize_id(raw_id: str) -> str:
    """Strip all characters except alphanumeric, underscore, and dash."""
    return re.sub(r"[^a-zA-Z0-9_\-]", "", raw_id)


def _is_done(item_dir: Path, item_id: str) -> bool:
    return (item_dir / f"{item_id}.run.0").exists()


def resolve_state_dir(directory: Path, state_dir_name: str = "state") -> Path:
    """Resolve the state directory path relative to the pipeline directory."""
    return (directory / state_dir_name).resolve()


def _resolve_directory(directory: Path, state_dir_name: str = "state") -> tuple[Path, Path]:
    """Resolve and validate directory + state dir. Raises SystemExit on error."""
    directory = directory.resolve()
    if not directory.is_dir():
        log.error("Not a directory: %s", directory)
        raise SystemExit(1)
    state_dir = resolve_state_dir(directory, state_dir_name)
    return directory, state_dir


# === Discovery ===


def _discover_script(directory: Path, pattern: str) -> str | None:
    matches = glob(str(directory / pattern))
    matches = [m for m in matches if os.access(m, os.X_OK)]

    if len(matches) == 1:
        return Path(matches[0]).name
    if len(matches) > 1:
        raise SystemExit(f"Multiple matches for {pattern} in {directory}: {matches}")
    return None


def _auto_discover(directory: Path) -> dict[str, str | None]:
    return {
        "extractor": _discover_script(directory, "*extract.*"),
        "transformer": _discover_script(directory, "*transform.*"),
        "loader": _discover_script(directory, "*loader.*"),
        "collector": _discover_script(directory, "*collect.*"),
    }


# === CLI ===


HELP_TEXT = """\
usage: etl [run] [directory] [options]
       etl [run] [directory] -a [+t] [+l] [+c]
       etl [run] [directory] -e CMD [-t CMD] [-l CMD] [-c CMD]
       etl status [directory] [--state-dir DIR]
       etl clean [directory] --days <n>

Extract, deduplicate, transform, load pipeline.

EXAMPLES
  etl -a
      Auto-discover scripts (*extract.*, *transform.*, *loader.*,
      *collect.*) in the current directory and run the pipeline.

  etl -e extract.sh -l loader.sh
      Run with explicit commands in the current directory.

  etl /path/to/poller -a +t
      Auto-discover in a specific directory, skip the transformer.

  etl -e "curl -s url | jq -r '.[] | .id + \\" \\" + .name'"
      Inline shell command as extractor.

  echo "id1 data" | etl -e -
      Read extractor output from stdin.

  etl -e extract.sh --dry-run
      Preview what would happen without writing any state.

  etl -e extract.sh --parser ._id
      Parse JSON objects, using _id as the item ID.

  etl -e extract.sh -t transform.sh --filter-exits 99
      Only exit code 99 silently filters; other non-zero
      codes from the transformer are treated as errors.

  etl -e extract.sh --clean-gt-days 30
      Run pipeline, then clean old state. IDs seen in
      the current extractor output are never cleaned.

  etl status
      Show item counts by exit code and success rates.

  etl clean --days 30
      Remove all state older than 30 days.

  etl run status -a
      Run a pipeline in a directory called "status".

OPTIONS
  [directory]               Pipeline directory (default: cwd)
  -e, --extractor CMD       Extractor command (default: stdin)
  -t, --transformer CMD     Transformer command
  -l, --loader CMD          Loader command
  -c, --collector CMD       Collector command (runs once after loading)
  -a, --auto-discover       Find scripts by pattern in directory
  +t, +l, +c               Exclude stage from auto-discovery
  -0                        NULL-delimited extractor output
  -n, --workers N           Number of loader workers (default: 10)
  --parser MODE             auto (default), pop-first-col, or .FIELD
  --dry-run                 Preview without writing state
  --fail-empty              Error on zero valid lines (default: on)
  --no-fail-empty           Disable --fail-empty
  --filter-exits CODE,...   Transformer exit codes that silently filter
  --clean-gt-days N         Clean state older than N days after run
  --state-dir DIR           State directory name (default: state)
  -v, --verbose             Enable debug logging
  -h, --help                Show this help message

CONCEPTS
  The pipeline has five stages: extract, deduplicate, transform,
  load, collect. Only extract and deduplicate are mandatory.

  The power of etl comes from separation of concern. You only have
  to worry about building an extractor, then you slap etl on top of
  it and you get deduplication (the extractor needs no logic for
  this) and if you slap cron on top of etl, you have a polling
  pipeline.

  Commands are passed as strings and executed via sh -c with
  PATH=directory:$PATH. This means a bare script name like
  "extract.sh" resolves to the script in the pipeline directory,
  while "curl -s url | jq ..." works just as well.

  The extractor outputs lines to stdout. By default each line is
  <id> [data], split on first whitespace. With --parser .X, each
  line is a JSON object and the ID is taken from key X. The default
  parser (auto) detects JSON lines by a leading { and uses ._id,
  otherwise falls back to pop-first-col.

  IDs are sanitized (only alphanumeric, underscore, and dash are
  kept) and used to create state directories for deduplication.
  State is sharded by the first two characters of the sanitized ID
  into state/<shard>/<id>/.

  Clearly for this to work, you have to write extractors that have
  consistent id outputs for the same item. E.g. a scraper could use
  the ids used on the website, rather than generating uuids on the
  fly.

  The presence of <id>.run.0 marks an item as done. Failed items
  (<id>.run.<non-zero>) are retried on the next run. Filtered
  items (transformer exit non-zero) leave no trace and will be
  retried every run.

  The collector runs once after all loading completes, receiving
  all .run.N file paths as arguments. Use it for post-processing,
  notifications, or dashboard updates.

  The pipeline streams: items are parsed, deduped, transformed,
  and loaded as the extractor produces them. Loaders run
  concurrently with extraction.

  If neither -e nor -a is given, etl reads from stdin (with a
  warning — pass -e - to silence it).

ENVIRONMENT VARIABLES
  Commands receive PATH=directory:$PATH plus:
    ETL_STATE_DIR   State directory path (all stages)
    ETL_ID          Item ID (transformer, loader)
    ETL_DIR         Pipeline directory (collector)
    ETL_IDS         Space-separated loaded IDs (collector)

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
      TITLE=$(cat)
      echo "$TITLE" | grep -q '^ALERT:' || exit 1
      echo "$TITLE"

  /home/user/pollers/rss/loader.sh:
      #!/usr/bin/env bash
      TITLE=$(cat "$1")
      echo "$TITLE" | mail -s "$TITLE" root

  Cron:  */15 * * * * etl /home/user/pollers/rss -a

  NOTE: Scripts must be executable (chmod +x).
  NOTE: Scripts need not be shell scripts — any executable works.

ETERNAL EXTRACTORS
  Extractors can run forever (e.g. tail -f, inotifywait).
  Items are processed as the extractor produces them:

      etl -e "inotifywait -m -e create dir --format '%f'" -l loader.sh

  With eternal extractors, don't use -c (or pass +c) to avoid
  collecting an unbounded list of load results in memory.

VIBE CODING
  It is notoriously easy to vibe code an etl pipeline. Open up your
  command line agent, such as claude, and then do:

      ! mkdir my-pipeline
      ! etl --help
      make a pipeline in that folder that ...

  (! in claude executes a command and hands the output to the agent)
"""


def _parse_args() -> argparse.Namespace:
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

    if argv and argv[0] == "status":
        argv = argv[1:]
        ns = _parse_status(argv)
        ns.verbose = verbose
        return ns

    if argv and argv[0] == "list":
        argv = argv[1:]
        ns = _parse_list(argv)
        ns.verbose = verbose
        return ns

    if argv and argv[0] == "run":
        argv = argv[1:]

    ns = _parse_run(argv)
    ns.verbose = verbose
    return ns


def _parse_clean(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="etl clean", add_help=False)
    parser.add_argument("directory", nargs="?", default=".")
    parser.add_argument("--days", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--state-dir", default="state")
    ns = parser.parse_args(argv)
    ns.command = "clean"
    return ns


def _parse_status(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="etl status", add_help=False)
    parser.add_argument("directory", nargs="?", default=".")
    parser.add_argument("--state-dir", default="state")
    ns = parser.parse_args(argv)
    ns.command = "status"
    return ns


def _parse_list(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="etl list", add_help=False)
    parser.add_argument("directory", nargs="?", default=".")
    parser.add_argument("--state-dir", default="state")
    ns = parser.parse_args(argv)
    ns.command = "list"
    return ns


def _parse_run(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="etl", add_help=False)
    parser.add_argument("directory", nargs="?", default=".")
    parser.add_argument("-a", "--auto-discover", action="store_true")
    parser.add_argument("-e", "--extractor")
    parser.add_argument("-t", "--transformer")
    parser.add_argument("-l", "--loader")
    parser.add_argument("-c", "--collector")
    parser.add_argument("-0", "--null", action="store_true", dest="null_delimited")
    parser.add_argument("-n", "--workers", type=int, default=10)
    parser.add_argument("--parser", default="auto", dest="parser")
    parser.add_argument("--fail-empty", action="store_true", default=True)
    parser.add_argument("--no-fail-empty", action="store_false", dest="fail_empty")
    parser.add_argument("--filter-exits", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--clean-gt-days", type=int, default=None)
    parser.add_argument("--state-dir", default="state")
    # Pull out +t/+l/+c exclusion flags before argparse
    skip_transformer = "+t" in argv
    skip_loader = "+l" in argv
    skip_collector = "+c" in argv
    argv = [a for a in argv if a not in ("+t", "+l", "+c")]

    ns = parser.parse_args(argv)
    ns.command = "run"
    ns.skip_transformer = skip_transformer
    ns.skip_loader = skip_loader
    ns.skip_collector = skip_collector

    if ns.auto_discover:
        directory = Path(ns.directory).resolve()
        discovered = _auto_discover(directory)
        # Explicit flags override discovered scripts
        if ns.extractor is None:
            ns.extractor = discovered["extractor"]
        if ns.transformer is None and not ns.skip_transformer:
            ns.transformer = discovered["transformer"]
        if ns.loader is None and not ns.skip_loader:
            ns.loader = discovered["loader"]
        if ns.collector is None and not ns.skip_collector:
            ns.collector = discovered["collector"]
        if ns.extractor is None:
            log.error("No executable *extract.* found in %s", ns.directory)
            sys.exit(1)
    elif ns.extractor is None:
        log.warning(
            "No extractor command passed, will read from stdin. "
            "Pass `-e -` explicitly to silence this warning."
        )
        ns.extractor = "-"

    return ns


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(levelname)s: %(message)s",
        level=level,
    )


if __name__ == "__main__":
    cli()
