# etl

A minimal extract-deduplicate-transform-load pipeline. You write a shell script
that outputs `<id> <data>` lines; `etl` handles deduplication, optional
transformation, and optional loading — with parallel loaders and cron-friendly
operation built in.

Everything is stored on disk and there are options to clean the state files.

If you write a consistent extractor, then it is easy to strap etl on top of
it and then cron on top of etl to make it a proper polling pipeline on disk.

## Install

```sh
uv tool install git+https://github.com/hemmingsv/etl-cli
```

## Usage

```
$ etl --help
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

  etl -e "curl -s url | jq -r '.[] | .id + \" \" + .name'"
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
      curl -s https://example.com/feed.xml \
          | xmlstarlet sel -t -m '//item' \
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
```
