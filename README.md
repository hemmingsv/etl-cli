# etl

A minimal extract-deduplicate-transform-load pipeline. You write a shell script
that outputs `<id> <data>` lines; `etl` handles deduplication, optional
transformation, and optional loading — with parallel loaders and cron-friendly
operation built in.

## Install

```sh
uv tool install git+https://github.com/hemmingsv/etl-cli
```

## Usage

```
$ etl --help
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

  Transformers can act as filters by exiting non-zero for items
  that should be ignored. Filtered items leave no trace and will
  be retried every run.

  Loaders run in parallel (up to 10 at a time).

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
```
