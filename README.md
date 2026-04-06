# etl

A minimal extract-deduplicate-transform-load pipeline. You write a shell script
that outputs `<id> <data>` lines; `etl` handles deduplication, optional
transformation, and optional loading — with parallel loaders and cron-friendly
operation built in.

Everything is stored on disk and there are options to clean the state files.

## Install

```sh
uv tool install git+https://github.com/hemmingsv/etl-cli
```

## Usage

```
$ etl --help
usage: etl [run] [DIR] [options]
       etl [run] [DIR] -a [+t] [+l] [+c]
       etl [run] [DIR] -e CMD [-t CMD] [-l CMD] [-c CMD]
       etl list [DIR] [--state-dir RELATIVE_DIR]
       etl status [DIR] [--state-dir RELATIVE_DIR]
       etl clean [DIR] --days <n> [--state-dir RELATIVE_DIR]

Run a command, deduplicate its output by ID, optionally transform
and load each item — with automatic retry, parallel loading, and
cron-friendly operation. State is stored on disk so the pipeline
is idempotent: run it as often as you want, each item is processed
processed successfully only once.

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

  etl list                     List items: ID, date, data path
  etl status                   Show success rates and item counts
  etl clean --days 30          Remove state older than 30 days

  etl run status -a
      Run a pipeline in a directory called "status".

OPTIONS
  [DIR]                     Pipeline directory (default: cwd)
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
  --state-dir RELATIVE_DIR  State directory, relative to DIR (default: state)
  -v, --verbose             Enable debug logging
  -h, --help                Show this help message

THE PIPELINE DIRECTORY
  Everything in etl revolves around a single directory. It
  defaults to the current working directory, or you can pass it
  as the first argument (etl /path/to/poller -a).

  etl never changes its working directory. All paths are resolved
  relative to the pipeline directory, not the CWD. This means you
  can run etl /some/dir from anywhere and get the same result —
  unless you mix in cwd-relative commands such as ./random-script.sh.

  What the directory controls:

  - PATH: Commands are executed via sh -c with the directory
    prepended to PATH. So -e extract.sh finds extract.sh in the
    pipeline directory, while -e "curl url | jq ." runs from the
    system PATH as usual.

  - State: Dedup state is stored in state/ inside the directory
    (override with --state-dir). Each item gets a folder at
    state/<shard>/<id>/ containing .data and .run.N files.

  - Auto-discovery (-a): Looks for *extract.*, *transform.*,
    *loader.*, *collect.* in the directory. Use +t, +l, +c to
    exclude a stage.

  A typical pipeline directory looks like:

      my-poller/
        extract.sh
        transform.sh      (optional)
        loader.sh          (optional)
        collect.sh         (optional)
        state/             (created automatically)
          ab/
            abc123/
              abc123.data
              abc123.run.0

THE PIPELINE
  etl runs your scripts in five stages. Each stage is optional
  except extract (pass -e - to read from stdin instead) and
  deduplicate. Deduplication is automatic and is a core function
  of etl. If you don't need it, you probably just want a normal
  shell pipeline without the etl command.

  1. EXTRACT (-e CMD or -a)
     Runs a command and reads its stdout line by line. Each line
     is one item. There are two line formats:

     pop-first-col: the first whitespace-separated token is the
     ID, the rest is the data.

         msg-001 {"title": "Alert", "body": "disk full"}

     JSON: each line is a JSON object. The ID is taken from a
     named key (default: _id). Force with --parser .FIELD, e.g.
     --parser .id.

         {"_id": "msg-001", "title": "Alert"}

     The default parser (auto) picks JSON if the line starts
     with {, otherwise pop-first-col. In practice, this means
     your extractor either outputs ID-space-data lines, or JSON
     objects with an _id key.

     The extractor can be any command: a script in the pipeline
     directory (found via PATH — see above), a shell pipeline,
     or `-` for stdin. Items are processed as the extractor
     produces them, so the extractor can even run forever
     (e.g. tail -f).

  2. DEDUPLICATE (automatic)
     Each ID is checked against the state directory. If a file
     <id>.run.0 already exists, the item is skipped. This is
     what makes etl idempotent — run it as often as you want and
     each item is processed processed successfully only once.

     IDs are sanitized to [a-zA-Z0-9_-]. State is stored in
     state/<first-two-chars>/<id>/. IDs must be consistent
     across runs: if your extractor produces different IDs for
     the same item each time, dedup won't work.

     Items that failed previously (<id>.run.<non-zero>) are
     retried automatically — they pass dedup because there is
     no .run.0, so they go through transform and load again.

     Within a single run, etl also deduplicates IDs that appear
     more than once in the extractor output. This in-memory set
     is bounded to 10,000 entries to support eternal extractors
     without leaking memory.

  3. TRANSFORM (-t CMD, optional)
     The extractor can only output line-separated items, so the
     transform step exists to reshape data into the format the
     loader needs. It also serves as a filter to skip items.

     Receives item data on stdin, writes transformed data to
     stdout. The output becomes the .data file that the loader
     receives. The item ID is available as $ETL_ID. Exit
     non-zero to silently filter the item.

     By default, any non-zero exit code is a silent filter. Use
     --filter-exits 99 to make only specific codes silent — other
     non-zero codes become errors with the transformer's exit code
     propagated.

  4. LOAD (-l CMD, optional)
     Receives the path to the .data file as its first argument.
     Runs in parallel (--workers N, default 10). Each loader's
     stdout and stderr are captured in the .run.N file, where N
     is the exit code. Exit 0 marks success; any other code is a
     failure that will be retried on the next run.

     If no loader is given, items are marked as done immediately
     after extraction and optional transformation.

  5. COLLECT (-c CMD, optional)
     Runs once after all loading completes. Receives all .run.N
     file paths as arguments, but does not have to use them —
     it can simply trigger whatever needs to happen when
     something new has been loaded (send a summary, update a
     dashboard, ping a webhook). Only runs if at least one
     item reached the load stage.

SUBCOMMANDS
  etl list [DIR] [--state-dir RELATIVE_DIR]
     Lists all items in the state directory, one per line:

         ID YYYY-MM-DDThh:mm:ssZ /path/to/item.data

     Primary use: inspect what items exist in the state directory
     and their IDs. The output is also valid extractor input —
     the ID is the first token and the rest becomes the data.
     This lets you chain pipelines (see CHAINING PIPELINES).

     Note: the date becomes part of the extractor data when
     chaining. If you need just the path, pipe through awk:

         etl list | awk '{print $1, $3}'

     To process items sorted by date:

         etl list | sort -k2 | awk '{print $1, $3}'

  etl status [DIR] [--state-dir RELATIVE_DIR]
     Shows item counts grouped by exit code (0 = success,
     non-zero = failure), the most recent run date for each,
     and success rates over time windows (1h, 12h, 24h, 3d,
     7d, 30d, 90d, 365d, all time).

  etl clean [DIR] --days <n> [--state-dir RELATIVE_DIR] [--dry-run]
     Removes all state older than N days. With --dry-run, shows
     what would be removed without deleting anything.

ENVIRONMENT VARIABLES
  All commands receive PATH=directory:$PATH, plus:
    ETL_STATE_DIR   Absolute path to the state directory (all stages)
    ETL_ID          Current item ID (transformer, loader)
    ETL_DIR         Pipeline directory (collector only)
    ETL_IDS         Space-separated list of loaded IDs (collector only)

EXIT CODES AND STDERR FORWARDING
  etl's exit code reflects the first error encountered:

    0    All items processed successfully (or no new items).
    1    etl's own error (e.g. --fail-empty triggered, bad args).
    N    Forwarded from the first failing command. If the extractor
         exits 3, etl exits 3. If a transformer errors with 42,
         etl exits 42. If the loader fails with 5, etl exits 5.
         The extractor's exit code takes priority over downstream
         errors. The exit code is determined after all items have
         been processed — etl does not abort on the first failure.

  Stderr forwarding by stage:

    Extractor    stderr is logged as a WARNING after the extractor
                 finishes. The extractor's output is still processed
                 even if it exits non-zero (partial output recovery).
    Transformer  stderr is logged as a WARNING when the exit code is
                 not in --filter-exits (i.e. a real error). Silent
                 filter exits discard stderr.
    Loader       stdout and stderr are merged and captured in the
                 .run.N file. Failures are logged as WARNINGs.
    Collector    stderr is logged as a WARNING. Non-zero exit is
                 logged and contributes to etl's exit code.

  All WARNINGs go to stderr. In cron, make sure stderr is
  delivered somewhere you check (email, log file, monitoring).
  etl itself never writes to stdout during a normal run — only
  --dry-run, etl list, and etl status produce stdout output.

  etl does not install signal handlers. On SIGINT (ctrl-C) or
  SIGTERM, the process exits via Python's default behavior. This
  is safe:
  re-running the pipeline picks up where it left off. Items
  without .run.0 are retried, items already done are skipped,
  and partially written .data files are overwritten on retry.

FULL EXAMPLE
  An RSS alert poller. Only items with "ALERT:" in the title
  are kept; matching items are emailed to root.

  ~/pollers/rss/extract.sh:
      #!/usr/bin/env bash
      curl -s https://example.com/feed.xml \
          | xmlstarlet sel -t -m '//item' \
              -v 'concat(guid, " ", title)' -n

  ~/pollers/rss/transform.sh:
      #!/usr/bin/env bash
      TITLE=$(cat)
      echo "$TITLE" | grep -q '^ALERT:' || exit 99
      echo "$TITLE"

  ~/pollers/rss/loader.sh:
      #!/usr/bin/env bash
      TITLE=$(cat "$1")
      echo "$TITLE" | mail -s "$TITLE" root

  Cron:  */15 * * * * etl ~/pollers/rss -a --filter-exits 99

  Scripts must be executable (chmod +x) but need not be shell
  scripts — any executable works (Python, Ruby, a compiled binary).

CHAINING PIPELINES WITH etl list
  etl list outputs one line per item in the state directory:

      ID YYYY-MM-DDThh:mm:ssZ /path/to/item.data

  This is valid extractor input (the ID is the first token, the
  rest becomes the data). You can use it to build a second pipeline
  that branches off the first one's results.

  Example: an email poller creates a task for every new email.
  A separate pipeline reads those emails and uploads attachments.

  Pipeline 1 — extract emails, create tasks:
      ~/mail/extract.sh     Fetches emails, outputs msg-id + body
      ~/mail/loader.sh      Creates a task item per email

      etl ~/mail -a --state-dir state-emails

  Pipeline 2 — process attachments from extracted emails:
      etl ~/mail \
          -e "etl list ~/mail --state-dir state-emails" \
          -t extract-attachments.sh \
          -l upload-attachment.sh \
          --state-dir state-attachments

  Why separate? If "create task" and "upload attachment" were in
  the same loader, a failure is ambiguous: did the task get created
  but the upload failed? Should the whole item be retried? By
  splitting into two pipelines with separate state, each side
  effect is tracked independently. The first pipeline's state
  records which emails were processed. The second pipeline's state
  records which attachments were uploaded. A failure in one does
  not affect the other.

  Both pipelines can live in the same directory — they just need
  separate --state-dir values. However, if you use -a (auto-
  discover), use different directories altogether to avoid
  discovering each other's scripts.

CHECKLIST FOR A ROBUST PIPELINE

  Before deploying:
  [ ] Use --dry-run first. Verify parsing, dedup, and transform
      behavior before the pipeline writes any state.

  Extraction:
  [ ] Extractor IDs are consistent across runs. If the same item
      always produces the same ID, dedup works. If not, you get
      duplicates. Prefer natural IDs (URLs, database PKs) over
      generated ones (generated UUIDs, generated timestamps).
      Timestamps that are part of the item itself (created_at,
      modified_at) can be parts of a valid composite ID. If your
      items vary across several columns, concatenate them into a
      composite ID.
  [ ] --fail-empty is on (default). If your extractor returns
      nothing, that usually means something is wrong (API down,
      auth expired, network issue). Don't disable it unless
      empty output is genuinely expected.
  [ ] Filter in transform, not in extract. If you add filtering
      to the extractor (e.g. grep), an empty result is
      indistinguishable from the extractor being broken. Instead,
      let the extractor return everything and use the transformer
      to filter. That way --fail-empty catches real extraction
      failures while filtered items are handled gracefully.

  Transformation:
  [ ] Keep the transformer pure. The transform step runs items one
      at a time and interprets non-zero exits as filters. If your
      transformer has side effects (HTTP calls, writes to disk),
      transient failures look like intentional filtering — items
      silently disappear. Transform should only reshape data into
      the format the loader needs. Put all side effects in the
      loader, where failures are recorded and retried.
  [ ] Use explicit --filter-exits codes. By default, any non-zero
      transformer exit is a silent filter. If your transformer uses
      a specific exit code to filter (e.g. exit 99), pass
      --filter-exits 99 explicitly. That way, unexpected failures
      (segfault, permission error, typo) surface as errors instead
      of silently dropping items. Use an uncommon code like 99 —
      common codes (1, 2) are too easily returned by accident from
      failed commands inside your transformer.

  Loading:
  [ ] One side effect per loader. If your loader does two things
      (e.g. create a task AND upload an attachment), a failure is
      ambiguous: which part failed? Should the item be retried?
      Split into two pipelines with separate state (see CHAINING
      PIPELINES above). Each pipeline tracks one side effect,
      making retries precise and failures unambiguous. Multiple
      side effects in one loader are fine if the earlier ones are
      safe to retry (idempotent) when a later one fails.

  Operations:
  [ ] Cron error reporting works. etl exits non-zero on failure
      and logs to stderr. Make sure your cron daemon delivers
      error output somewhere you actually check: email, a log
      file, a monitoring system. A silent cron failure is worse
      than no automation.
  [ ] Don't use -a on multi-user systems. A malicious user could
      drop a transform.sh into the directory and it would run
      under your account. Use explicit -e/-t/-l/-c instead.
  [ ] Skip the collector with eternal extractors (+c). If your
      extractor runs forever (tail -f, inotifywait), don't use
      a collector — it would buffer all results in memory
      indefinitely.
  [ ] Separate state for multiple pipelines. If you run several
      etl pipelines in the same directory, pass --state-dir
      explicitly for each. This is critical when chaining with
      etl list: the inner etl list must read from one state dir
      and the outer pipeline must write to a different one.
      Example:
        etl dir -e "etl list dir --state-dir state-A" \
                --state-dir state-B
      Without explicit --state-dir on both sides, the pipelines
      would share state and silently skip each other's items.
  [ ] Clean old state. Use --clean-gt-days N or etl clean to
      prevent the state directory from growing unbounded. IDs
      seen in the current extractor output are protected from
      cleaning to prevent reprocessing.

VIBE CODING
  It is notoriously easy to vibe code an etl pipeline. Open up
  your command line agent, such as claude code or open code, and
  then do:

      ! mkdir my-pipeline
      ! etl --help
      make a pipeline in that folder that ...

  (! in claude executes a command and hands the output to the agent)
```
