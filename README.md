# Waterfall-Enrichment (Rust)

A Rust port of the LeadIntel lead enrichment pipeline: a CLI that ingests
leads from CSV, scores each one with a doubt score, and runs it through a
cost-tiered waterfall of enrichment stages until the doubt score drops low
enough or the budget runs out.

The Python original lives in
[waterfall-enrichment-python](https://github.com/JessicaTemplet/waterfall-enrichment-python).
This is a line-by-line port of that repo's `leadintel/core/` reference
implementation, module for module: every source file here has a Python
equivalent noted in its doc comments, and `main.rs` maps each CLI command
back to the typer function it replaces.

## How the waterfall works

`doubt.py`'s scoring carries over unchanged: a lead with no observations
scores 1.0, missing a title costs 0.4, conflicting titles cost 0.3, missing
an email costs 0.3. `pipeline.yaml` at the repo root maps doubt thresholds to
stages:

```
doubt score          stage        cost
-----------------------------------------
> 0.5      ----->    shallow      2c
> 0.2      ----->    waterfall    8c   (tries up to 4 vendor sources)
> 0.1      ----->    agent        15c  (deep-research fallback)
0.0        ----->    done
```

## CLI

```sh
cargo build --release
./target/release/leadintel initdb
./target/release/leadintel ingest leads.csv
./target/release/leadintel run
```

| Flag | Meaning |
|---|---|
| `--db <path>` | SQLite database file, defaults to `leadintel.db` |
| `--redis <url>` | Redis connection URL, defaults to `redis://127.0.0.1:6379` |
| `--pipeline <path>` | Path to the pipeline config, defaults to `pipeline.yaml` |

`ingest` expects a CSV with `name` and `company` columns; anything else is
ignored. `run` initializes the schema if needed, then starts the consumer,
scheduler, and lead enqueue and polls until every lead is `DONE`.

## What's ported, and how

- **`db.rs` / `models.rs` / `repository.rs`**: rusqlite in place of SQLAlchemy,
  hand-written SQL instead of an ORM.
- **`config.rs`**: `serde_yaml` deserializes `pipeline.yaml` into a typed
  struct rather than the Python version's plain dicts, so a typo in the YAML
  fails at startup instead of mid-pipeline.
- **`worker.rs` / `job/`**: the Redis-backed job queue (producer, consumer,
  scheduler) ported from the Python execution engine, using the `redis` crate
  with `tokio-comp` for async.
- **`main.rs`**: `clap` derive replaces typer; `#[tokio::main]` replaces
  `asyncio.run(...)`.

## Dependencies

tokio (async runtime), redis (`tokio-comp`), rusqlite (`bundled`), serde /
serde_json / serde_yaml, uuid, chrono, rand (mock vendor responses), clap,
anyhow, csv.

## Status

Same as the Python side: the vendor enrichment sources are mocked, not live
API calls. This port covers the pipeline mechanics (doubt scoring, staging,
budget, job queue); wiring in real vendors is the next step on either
implementation.
