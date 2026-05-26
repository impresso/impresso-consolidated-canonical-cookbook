# AGENTS.md

## Scope

These instructions apply to the whole repository.

This repository is the Impresso consolidated canonical processing pipeline. It
wires the shared `cookbook/` make fragments to merge canonical issue data with
langident/OCRQA enrichments and copy canonical page data into consolidated
canonical output locations.

There is also a `cookbook/AGENT.md` for the reusable cookbook layer. Follow that
file when editing generic cookbook fragments or helpers under `cookbook/`.

## Command Policy

When running local project commands yourself, use `remake` instead of `make`.

Examples:

- `remake help`
- `remake setup`
- `remake -n newspaper PROVIDER=BL NEWSPAPER=WTCH`
- `remake collection CFG=configs/config_consolidatedcanonical_v2025-11-23_initial.mk`

When editing files intended for users, documentation, release notes, README
examples, Makefile help text, or shell snippets, write commands as `make`, not
`remake`.

Do not mention `remake` in public-facing documentation unless explicitly asked.

## Repository Layout

- `Makefile`: top-level pipeline entrypoint for consolidated canonical
  processing.
- `lib/cli_consolidatedcanonical.py`: main Python processor for issue
  consolidation.
- `lib/cli_pages_tx_diff.py`: utility CLI for page text differences.
- `cookbook/`: shared make fragments and Python helper package used by this and
  related Impresso processing repositories.
- `configs/`: checked-in processing configurations for named runs.
- `config.sample.mk` and `dotenv.sample`: local configuration templates.
- `metadata/`: media metadata used by the processor when `--metadata-json` is
  supplied.
- `test_data/`, `test_consolidation.sh`: small local sample workflow for the
  consolidation CLI.

Local build artifacts normally live under `build.d/`. Treat that directory as
transient unless a task explicitly asks otherwise.

## Build System

The build is GNU Make based and composed from fragments in `cookbook/`.

Important top-level targets:

- `help`: show available targets and configuration notes.
- `setup`: prepare local directories and environment checks.
- `sync`, `sync-input`, `sync-output`: synchronize S3-backed inputs and outputs.
- `processing-target`: run the consolidated canonical processing rules.
- `newspaper`: sync and process one newspaper.
- `all`: fresh input/output sync followed by processing.
- `collection`: process multiple newspapers via GNU parallel.

Configuration is driven by make variables, optional `.env`, optional
`config.local.mk`, and `CFG=<file>`. Do not commit private `.env`,
`config.local.mk`, `.aws/`, credentials, or local build output.

## Pipeline Semantics

Issue consolidation reads canonical issue JSONL plus langident/OCRQA JSONL and
writes consolidated issue JSONL. The current processor behavior is flexible:
content items without enrichment data are preserved and logged, not treated as a
fatal error. Images are skipped for consolidation fields.

The processor currently:

- preserves original `lg` or legacy `l` in `lg_original` while keeping schema-required `lg`;
- adds `consolidated_lg`, `consolidated_ocrqa`,
  `consolidated_char_len`, `consolidated_langident_run_id`, and
  `consolidated_reocr_applied` when enrichment data exists;
- removes empty optional string fields that would violate schema validation;
- moves legacy `var_t` values to issue-level `media_title_variant` when needed;
- optionally sets `media_title` from `--metadata-json`;
- sets `consolidated=true`, preserves the original timestamp in
  `consolidated_ts_original`, and updates `ts`;
- can validate against the remote Impresso canonical issue schema when
  `--validate` is passed.

Page processing currently copies canonical page files from canonical S3 paths to
consolidated S3 paths and creates local stamp files.

Some README sections may describe older strict matching semantics. Prefer the
current implementation in `lib/cli_consolidatedcanonical.py` when resolving
behavioral ambiguity.

## S3 And Stamps

This repository uses S3 paths plus local stamp files heavily. Make rules often
depend on stamp files rather than the full remote object content.

Do not replace stamp-based dependency logic with direct file existence checks or
plain copies unless the task specifically requires changing orchestration
semantics.

Use the existing helpers and path variables in `cookbook/paths_*.mk`,
`cookbook/sync_*.mk`, and `cookbook/local_to_s3.mk` rather than hard-coding S3
paths in new make rules.

## Python Conventions

Keep Python changes compatible with the existing CLI style:

- standard `argparse` CLIs;
- `smart_open` plus `impresso_cookbook.get_transport_params()` for local/S3
  paths;
- `impresso_cookbook.setup_logging()` for logs;
- explicit `sys.exit(1)` for fatal input or validation errors;
- JSONL streaming for canonical issue input and output.

Prefer focused changes in the processing CLI over broad refactors. The processor
handles large JSONL files, so avoid loading canonical issue files wholly into
memory. Loading yearly enrichment data into a dictionary is an existing design
choice.

## Verification

For fast local checks, prefer the sample CLI path:

```bash
python3 lib/cli_consolidatedcanonical.py \
  --canonical-input test_data/sample_canonical_issue.jsonl \
  --enrichment-input test_data/sample_enrichment.jsonl \
  --output test_data/output_consolidated.jsonl \
  --langident-run-id langident-lid-ensemble_multilingual_v2-0-2 \
  --metadata-json metadata/corpus_access_catalogue.json \
  --log-level INFO
```

Add `--validate` when schema access is available and schema compatibility is part
of the change being tested.

Use dry-run make checks before S3-affecting runs when possible:

```bash
make -n newspaper PROVIDER=BL NEWSPAPER=WTCH
```

S3 sync and upload targets require valid local credentials and can move large
amounts of data. Be explicit about provider, newspaper, version, and bucket
settings before running them.

## Editing Guidance

- Keep public documentation examples using `make`.
- Keep local-agent command examples using `remake`.
- Preserve the existing include order in `Makefile` and cookbook integration
  patterns.
- Use double-colon targets where existing cookbook fragments expect extension.
- Avoid unrelated cleanup in `cookbook/` while changing the top-level pipeline.
- Do not edit generated output or local sample output unless the task requires
  updating expected sample results.
