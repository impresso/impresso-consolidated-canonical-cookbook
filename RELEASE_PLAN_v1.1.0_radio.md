# Radio Integration Release Plan - v1.1.0

**Planned tag:** `v1.1.0`
**Release type:** Minor release
**Previous release:** `v1.0.0`
**Current branch:** `integrate-radio`
**Target repository:** `impresso/impresso-consolidated-canonical-cookbook`
**Date prepared:** 2026-05-30
**Order:** reverse chronological, with final release actions first.

## Tag And Publish Plan

After the release commit is made:

```bash
git tag -a v1.1.0 -m "Release v1.1.0: radio audio integration"
git push origin v1.1.0
```

Publish the GitHub release from committed release notes:

```bash
gh release create v1.1.0 \
  --repo impresso/impresso-consolidated-canonical-cookbook \
  --title "v1.1.0: Radio audio integration" \
  --notes-file RELEASE_NOTES_v1.1.0.md
```

## Go/No-Go Criteria

Release is ready to tag when:

- release notes are committed;
- top-level release process and radio release plan are committed;
- README/config examples accurately describe radio/audio use;
- untracked fixture/support files are either intentionally tracked or excluded;
- standard and audio smoke tests pass in `.venv`;
- newspaper and radio Make dry runs pass;
- S3 bucket, provider/source, langident run ID, and output version are confirmed.

## Release Commit Plan

Prepare one final release commit containing:

- `RELEASE_PROCESS.md`
- `RELEASE_PLAN_v1.1.0_radio.md`
- `RELEASE_NOTES_v1.1.0.md`
- documentation/configuration updates needed for radio/audio use
- any committed test fixtures required for repeatable smoke tests

Suggested commit message:

```text
Prepare release v1.1.0
```

## Remaining Verification

Before tagging, run:

```bash
make -n newspaper PROVIDER=BL NEWSPAPER=WTCH
```

Run the audio dry run again after final documentation/config updates:

```bash
make -n newspaper \
  CFG=configs/config_consolidatedcanonical_v2026-05-26_audio.mk \
  PROVIDER=RTS \
  NEWSPAPER=RTS/ana_media
```

If schema compatibility is part of the release acceptance criteria, run the
standard and audio fixture smoke tests with `--validate`, using the repository
virtualenv.

Only after dry-run review and explicit bucket/version confirmation, perform any
S3-affecting run.

## Required Pre-Tag Work

1. Confirm whether `RUN_VERSION_CONSOLIDATEDCANONICAL` should remain
   `v2025-12-04` for the radio run or be bumped to a radio-specific consolidated
   output version before release.

2. Confirm release configuration values.
   - `LANGIDENT_ENRICHMENT_RUN_ID ?= langident-lid-ensemble_multilingual_v2-0-3`
   - `RUN_VERSION_CONSOLIDATEDCANONICAL ?= v2025-12-04`
   - `PROVIDER=RTS`
   - `NEWSPAPER=RTS/ana_media`
   - `CANONICAL_INPUT_KIND := audios`

3. Write `RELEASE_NOTES_v1.1.0.md` before tagging.
   - Include overview, major features, pipeline changes, configuration,
     verification, migration notes, known issues, and full changelog link.
   - Full changelog link:
     `https://github.com/impresso/impresso-consolidated-canonical-cookbook/compare/v1.0.0...v1.1.0`

4. Add `CHANGELOG.md` or explicitly decide that `RELEASE_NOTES_v1.1.0.md` is the
   release changelog for this first post-`v1.0.0` release.

5. Update user-facing documentation for radio/audio use.
   - Add `CANONICAL_INPUT_KIND := audios`.
   - Document input layout under canonical `audios/`.
   - Document output layout under consolidated `audios/`.
   - Mention the RTS example configuration.
   - Correct stale README language that still describes strict enrichment
     matching if it conflicts with current processor behavior.

## Verification Already Run

Audio Make dry run passed:

```bash
make -n newspaper \
  CFG=configs/config_consolidatedcanonical_v2026-05-26_audio.mk \
  PROVIDER=RTS \
  NEWSPAPER=RTS/ana_media
```

Result: dry run selected consolidated issue outputs for `ana_media-1996` and
`ana_media-1997`, and selected consolidated audio stamps under
`build.d/118-canonical-consolidated-final/v2025-12-04/RTS/ana_media/audios/`.

Audio fixture smoke test passed:

```bash
.venv/bin/python lib/cli_consolidatedcanonical.py \
  --canonical-input test_data/audio_issue.jsonl \
  --enrichment-input test_data/audio_enrichment.jsonl \
  --output /private/tmp/output_audio_release_smoke.jsonl \
  --langident-run-id langident-lid-ensemble_multilingual_v2-0-3 \
  --log-level INFO
```

Result: 1 audio issue processed, 1 content item consolidated, 0 skipped. The
output kept `sm=audio`, `rc`, `rp`, `rr`, `speakers`, and
`provided_metadata`, and did not include `olr`.

Standard issue smoke test passed:

```bash
.venv/bin/python lib/cli_consolidatedcanonical.py \
  --canonical-input test_data/sample_canonical_issue.jsonl \
  --enrichment-input test_data/sample_enrichment.jsonl \
  --output /private/tmp/output_consolidated_release_smoke.jsonl \
  --langident-run-id langident-lid-ensemble_multilingual_v2-0-2 \
  --metadata-json metadata/corpus_access_catalogue.json \
  --log-level INFO
```

Result: 1 issue processed, 2 content items consolidated, 0 skipped.

Syntax check passed:

```bash
.venv/bin/python -m py_compile lib/cli_consolidatedcanonical.py lib/cli_pages_tx_diff.py
```

## Current Change Inventory

Compared with `v1.0.0`, the branch contains 33 commits. The important top-level
changes are:

- `.gitmodules`
- `AGENTS.md`
- `AUDIO-INTEGRATION-PLAN.md`
- `Makefile`
- `README.md`
- `configs/config_consolidatedcanonical_v2025-11-23_initial.mk`
- `configs/config_consolidatedcanonical_v2025-12-04.mk`
- `configs/config_consolidatedcanonical_v2026-05-26_audio.mk`
- `cookbook` submodule pointer
- `lib/cli_consolidatedcanonical.py`
- `lib/cli_pages_tx_diff.py`
- `requirements.txt`
- `RELEASE_PROCESS.md`

There are currently untracked local fixture/support paths:

- `metadata/`
- `test_data/`
- `test.sh`
- `test_consolidation.sh`

Before tagging, decide whether these are release fixtures that should be tracked
or local-only files that should remain outside the release commit. The smoke tests
above use `metadata/` and `test_data/`, so release verification should either
track the required fixtures or replace the checks with committed fixtures.

## Release Scope

Include the radio integration work currently on `integrate-radio`:

- audio-aware consolidated canonical Make processing;
- consolidated canonical audio path and stamp handling through the cookbook
  submodule update;
- `configs/config_consolidatedcanonical_v2026-05-26_audio.mk`;
- radio issue handling in `lib/cli_consolidatedcanonical.py`, including removal
  of print-only `olr` from audio issues;
- preservation of radio fields such as `rc`, `rp`, `rr`, `speakers`, and
  `provided_metadata`;
- `lib/cli_pages_tx_diff.py` line comparison utility already present since
  `v1.0.0`;
- release process documentation in `RELEASE_PROCESS.md`.

Do not include private credentials, local build output, or unrelated transient
files.

## Rationale

Radio/audio support is a backwards-compatible feature addition to the
consolidated canonical pipeline. It adds support for canonical `audios/` records,
radio issue cleanup, and an RTS audio configuration while preserving the existing
newspaper/page workflow. Under the adapted release process in `RELEASE_PROCESS.md`,
this is a minor release: `v1.0.0` -> `v1.1.0`.
