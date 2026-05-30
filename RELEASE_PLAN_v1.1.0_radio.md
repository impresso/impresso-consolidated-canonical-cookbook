# Radio Integration Release Plan - v1.1.0

**Planned tag:** `v1.1.0`
**Release type:** Minor release
**Previous release:** `v1.0.0`
**Consolidated output version:** `v2025-12-04`
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
- untracked fixture/support files are excluded from the release;
- newspaper and radio Make dry runs pass;
- S3 bucket, provider/source, and langident run ID are confirmed.

## Release Commit Plan

Prepare one final release commit containing:

- `RELEASE_PROCESS.md`
- `RELEASE_PLAN_v1.1.0_radio.md`
- `RELEASE_NOTES_v1.1.0.md`
- documentation/configuration updates needed for radio/audio use

Suggested commit message:

```text
Prepare release v1.1.0
```

## Remaining Verification

Before tagging, run the newspaper dry run with the checked-in consolidated
configuration:

```bash
make -n newspaper \
  CFG=configs/config_consolidatedcanonical_v2025-12-04.mk \
  PROVIDER=BL \
  NEWSPAPER=WTCH
```

Run the audio dry run again after final documentation/config updates:

```bash
make -n newspaper \
  CFG=configs/config_consolidatedcanonical_v2026-05-26_audio.mk \
  PROVIDER=RTS \
  NEWSPAPER=RTS/ana_media
```

Only after dry-run review and explicit bucket/version confirmation, perform any
S3-affecting run.

## Required Pre-Tag Work

1. Confirm remaining release configuration values.
   - `LANGIDENT_ENRICHMENT_RUN_ID ?= langident-lid-ensemble_multilingual_v2-0-3`
   - `RUN_VERSION_CONSOLIDATEDCANONICAL ?= v2025-12-04` confirmed: radio output
     stays in the same consolidated format/version.
   - `PROVIDER=RTS`
   - `NEWSPAPER=RTS/ana_media`
   - `CANONICAL_INPUT_KIND := audios`

2. Write `RELEASE_NOTES_v1.1.0.md` before tagging.
   - Include overview, major features, pipeline changes, configuration,
     verification, migration notes, known issues, and full changelog link.
   - Full changelog link:
     `https://github.com/impresso/impresso-consolidated-canonical-cookbook/compare/v1.0.0...v1.1.0`

3. Add `CHANGELOG.md` or explicitly decide that `RELEASE_NOTES_v1.1.0.md` is the
   release changelog for this first post-`v1.0.0` release.

4. Update user-facing documentation for radio/audio use.
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

Newspaper Make dry run with the checked-in `v2025-12-04` configuration passed:

```bash
make -n newspaper \
  CFG=configs/config_consolidatedcanonical_v2025-12-04.mk \
  PROVIDER=BL \
  NEWSPAPER=WTCH
```

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

These local fixture/support paths are intentionally excluded from release
verification and release staging because they are not committed:

- `metadata/`
- `test_data/`
- `test.sh`
- `test_consolidation.sh`

Before tagging, do not stage these paths unless they are separately reviewed and
committed intentionally.

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
