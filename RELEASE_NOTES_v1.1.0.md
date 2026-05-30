# Release Notes - v1.1.0

**Release Date:** 2026-05-30
**Tag:** `v1.1.0`
**Status:** Stable
**Consolidated output version:** `v2025-12-04`

## Overview

This release adds radio/audio support to the consolidated canonical processing
pipeline while preserving the existing newspaper/page workflow and consolidated
canonical output format.

Radio canonical issues can now be consolidated with langident/OCRQA enrichment
and canonical audio records can be copied to the consolidated canonical output
under the same `v2025-12-04` output version.

## Major Features

- Added support for canonical audio record workflows using
  `CANONICAL_INPUT_KIND := audios`.
- Added an RTS radio configuration:
  `configs/config_consolidatedcanonical_v2026-05-26_audio.mk`.
- Added audio-aware consolidated canonical path and stamp handling through the
  cookbook submodule update.
- Preserved radio issue/content-item fields such as `rc`, `rp`, `rr`,
  `speakers`, and `provided_metadata`.
- Removed print-only `olr` from audio issues during consolidation.

## Pipeline Changes

- Consolidated issue processing remains shared for newspaper and radio material.
- Page and audio record copying now use the selected canonical input kind.
- Consolidated audio records are written under:

  ```text
  s3://118-canonical-consolidated-final/v2025-12-04/RTS/ana_media/audios/
  ```

- Consolidated radio issues are written under:

  ```text
  s3://118-canonical-consolidated-final/v2025-12-04/RTS/ana_media/issues/
  ```

- Added `lib/cli_pages_tx_diff.py` for comparing line text between matching
  canonical pages on S3.

## Configuration

Radio release configuration:

```make
CFG=configs/config_consolidatedcanonical_v2026-05-26_audio.mk
PROVIDER=RTS
NEWSPAPER=RTS/ana_media
CANONICAL_INPUT_KIND := audios
LANGIDENT_ENRICHMENT_RUN_ID ?= langident-lid-ensemble_multilingual_v2-0-3
RUN_VERSION_CONSOLIDATEDCANONICAL ?= v2025-12-04
```

The git release is `v1.1.0`. The consolidated canonical S3 output version remains
`v2025-12-04` because the output format remains unchanged.

## Verification

Syntax check passed:

```bash
.venv/bin/python -m py_compile lib/cli_consolidatedcanonical.py lib/cli_pages_tx_diff.py
```

Newspaper Make dry run with the checked-in `v2025-12-04` configuration passed:

```bash
make -n newspaper \
  CFG=configs/config_consolidatedcanonical_v2025-12-04.mk \
  PROVIDER=BL \
  NEWSPAPER=WTCH
```

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

## Breaking Changes

None. Existing newspaper/page consolidation workflows remain supported.

## Migration Notes

- Existing newspaper/page users do not need to change configuration.
- Radio/audio users should use
  `configs/config_consolidatedcanonical_v2026-05-26_audio.mk` or set
  `CANONICAL_INPUT_KIND := audios` in their own configuration.
- Radio/audio runs should use langident enrichment
  `langident-lid-ensemble_multilingual_v2-0-3`.

## Known Issues

- S3 processing/upload targets still require valid local S3 credentials and
  should be dry-run reviewed before execution.
- Schema validation should be rerun with `--validate` when schema compatibility
  is part of release acceptance.
- Untracked local fixture files are excluded from this release.

## Links

- Full Changelog:
  https://github.com/impresso/impresso-consolidated-canonical-cookbook/compare/v1.0.0...v1.1.0
- Release Process: `RELEASE_PROCESS.md`
- Release Plan: `RELEASE_PLAN_v1.1.0_radio.md`

## Contributors

- Impresso project contributors
