# Audio Integration Plan for Consolidated Canonical

## Location And Cross-Repo Context

This plan is for the consolidated canonical repository at:

```text
/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook
```

The originally mentioned path,
`/Users/siclemat/pj/2026/impresso/impresso-consolidated-canonical-cookbook`,
does not exist in the current filesystem. If you later work from a symlinked
checkout, keep this absolute 2025 path as the source-of-truth reference.

Related language-identification work is in:

```text
/Users/siclemat/pj/2026/impresso/impresso-language-identification-cookbook
```

Relevant files from that repo:

- `/Users/siclemat/pj/2026/impresso/impresso-language-identification-cookbook/configs/config-langidentocrqa_canonical-lid-ensemble_multilingual_v2-0-3.mk`
- `/Users/siclemat/pj/2026/impresso/impresso-language-identification-cookbook/lib/impresso_langident_systems.py`
- `/Users/siclemat/pj/2026/impresso/impresso-language-identification-cookbook/AUTO-INTEGRATION.md`

## Goal

Adapt consolidated canonical processing so radio canonical material is handled
beside newspaper page material with minimal media-specific branching.

The target radio path is:

1. Read canonical radio issues from `issues/`.
2. Read langident/OCRQA enrichment from the existing langident output path.
3. Write consolidated radio issues with the same `consolidated_*` fields as
   newspaper issues.
4. Copy canonical audio records from `audios/` to the consolidated canonical
   output, analogous to the current page-copy step.

The implementation should keep the shared flow:

- canonical issues are always consolidated by `lib/cli_consolidatedcanonical.py`
- canonical record files are copied by Make rules
- the selected record kind is controlled by `CANONICAL_INPUT_KIND`

## Current State

Repository instructions are in:

- `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/AGENTS.md`
- `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/cookbook/AGENT.md`

Important current files:

- `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/lib/cli_consolidatedcanonical.py`
- `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/cookbook/paths_canonical.mk`
- `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/cookbook/sync_canonical.mk`
- `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/cookbook/paths_consolidatedcanonical.mk`
- `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/cookbook/sync_consolidatedcanonical.mk`
- `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/cookbook/processing_consolidatedcanonical.mk`
- `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/configs/config_consolidatedcanonical_v2025-12-04.mk`

Observed state:

- `cookbook/paths_canonical.mk` already has `CANONICAL_INPUT_KIND`, `pages`,
  and `audios` path/stamp variables.
- `cookbook/sync_canonical.mk` already knows how to sync canonical `pages`,
  `audios`, or both in `auto` mode.
- `cookbook/paths_langident.mk` already uses `CANONICAL_PATH_SEGMENT` for
  canonical langident paths, which is required for paths such as
  `RTS/ana_media`.
- `cookbook/processing_consolidatedcanonical.mk` remains page-centric for
  dependency mapping and record-copy output.
- `cookbook/paths_consolidatedcanonical.mk` exposes consolidated `issues` and
  `pages`, but not `audios`.
- `cookbook/sync_consolidatedcanonical.mk` syncs consolidated issue outputs and
  consolidated pages output stamps, but not consolidated audios output stamps.

There are existing uncommitted changes in the consolidated repo. When
implementing later, do not revert unrelated local changes.

## Data Layout

Newspaper canonical input:

```text
s3://112-canonical-final/PROVIDER/NEWSPAPER/
├── issues/NEWSPAPER-YEAR-issues.jsonl.bz2
└── pages/NEWSPAPER-YEAR/*-pages.jsonl.bz2
```

Radio canonical input:

```text
s3://112-canonical-final/PROVIDER/SOURCE/
├── issues/SOURCE-YEAR-issues.jsonl.bz2
└── audios/SOURCE-YEAR/*-audios.jsonl.bz2
```

Example staging radio input:

```text
s3://111-canonical-staging/RTS/ana_media/
├── issues/ana_media-1996-issues.jsonl.bz2
└── audios/ana_media-1996/ana_media-1996-11-11-a-audios.jsonl.bz2
```

Langident/OCRQA enrichment from the language-identification cookbook:

```text
s3://115-canonical-processed-final/langident/langident-lid-ensemble_multilingual_v2-0-3/RTS/ana_media/ana_media-1996.jsonl.bz2
```

Consolidated canonical output should mirror canonical record kind:

```text
s3://118-canonical-consolidated-final/VERSION/RTS/ana_media/
├── issues/ana_media-1996-issues.jsonl.bz2
└── audios/ana_media-1996/ana_media-1996-11-11-a-audios.jsonl.bz2
```

## Design Decision

Use `CANONICAL_INPUT_KIND` consistently:

- `auto`: default; sync and process discovered canonical `pages` and/or
  `audios` stamps
- `pages`: force newspaper page record copying
- `audios`: force radio audio record copying

Do not infer media type from provider names. Providers can contain different
media types; the stable directory and filename conventions are the reliable
signals:

- `pages/*/*-pages.jsonl.bz2`
- `audios/*/*-audios.jsonl.bz2`

## Processor Changes

### `lib/cli_consolidatedcanonical.py`

This file is already mostly media-agnostic because it consolidates issue
metadata and does not read page/audio record files directly.

Required checks and small changes:

1. Keep applying `consolidated_lg`, `consolidated_ocrqa`,
   `consolidated_char_len`, and `consolidated_langident_run_id` to radio content
   item metadata exactly as for newspaper content item metadata.
2. Keep OCRQA naming as-is. OCRQA is a known-token-type ratio score and is valid
   for radio transcript text in this pipeline.
3. Avoid adding radio-specific ASR fields in this repo.
4. Review `olr` inference in `process_issue()`. It currently infers `olr` for
   missing issues based on page/article content types. For radio issues where
   `sm == "audio"`, avoid adding `olr`; `olr` is a print/page concept. Proposed:

   ```python
   if issue_data.get("sm") == "audio":
       issue_data.pop("olr", None)
   elif "olr" not in issue_data:
       # existing inference
   ```

5. Keep optional issue cleanup for `rc`, `rp`, and `provided_metadata`.
6. Add a small sample/fixture test with an `sm: "audio"` issue whose content
   item metadata uses `rr`, `speakers`, and radio content types.

Potential validation risk:

- `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/lib/cli_consolidatedcanonical.py`
  uses `Draft7Validator`, while the schema is draft 2020-12. This pre-existing
  issue may matter more when validating audio conditional rules. Consider
  switching to `jsonschema.validators.validator_for(schema_dict)` during
  implementation, but keep that as a focused validation fix.

## Make Path Changes

### `cookbook/paths_consolidatedcanonical.mk`

Add consolidated audio paths alongside pages:

```make
S3_PATH_CONSOLIDATEDCANONICAL_AUDIOS := s3://$(PATH_CONSOLIDATEDCANONICAL)/audios
LOCAL_PATH_CONSOLIDATEDCANONICAL_AUDIOS := $(LOCAL_PATH_CONSOLIDATEDCANONICAL)/audios
```

Add selected record output aliases:

```make
ifeq ($(CANONICAL_INPUT_KIND),audios)
LOCAL_PATH_CONSOLIDATEDCANONICAL_RECORDS := $(LOCAL_PATH_CONSOLIDATEDCANONICAL_AUDIOS)
S3_PATH_CONSOLIDATEDCANONICAL_RECORDS := $(S3_PATH_CONSOLIDATEDCANONICAL_AUDIOS)
else ifeq ($(CANONICAL_INPUT_KIND),pages)
LOCAL_PATH_CONSOLIDATEDCANONICAL_RECORDS := $(LOCAL_PATH_CONSOLIDATEDCANONICAL_PAGES)
S3_PATH_CONSOLIDATEDCANONICAL_RECORDS := $(S3_PATH_CONSOLIDATEDCANONICAL_PAGES)
endif
```

For `auto`, the processing file list can use both pages and audios stamps; a
single selected alias is less useful than media-specific mapping functions.

### `cookbook/sync_consolidatedcanonical.mk`

Add:

```make
LOCAL_CONSOLIDATEDCANONICAL_AUDIOS_SYNC_STAMP_FILE := $(LOCAL_PATH_CONSOLIDATEDCANONICAL_AUDIOS).last_synced
```

Add a sync rule mirroring the pages rule:

```make
$(LOCAL_CONSOLIDATEDCANONICAL_AUDIOS_SYNC_STAMP_FILE):
	mkdir -p $(@D) && \
	python -m impresso_cookbook.s3_to_local_stamps \
	   $(S3_PATH_CONSOLIDATEDCANONICAL_AUDIOS) \
	   --local-dir $(BUILD_DIR) \
	   --stamp-mode per-directory \
	   --remove-dangling-stamps \
	   --logfile $@.log.gz \
	   --log-level $(LOGGING_LEVEL) \
	&& touch $@
```

Update `sync-consolidatedcanonical` to depend on:

- issues sync stamp
- pages output sync stamp when `CANONICAL_INPUT_KIND=pages`
- audios output sync stamp when `CANONICAL_INPUT_KIND=audios`
- both pages and audios output sync stamps when `CANONICAL_INPUT_KIND=auto`

Update `clean-sync-consolidatedcanonical` to remove the audio sync marker and
logfile.

## Make Processing Changes

### `cookbook/processing_consolidatedcanonical.mk`

Replace page-only canonical stamp lists and mappings with record-kind-aware
versions.

Current page-only variables:

```make
LOCAL_CANONICAL_PAGES_STAMP_FILES
LOCAL_CONSOLIDATEDCANONICAL_ISSUE_FILES
LOCAL_CONSOLIDATEDCANONICAL_PAGES_STAMPS
```

Target variables:

```make
LOCAL_CANONICAL_RECORD_STAMP_FILES := $(LOCAL_CANONICAL_INPUT_STAMP_FILE_LIST)
LOCAL_CONSOLIDATEDCANONICAL_ISSUE_FILES := $(call LocalCanonicalRecordToConsolidatedIssueFile,$(LOCAL_CANONICAL_RECORD_STAMP_FILES))
LOCAL_CONSOLIDATEDCANONICAL_RECORD_STAMPS := $(call LocalCanonicalRecordToConsolidatedRecordStamp,$(LOCAL_CANONICAL_RECORD_STAMP_FILES))
```

Add path transformation functions:

```make
define LocalCanonicalRecordToConsolidatedIssueFile
$(patsubst $(LOCAL_PATH_CANONICAL_AUDIOS)/%.stamp,$(LOCAL_PATH_CONSOLIDATEDCANONICAL)/issues/%-issues.jsonl.bz2,$(patsubst $(LOCAL_PATH_CANONICAL_PAGES)/%.stamp,$(LOCAL_PATH_CONSOLIDATEDCANONICAL)/issues/%-issues.jsonl.bz2,$(1)))
endef

define LocalCanonicalRecordToEnrichmentFile
$(patsubst $(LOCAL_PATH_CANONICAL_AUDIOS)/%.stamp,$(LOCAL_PATH_LANGIDENT_ENRICHMENT)/%.jsonl.bz2,$(patsubst $(LOCAL_PATH_CANONICAL_PAGES)/%.stamp,$(LOCAL_PATH_LANGIDENT_ENRICHMENT)/%.jsonl.bz2,$(1)))
endef

define LocalCanonicalRecordToConsolidatedRecordStamp
$(patsubst $(LOCAL_PATH_CANONICAL_AUDIOS)/%.stamp,$(LOCAL_PATH_CONSOLIDATEDCANONICAL_AUDIOS)/%.stamp,$(patsubst $(LOCAL_PATH_CANONICAL_PAGES)/%.stamp,$(LOCAL_PATH_CONSOLIDATEDCANONICAL_PAGES)/%.stamp,$(1)))
endef
```

Then update:

```make
consolidatedcanonical-files-target: \
  $(LOCAL_CONSOLIDATEDCANONICAL_ISSUE_FILES) \
  $(LOCAL_CONSOLIDATEDCANONICAL_RECORD_STAMPS)
```

### Issue Consolidation Rules

The issue consolidation recipe currently depends only on
`$(LOCAL_PATH_CANONICAL_PAGES)/%.stamp`.

Add an audio version with the same command:

```make
$(LOCAL_PATH_CONSOLIDATEDCANONICAL)/issues/%-issues.jsonl.bz2: \
    $(LOCAL_PATH_CANONICAL_AUDIOS)/%.stamp \
    $(LOCAL_PATH_LANGIDENT_ENRICHMENT)/%.jsonl.bz2
	...
```

Both page and audio rules should call:

```make
python3 lib/cli_consolidatedcanonical.py \
  --canonical-input $(S3_PATH_CANONICAL_ISSUES)/$*-issues.jsonl.bz2 \
  --enrichment-input $(call LocalToS3,$(word 2,$^)) \
  --output $@ \
  --langident-run-id $(LANGIDENT_ENRICHMENT_RUN_ID) \
  ...
```

No processor flag is needed because the issue JSON tells the processor whether
the issue is print/typescript/audio through `sm`.

### Record Copy Rules

Keep the existing page copy rule.

Add an audio copy rule:

```make
$(LOCAL_PATH_CONSOLIDATEDCANONICAL_AUDIOS)/%.stamp: \
    $(LOCAL_PATH_CANONICAL_AUDIOS)/%.stamp
	$(MAKE_SILENCE_RECIPE) \
	mkdir -p $(@D) && \
	AWS_CONFIG_FILE=.aws/config AWS_SHARED_CREDENTIALS_FILE=.aws/credentials aws s3 cp \
		--recursive \
		--endpoint-url $(SE_HOST_URL) \
		$(S3_PATH_CANONICAL_AUDIOS)/$*/ \
		$(S3_PATH_CONSOLIDATEDCANONICAL_AUDIOS)/$*/ \
	&& touch $@
```

This copies:

```text
s3://112-canonical-final/RTS/ana_media/audios/ana_media-1996/
```

to:

```text
s3://118-canonical-consolidated-final/VERSION/RTS/ana_media/audios/ana_media-1996/
```

## Configuration Changes

Add a radio-capable config, for example:

```text
/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/configs/config_consolidatedcanonical_v2026-05-26_audio.mk
```

Suggested defaults:

```make
USE_CANONICAL ?= 1
NEWSPAPER_HAS_PROVIDER ?= 1
CANONICAL_INPUT_KIND ?= auto
S3_BUCKET_CANONICAL ?= 112-canonical-final
S3_BUCKET_LANGIDENT_ENRICHMENT ?= 115-canonical-processed-final
S3_BUCKET_CONSOLIDATEDCANONICAL ?= 118-canonical-consolidated-final
LANGIDENT_ENRICHMENT_RUN_ID ?= langident-lid-ensemble_multilingual_v2-0-3
RUN_VERSION_CONSOLIDATEDCANONICAL ?= v2026-05-26_audio
CONSOLIDATEDCANONICAL_VALIDATE_OPTION ?= --validate
```

For staging tests, override canonical input bucket at runtime:

```bash
make -n newspaper \
  CFG=configs/config_consolidatedcanonical_v2026-05-26_audio.mk \
  S3_BUCKET_CANONICAL=111-canonical-staging \
  PROVIDER=RTS \
  NEWSPAPER=ana_media \
  CANONICAL_INPUT_KIND=audios
```

## Test Plan

### 1. CLI Unit/Fixture Test

Create a minimal local audio issue fixture:

- issue has `sm: "audio"` and `st: "radio_broadcast"`
- top-level `rr` points to audio record IDs
- content item metadata has `rr`, `tp`, `lg`, optional `speakers`
- enrichment JSONL has matching `id`, `lg`, `ocrqa`, `len`

Run:

```bash
python3 lib/cli_consolidatedcanonical.py \
  --canonical-input test_data/audio_issue.jsonl \
  --enrichment-input test_data/audio_enrichment.jsonl \
  --output test_data/audio_output_consolidated.jsonl \
  --langident-run-id langident-lid-ensemble_multilingual_v2-0-3 \
  --log-level INFO
```

Expected:

- issue gets `consolidated=true`
- content item metadata gets `lg_original`, `consolidated_lg`,
  `consolidated_ocrqa`, `consolidated_char_len`,
  `consolidated_langident_run_id`, `consolidated_reocr_applied`
- `rr`, `rc`, `rp`, `speakers`, and `provided_metadata` are preserved
- `olr` is not added to audio issues

### 2. Make Dry Run With Temporary Stamps

Create temporary local stamps under `/private/tmp` and dry-run:

```bash
make -n consolidatedcanonical-files-target \
  CFG=configs/config_consolidatedcanonical_v2026-05-26_audio.mk \
  BUILD_DIR=/private/tmp/consolidated-audio-test \
  S3_BUCKET_CANONICAL=111-canonical-staging \
  PROVIDER=RTS \
  NEWSPAPER=ana_media \
  CANONICAL_INPUT_KIND=audios
```

Expected command fragments:

```text
--canonical-input s3://111-canonical-staging/RTS/ana_media/issues/ana_media-1996-issues.jsonl.bz2
--enrichment-input s3://115-canonical-processed-final/langident/langident-lid-ensemble_multilingual_v2-0-3/RTS/ana_media/ana_media-1996.jsonl.bz2
s3://111-canonical-staging/RTS/ana_media/audios/ana_media-1996/
s3://118-canonical-consolidated-final/VERSION/RTS/ana_media/audios/ana_media-1996/
```

### 3. Single Radio Source End-to-End Dry Run

```bash
make -n newspaper \
  CFG=configs/config_consolidatedcanonical_v2026-05-26_audio.mk \
  S3_BUCKET_CANONICAL=111-canonical-staging \
  PROVIDER=RTS \
  NEWSPAPER=ana_media \
  CANONICAL_INPUT_KIND=audios
```

### 4. Newspaper Regression Dry Run

```bash
make -n newspaper \
  CFG=configs/config_consolidatedcanonical_v2026-05-26_audio.mk \
  PROVIDER=BNL \
  NEWSPAPER=armeteufel \
  CANONICAL_INPUT_KIND=pages
```

The page path should remain unchanged except for the selected config/run
version.

## Implementation Order

1. Update `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/lib/cli_consolidatedcanonical.py`
   so audio issues do not receive inferred `olr`.
2. Add consolidated audio paths to
   `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/cookbook/paths_consolidatedcanonical.mk`.
3. Add consolidated audio output sync to
   `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/cookbook/sync_consolidatedcanonical.mk`.
4. Generalize record stamp mapping in
   `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/cookbook/processing_consolidatedcanonical.mk`.
5. Add audio issue consolidation and audio record copy rules in
   `processing_consolidatedcanonical.mk`.
6. Add a radio-capable config under
   `/Users/siclemat/pj/2025/impresso/impresso-consolidated-canonical-cookbook/configs/`.
7. Add minimal `test_data/` audio fixtures and run the CLI fixture test.
8. Run Make dry-runs for RTS audio and BNL pages.

## Open Questions

- Which consolidated output version should be used for the first radio-capable
  run: date-based `v2026-05-26_audio`, or a broader production release name?
- Should `CANONICAL_INPUT_KIND=auto` sync both `pages` and `audios`, or should
  it probe S3 first and sync only the detected record kind? Current shared
  cookbook behavior syncs both.
- Should validation be kept enabled for first radio tests, given the schema and
  validator draft mismatch risk?
