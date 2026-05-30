###############################################################################
# Radio-capable consolidated canonical processing configuration
#
# Usage:
#   make -n newspaper \
#     CFG=configs/config_consolidatedcanonical_v2026-05-26_audio.mk \
#     PROVIDER=RTS \
#     NEWSPAPER=RTS/ana_media
###############################################################################

USE_CANONICAL ?= 1
NEWSPAPER_HAS_PROVIDER ?= 1
CANONICAL_INPUT_KIND := audios
PROVIDER := RTS
NEWSPAPER ?= RTS/ana_media
NEWSPAPER_FNMATCH := RTS/ana_media
#NEWSPAPERS_TO_PROCESS_FILE ?= $(BUILD_DIR)/newspapers-rts-audio.txt

S3_BUCKET_CANONICAL := 112-canonical-final
S3_PREFIX_NEWSPAPERS_TO_PROCESS_BUCKET ?= $(S3_BUCKET_CANONICAL)
S3_BUCKET_LANGIDENT_STAGE1 ?= 115-canonical-processed-final
S3_BUCKET_LANGIDENT ?= 115-canonical-processed-final
S3_BUCKET_LANGIDENT_ENRICHMENT ?= $(S3_BUCKET_LANGIDENT)
S3_BUCKET_CONSOLIDATEDCANONICAL ?= 118-canonical-consolidated-final

LANGIDENT_ENRICHMENT_RUN_ID ?= langident-lid-ensemble_multilingual_v2-0-3
RUN_ID_LANGIDENT ?= $(LANGIDENT_ENRICHMENT_RUN_ID)
RUN_VERSION_CONSOLIDATEDCANONICAL ?= v2025-12-04

CONSOLIDATEDCANONICAL_VALIDATE_OPTION ?= --validate
LOGGING_LEVEL ?= INFO
