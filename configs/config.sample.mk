###############################################################################
# Sample Configuration File for Consolidated Canonical Processing
#
# Copy this file to create your own configuration:
#   cp config.sample.mk config.local.mk
#   # or
#   cp config.sample.mk config.production.mk
#
# Usage:
#   make newspaper                              # Uses config.local.mk (default)
#   make newspaper CFG=config.production.mk     # Uses specific config file
#
# This file is included after the default values are set, so you can override
# any USER-VARIABLE defined in the various .mk files.
###############################################################################


# REQUIRED CONFIGURATION
# =====================

# Flag to use canonical format (always 1 for consolidated canonical processing)
USE_CANONICAL ?= 1

# Flag indicating if newspapers are organized with PROVIDER level in S3
# Set to 1 for PROVIDER/NEWSPAPER structure (e.g., BL/WTCH)
# Set to 0 for direct NEWSPAPER structure (e.g., WTCH)
NEWSPAPER_HAS_PROVIDER ?= 1

# Pattern to filter newspapers for processing
# Examples:
#   BL/*           - Process all British Library newspapers
#   SWA/*          - Process all Swiss newspapers
#   */WTCH         - Process WTCH newspaper across all providers
#   BL/AATA        - Process only BL/AATA
#   *              - Process all newspapers (leave empty for all)
NEWSPAPER_FNMATCH ?= BL/*

# Data provider organization (e.g., BL, SWA, NZZ, INA)
# Can be empty if NEWSPAPER contains provider (e.g., BL/WTCH)
# Required if NEWSPAPER doesn't contain provider and NEWSPAPER_HAS_PROVIDER=1
PROVIDER := BL

# Default newspaper to process (can be overridden on command line)
# Can include provider prefix (e.g., BL/WTCH) or just newspaper name (e.g., WTCH)
NEWSPAPER := WTCH


# S3 BUCKET CONFIGURATION
# =======================

# Canonical input data bucket
# S3_BUCKET_CANONICAL := 112-canonical-final

# Langident enrichment data bucket
S3_BUCKET_LANGIDENT_ENRICHMENT := 115-canonical-processed-final

# Consolidated output data bucket
S3_BUCKET_consolidatedcanonical := 116-canonical-consolidated-sandbox


# PROCESSING CONFIGURATION
# ========================

# Langident enrichment run ID to use for consolidation
# This should match the run ID in the langident enrichment S3 path
LANGIDENT_ENRICHMENT_RUN_ID := langident-lid-ensemble_multilingual_v2-0-2

# Version identifier for consolidated output
# Format: vYYYY-MM-DD_INFO
# This will be used as the VERSION prefix in the output path:
# s3://118-canonical-consolidated-final/VERSION/PROVIDER/NEWSPAPER/
#
# IMPORTANT: The INFO suffix should describe the consolidation semantics,
# NOT the provider being processed. Good examples:
#   v2025-11-23_initial  - First consolidation run
#   v2025-11-23_final    - Final production version
#   v2025-11-23_test     - Testing consolidation
#   v2025-11-23_v2       - Second iteration
# BAD examples:
#   v2025-11-23_BL       - Don't use provider names
#   v2025-11-23_SWA      - Don't use provider names
RUN_VERSION_consolidatedcanonical := v2025-11-23_initial


# PARALLELIZATION SETTINGS
# ========================

# Number of newspapers to process in parallel for collection processing
# Higher values increase throughput but consume more memory
# Recommended: 2-8 depending on available RAM and CPU cores
COLLECTION_JOBS := 2

# Number of parallel jobs per newspaper
# Auto-calculated as NPROC / COLLECTION_JOBS if not set
# NEWSPAPER_JOBS := 4

# Maximum system load average to prevent system overload
# Set to lower value if system becomes unresponsive during processing
# MAX_LOAD := 16

# Override auto-detected CPU count if needed
# NPROC := 32


# BUILD CONFIGURATION
# ===================

# Local build directory
# BUILD_DIR := build.d

# Newspaper year sorting for processing order
# Options: shuf (random), cat (chronological), tac (reverse chronological)
# NEWSPAPER_YEAR_SORTING := shuf


# LOGGING CONFIGURATION
# =====================

# Logging level for Make and Python processing
# Options: DEBUG, INFO, WARNING, ERROR
LOGGING_LEVEL := INFO


# NEWSPAPER LIST CONFIGURATION
# ============================

# File containing list of newspapers to process for collection target
# One newspaper per line, or space-separated on one line
# NEWSPAPERS_TO_PROCESS_FILE := $(BUILD_DIR)/newspapers.txt

# Method for generating newspaper list from S3
# Options: shuf (random order), sort (alphabetical), cat (S3 order)
# NEWSPAPER_LIST_SORTING := shuf


# EXAMPLE PROVIDER-SPECIFIC CONFIGURATIONS
# ========================================

# Uncomment and adjust for different providers:

# British Library newspapers
ifeq ($(NEWSPAPER_FNMATCH),BL/*)
  LANGIDENT_ENRICHMENT_RUN_ID := langident-lid-ensemble_multilingual_v2-0-2
  # RUN_VERSION_consolidatedcanonical := v2025-11-23_initial
  # Process only BL newspapers
  NEWSPAPER_FNMATCH := BL/*
endif

# Swiss newspapers (SWA)
ifeq ($(NEWSPAPER_FNMATCH),SWA/*)
  LANGIDENT_ENRICHMENT_RUN_ID := langident-lid-ensemble_multilingual_v2-0-2
  # RUN_VERSION_consolidatedcanonical := v2025-11-23_initial
  NEWSPAPER_FNMATCH := SWA/*
endif

# French newspapers (INA)
ifeq ($(NEWSPAPER_FNMATCH),INA/*)
  LANGIDENT_ENRICHMENT_RUN_ID := langident-lid-ensemble_multilingual_v2-0-2
  # RUN_VERSION_consolidatedcanonical := v2025-11-23_initial
  NEWSPAPER_FNMATCH := INA/*
endif

# Process all providers
# ifeq ($(NEWSPAPER_FNMATCH),*)
#   LANGIDENT_ENRICHMENT_RUN_ID := langident-lid-ensemble_multilingual_v2-0-2
#   RUN_VERSION_consolidatedcanonical := v2025-11-23_all
# endif
