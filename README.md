# Impresso Consolidated Canonical Processing Pipeline

This repository provides a Make-based processing pipeline for creating consolidated canonical newspaper data within the Impresso project ecosystem. It demonstrates best practices for building scalable, distributed newspaper processing workflows that merge canonical data with language identification and OCR quality assessment enrichments.

## Table of Contents

- [Overview](#overview)
- [Processing Pipeline](#processing-pipeline)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Data Requirements](#data-requirements)
- [Build System](#build-system)
- [Contributing](#contributing)
- [About Impresso](#about-impresso)

## Overview

This pipeline consolidates canonical newspaper data with language identification and OCR quality assessment enrichments to produce **consolidated canonical format** as defined in the Impresso schema (`issue.schema.json`).

### What is Consolidation?

Consolidation merges:

- **Canonical newspaper issues** (from `s3://112-canonical-final/`)
- **Language identification results** (from `s3://115-canonical-processed-final/langident/`)
- **OCR quality assessment scores** (included in langident results)

Into a unified format that includes:

- `consolidated_lg`: Computed language per content item
- `consolidated_ocrqa`: OCR quality score (0-1 range)
- `consolidated_langident_run_id`: Provenance tracking
- `consolidated_ts_original`: Original creation timestamp
- `lg_original`: Renamed from original `lg` field (if existed)

### Pipeline Features

- **Strict Matching**: Requires exact 1:1 correspondence between canonical content items and enrichment data
- **Horizontal Scalability**: Process data across multiple machines without conflicts
- **Large Dataset Handling**: Efficiently process large collections using S3 and local stamp files
- **Reproducibility**: Ensure reproducible results with proper dependency management and versioning
- **Parallel Processing**: Utilize multi-core systems and distributed computing
- **S3 Integration**: Seamlessly work with both local files and S3 storage

## Processing Pipeline

### Input Data

1. **Canonical Issues** (`s3://112-canonical-final/`):

   ```
   s3://112-canonical-final/PROVIDER/NEWSPAPER/issues/NEWSPAPER-YEAR-issues.jsonl.bz2
   ```

   - Contains newspaper issues with content items (articles, ads, images, etc.)
   - Organized by data provider (e.g., BL, SWA, NZZ)
   - Format: JSONL (one issue per line)

2. **Canonical Pages** (`s3://112-canonical-final/`):

   ```
   s3://112-canonical-final/PROVIDER/NEWSPAPER/pages/NEWSPAPER-YEAR/NEWSPAPER-YEAR-DATE-pages.jsonl.bz2
   ```

   - Contains page-level newspaper data organized by year directories
   - Organized by data provider matching issues structure
   - Format: JSONL (one page per line)

3. **Langident/OCRQA Enrichments** (`s3://115-canonical-processed-final/`):
   ```
   s3://115-canonical-processed-final/langident/langident-lid-ensemble_multilingual_v2-0-2/PROVIDER/NEWSPAPER/NEWSPAPER-YEAR.jsonl.bz2
   ```
   - Contains per-content-item language identification and OCR quality scores
   - Organized by data provider matching canonical structure
   - Format: JSONL (one content item per line)

### Processing Steps

1. **Data Synchronization**:

   - Downloads canonical issues from S3
   - Downloads canonical pages from S3
   - Downloads langident/OCRQA enrichments from S3
   - Uses stamp files to track sync status

2. **Consolidation**:

   **Issues Processing:**

   - For each issue file:
     - Loads all enrichment data into memory
     - Reads each issue line-by-line
     - For each content item:
       - Validates enrichment data exists (strict matching)
       - Renames `lg` → `lg_original`
       - Adds `consolidated_lg`, `consolidated_ocrqa`, `consolidated_langident_run_id`
     - Updates issue-level metadata:
       - Sets `consolidated = true`
       - Stores original `ts` in `consolidated_ts_original`
       - Updates `ts` to processing timestamp
     - Writes consolidated issue to output

   **Pages Processing:**

   - For each year of pages:
     - Copies all page files from canonical S3 to consolidated S3
     - Preserves directory structure and organization
     - Future versions may integrate additional data (e.g., ReOCR results)

3. **Output Upload**:
   - Uploads consolidated canonical issues to S3
   - Uploads consolidated canonical pages to S3
   - Preserves logs for troubleshooting

### Output Data

**Consolidated Canonical Issues** (`s3://118-canonical-consolidated-final/`):

```
s3://118-canonical-consolidated-final/VERSION/PROVIDER/NEWSPAPER/issues/NEWSPAPER-YEAR-issues.jsonl.bz2
```

- Format: JSONL (one issue per line)
- Schema: Conforms to `issue.schema.json` with `consolidated=true`
- Versioning: Uses date-based versioning (e.g., `v2025-11-23_initial`)
- Organization: Mirrors canonical structure with VERSION prefix

**Consolidated Canonical Pages** (`s3://118-canonical-consolidated-final/`):

```
s3://118-canonical-consolidated-final/VERSION/PROVIDER/NEWSPAPER/pages/NEWSPAPER-YEAR/NEWSPAPER-YEAR-DATE-pages.jsonl.bz2
```

- Format: JSONL (one page per line)
- Schema: Conforms to canonical pages schema
- Versioning: Uses same VERSION as issues
- Organization: Mirrors canonical pages structure with VERSION prefix

## Quick Start

Follow these steps to get started with the consolidation pipeline:

### 1. Prerequisites

Ensure you have the required system dependencies installed:

**Ubuntu/Debian:**

```bash
sudo apt-get update
sudo apt-get install -y make git git-lfs parallel coreutils python3 python3-pip

# Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
rm -rf aws awscliv2.zip
```

**macOS:**

```bash
# Install Homebrew if not already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install dependencies
brew install make git git-lfs parallel coreutils python3 awscli
```

**System Requirements:**

- Python 3.11+
- Make (GNU Make recommended)
- Git with git-lfs
- AWS CLI (for S3 access)

### 2. Clone and Setup

1. **Clone the repository:**

   ```bash
   git clone --recursive https://github.com/impresso/impresso-consolidated-canonical-cookbook.git
   cd impresso-consolidated-canonical-cookbook
   ```

2. **Configure environment:**

   ```bash
   cp dotenv.sample .env
   # Edit .env with your S3 credentials (see Configuration section below)
   ```

3. **Install Python dependencies:**

   ```bash
   # Using pipenv (recommended)
   pipenv install

   # Or using pip directly
   python3 -m pip install -r requirements.txt
   ```

4. **Initialize the environment:**

   ```bash
   make setup
   ```

5. **Create a configuration file (optional but recommended):**

   ```bash
   # Copy the sample configuration
   cp config.sample.mk config.local.mk

   # Edit config.local.mk with your settings
   # Set PROVIDER, NEWSPAPER, version identifiers, etc.
   ```

### 3. Verify Installation

Test your setup with a quick help command:

```bash
make help
```

You should see available targets and configuration options.

## Configuration

The pipeline can be configured in multiple ways, with increasing priority:

1. **Default values** in the Makefile includes
2. **Environment variables** from `.env` file
3. **Configuration file** (e.g., `config.local.mk`)
4. **Command-line arguments** to `make`

### Using Configuration Files

Configuration files provide a convenient way to manage different processing scenarios:

```bash
# Use default config.local.mk (if exists)
make newspaper PROVIDER=BL NEWSPAPER=WTCH

# Use a specific configuration file
make newspaper CFG=config.production.mk

# Use configuration file with command-line overrides
make newspaper CFG=config.bl.mk NEWSPAPER=AATA
```

**Create your configuration file:**

```bash
# Copy the sample
cp config.sample.mk config.local.mk

# Edit with your settings
vim config.local.mk
```

The configuration file can set any variable, including:

- `PROVIDER` and `NEWSPAPER` defaults
- S3 bucket names
- Version identifiers
- Parallelization settings
- Logging levels

**Example configuration files:**

```makefile
# config.bl.mk - British Library newspapers
PROVIDER := BL
LANGIDENT_ENRICHMENT_RUN_ID := langident-lid-ensemble_multilingual_v2-0-2
RUN_VERSION_CONSOLIDATEDCANONICAL := v2025-11-23_initial
COLLECTION_JOBS := 4

# config.production.mk - Production settings
PROVIDER := BL
S3_BUCKET_CONSOLIDATEDCANONICAL := 118-canonical-consolidated-final
RUN_VERSION_CONSOLIDATEDCANONICAL := v2025-11-23_production
LOGGING_LEVEL := INFO
COLLECTION_JOBS := 8
MAX_LOAD := 16
```

See `config.sample.mk` for a complete list of configurable variables.

## Environment Variables

Before running any processing, configure your environment:

### Required Environment Variables

Edit your `.env` file with these required settings:

```bash
# S3 Configuration (required)
SE_ACCESS_KEY=your_s3_access_key
SE_SECRET_KEY=your_s3_secret_key
SE_HOST_URL=https://os.zhdk.cloud.switch.ch/

# Logging Configuration (optional)
LOGGING_LEVEL=INFO
```

### Processing Configuration

These can be set in `.env` as shell variables or passed as command arguments to make:

**Required Variables:**

- `PROVIDER`: Data provider organization (e.g., `BL`, `SWA`, `NZZ`)
  - Can be omitted if `NEWSPAPER` includes provider prefix (e.g., `BL/WTCH`)
- `NEWSPAPER`: Target newspaper to process (e.g., `WTCH`, `actionfem`, or `BL/WTCH`)

**Filtering Variables:**

- `USE_CANONICAL`: Always set to `1` for consolidated canonical processing (default: `1`)
- `NEWSPAPER_HAS_PROVIDER`: Set to `1` if data organized as `PROVIDER/NEWSPAPER` (default: `1`)
- `NEWSPAPER_FNMATCH`: Pattern to filter newspapers for collection processing
  - Examples: `BL/*` (all BL newspapers), `SWA/*`, `*/WTCH`, `BL/AATA`
  - Leave empty to process all newspapers

**Optional Processing Variables:**

- `BUILD_DIR`: Local build directory (default: `build.d`)
- `RUN_VERSION_CONSOLIDATEDCANONICAL`: Version identifier (default: `v2025-11-23_initial`)
- `LANGIDENT_ENRICHMENT_RUN_ID`: Langident run to use (default: `langident-lid-ensemble_multilingual_v2-0-2`)
- `NPROC`: Number of CPU cores (auto-detected if not set)
- `NEWSPAPER_JOBS`: Number of parallel jobs per newspaper
- `COLLECTION_JOBS`: Number of newspapers to process in parallel (default: 2)
- `MAX_LOAD`: Maximum system load (default: NPROC)

### S3 Bucket Configuration

Configure S3 buckets in your paths file or via environment variables:

- `S3_BUCKET_CANONICAL`: Canonical input data bucket (default: `112-canonical-final`)
- `S3_BUCKET_LANGIDENT_ENRICHMENT`: Enrichment data bucket (default: `115-canonical-processed-final`)
- `S3_BUCKET_CONSOLIDATEDCANONICAL`: Output data bucket (default: `118-canonical-consolidated-final`)

## Running the Pipeline

### Understanding Sync Targets

The pipeline provides three sync targets for different purposes:

- **`sync-input`**: Downloads **source data** needed for processing

  - Canonical issues from `s3://112-canonical-final/`
  - Langident enrichments from `s3://115-canonical-processed-final/`
  - Run this before processing to ensure you have the latest input data

- **`sync-output`**: Downloads **already-processed results** from S3 to local

  - Consolidated canonical files from `s3://118-canonical-consolidated-final/`
  - Useful for inspection, verification, or resuming interrupted work
  - Does NOT reprocess data, only downloads existing files

- **`sync`**: Downloads **both input and output** data
  - Equivalent to running both `sync-input` and `sync-output`

**Note:** The `processing-target` automatically syncs input data (`sync-canonical` and `sync-langident`) before processing, so you typically don't need to run `sync-input` manually.

### Process a Single Newspaper

Process a newspaper to consolidate its canonical data with enrichments:

```bash
# Process a specific newspaper (PROVIDER and NEWSPAPER required)
make newspaper PROVIDER=BL NEWSPAPER=WTCH
```

### Step-by-Step Processing

**1. Sync input data (optional - processing-target does this automatically):**

```bash
# Download canonical issues and langident enrichments
make sync-input PROVIDER=BL NEWSPAPER=WTCH
```

**2. Run consolidation processing:**

```bash
# Process and upload results (automatically syncs input first)
make processing-target PROVIDER=BL NEWSPAPER=WTCH
```

**3. Sync output data (optional - for inspection/verification):**

```bash
# Download already-consolidated results from S3
make sync-output PROVIDER=BL NEWSPAPER=WTCH
```

### Process Multiple Newspapers

Process collections of newspapers using filtering patterns:

```bash
# Process all British Library newspapers
make collection NEWSPAPER_FNMATCH="BL/*" COLLECTION_JOBS=4

# Process all Swiss newspapers
make collection NEWSPAPER_FNMATCH="SWA/*" COLLECTION_JOBS=4

# Process all newspapers (use with caution - may be very large)
make collection COLLECTION_JOBS=8

# Use a configuration file for complex setups
make collection CFG=configs/config_consolidatedcanonical_v2025-11-23_initial.mk
```

### Flexible Provider Handling

The pipeline supports newspapers with or without provider prefixes:

```bash
# NEWSPAPER includes provider prefix
make newspaper NEWSPAPER=BL/WTCH

# NEWSPAPER and PROVIDER set separately
make newspaper PROVIDER=BL NEWSPAPER=WTCH

# For collections: filter by provider pattern
make collection NEWSPAPER_FNMATCH="BL/*"
```

### Available Commands

Explore the build system:

```bash
# Show all available targets
make help

# Show current configuration
make config

# Clean local build directory
make clean-build
```

## Data Requirements

### Strict Matching Policy

The consolidation pipeline implements **strict matching**:

- Every content item in a canonical issue **MUST** have corresponding enrichment data
- If any content item is missing from the enrichment file, processing **exits with an error**
- This ensures data consistency and prevents partial consolidation

### Expected Data Structure

**Canonical Issues:**

```json
{
  "id": "WTCH-1828-01-06-a",
  "ts": "2024-01-15T10:30:00Z",
  "i": [
    {
      "m": {
        "id": "WTCH-1828-01-06-a-i0001",
        "tp": "article",
        "lg": "en",
        ...
      }
    }
  ]
}
```

**Langident Enrichments:**

```json
{
  "id": "WTCH-1828-01-06-a-i0001",
  "lg": "en",
  "ocrqa": 0.92,
  "lg_decision": "all",
  "systems": {...}
}
```

**Consolidated Output:**

```json
{
  "id": "WTCH-1828-01-06-a",
  "consolidated": true,
  "consolidated_ts_original": "2024-01-15T10:30:00Z",
  "ts": "2025-11-23T14:20:00Z",
  "i": [
    {
      "m": {
        "id": "WTCH-1828-01-06-a-i0001",
        "tp": "article",
        "lg_original": "en",
        "consolidated_lg": "en",
        "consolidated_ocrqa": 0.92,
        "consolidated_langident_run_id": "langident-lid-ensemble_multilingual_v2-0-2",
        ...
      }
    }
  ]
}
```

## Build System

### Core Targets

- `make help`: Show available targets and current configuration
- `make setup`: Initialize environment (run once after installation)
- `make newspaper`: Process single newspaper consolidation
- `make collection`: Process multiple newspapers in parallel
- `make all`: Complete processing pipeline with data sync

### Data Management

- `make sync-input`: Download canonical issues and langident enrichments from S3
- `make sync-output`: Upload consolidated results to S3 (never overwrites existing data)
- `make sync`: Sync both input and output data
- `make clean-build`: Remove local build directory

### Parallel Processing

The system automatically detects CPU cores and configures parallel processing:

```bash
# Process collection with custom parallelization
make collection COLLECTION_JOBS=4 MAX_LOAD=8
```

### Build System Architecture

The build system uses:

- **Stamp Files**: Track processing state without downloading full datasets
- **S3 Integration**: Direct processing from/to S3 storage
- **Distributed Processing**: Multiple machines can work independently
- **Dependency Management**: Automatic dependency resolution via Make
- **Strict Validation**: Exits with error if data requirements are not met

For detailed build system documentation, see [cookbook/README.md](cookbook/README.md).

## Versioning

The consolidation pipeline uses date-based versioning for output:

```
vYYYY-MM-DD_INFO
```

Examples:

- `v2025-11-23_initial`: Initial run on November 23, 2025
- `v2025-11-23_rerun`: Rerun on the same date
- `v2025-12-01_fixed_bug`: Bug fix run on December 1, 2025

Set via environment or command-line:

```bash
make newspaper NEWSPAPER=WTCH RUN_VERSION_CONSOLIDATEDCANONICAL=v2025-11-23_test
```

## Troubleshooting

### Common Issues

**Missing enrichment data:**

```
ERROR: Missing enrichment data for content item: WTCH-1828-01-06-a-i0042
```

- Cause: Content item in canonical issue has no corresponding enrichment
- Solution: Ensure enrichment data is complete or update to latest enrichment run

**Schema validation errors:**

```
ERROR: Issue WTCH-1828-01-06-a missing both 'ts' and 'cdt' fields
```

- Cause: Input canonical data doesn't conform to expected schema
- Solution: Verify canonical data source and format

**S3 authentication errors:**

```
ERROR: Error reading enrichment file: Access Denied
```

- Cause: Invalid S3 credentials
- Solution: Check `.env` file and verify SE_ACCESS_KEY, SE_SECRET_KEY, SE_HOST_URL

### Log Files

Each processing run creates detailed logs:

```
build.d/.../NEWSPAPER-YEAR-issues.jsonl.bz2.log.gz
```

Logs are also uploaded to S3 alongside output files for troubleshooting distributed runs.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with `make newspaper NEWSPAPER=WTCH`
5. Submit a pull request

## About Impresso

### Impresso Project

[Impresso - Media Monitoring of the Past](https://impresso-project.ch) is an interdisciplinary research project that aims to develop and consolidate tools for processing and exploring large collections of media archives across modalities, time, languages and national borders.

The project is funded by:

- Swiss National Science Foundation (grants [CRSII5_173719](http://p3.snf.ch/project-173719) and [CRSII5_213585](https://data.snf.ch/grants/grant/213585))
- Luxembourg National Research Fund (grant 17498891)

### Copyright

Copyright (C) 2024 The Impresso team.

### License

This program is provided as open source under the [GNU Affero General Public License](https://github.com/impresso/impresso-pyindexation/blob/master/LICENSE) v3 or later.

---

<p align="center">
  <img src="https://github.com/impresso/impresso.github.io/blob/master/assets/images/3x1--Yellow-Impresso-Black-on-White--transparent.png?raw=true" width="350" alt="Impresso Project Logo"/>
</p>
