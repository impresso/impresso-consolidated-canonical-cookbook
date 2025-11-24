#!/usr/bin/env python3
"""
Consolidated Canonical Processor

This module merges canonical newspaper data with language identification and OCR
quality assessment enrichments to produce consolidated canonical format.

The consolidation process:

1. **Reads Input Data**: Loads canonical issue data (JSONL) and corresponding
   langident/OCRQA enrichment data from S3 or local files.

2. **Strict Matching**: Requires exact 1:1 correspondence between content items
   in canonical issues and enrichment data. Exits with error if any CI is missing
   from enrichment data.

3. **Field Transformations**:
   - Renames `lg` → `lg_original` (if exists in canonical)
   - Adds `consolidated_lg` from enrichment `lg` field
   - Adds `consolidated_ocrqa` from enrichment `ocrqa` field
   - Adds `consolidated_langident_run_id` from run configuration
   - Sets `consolidated` flag to `true`
   - Stores original `ts` in `consolidated_ts_original`
   - Updates `ts` to current processing timestamp

4. **Schema Compliance**: Produces output conforming to the consolidated canonical
   issue schema with all required `consolidated_*` properties.

Usage:
    $ python cli_consolidatedcanonical.py \
        --canonical-input s3://12-canonical-final/BL/WTCH/issues/... \
        --enrichment-input s3://115-canonical-processed-final/... \
        --output s3://140-processed-data-sandbox/.../WTCH/... \
        --langident-run-id langident-lid-ensemble_multilingual_v2-0-2 \
        --log-level INFO

Schema Reference:
    The output conforms to issue.schema.json with these consolidated properties:
    - consolidated: boolean (true)
    - consolidated_ts_original: string (original timestamp)
    - consolidated_lg: string (computed language)
    - consolidated_langident_run_id: string
    - consolidated_ocrqa: number (0-1)
    
    Per content item metadata:
    - lg_original: string|null (original language if existed)
    - consolidated_lg: string|null (computed language)
    - consolidated_ocrqa: number
    - consolidated_langident_run_id: string
"""

import logging
import argparse
import json
import sys
from typing import Dict, List, Optional, Any
from smart_open import open as smart_open  # type: ignore

from impresso_cookbook import (  # type: ignore
    get_s3_client,
    get_timestamp,
    setup_logging,
    get_transport_params,
)

log = logging.getLogger(__name__)


def parse_arguments(args: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parse command-line arguments.

    Args:
        args: Command-line arguments (uses sys.argv if None)

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Consolidate canonical issues with langident/OCRQA enrichments."
    )
    parser.add_argument(
        "--log-file", dest="log_file", help="Write log to FILE", metavar="FILE"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: %(default)s)",
    )
    parser.add_argument(
        "--canonical-input",
        dest="canonical_input",
        help="Canonical issue input file (JSONL, required)",
        required=True,
    )
    parser.add_argument(
        "--enrichment-input",
        dest="enrichment_input",
        help="Langident/OCRQA enrichment input file (JSONL, required)",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        help="Consolidated canonical output file (JSONL, required)",
        required=True,
    )
    parser.add_argument(
        "--langident-run-id",
        dest="langident_run_id",
        help="Langident run ID for provenance tracking (required)",
        required=True,
    )
    return parser.parse_args(args)


class ConsolidatedCanonicalProcessor:
    """
    Processor that merges canonical issues with langident/OCRQA enrichments.

    Implements strict matching: all content items in canonical must have
    corresponding enrichment data, or processing fails with error.
    """

    def __init__(
        self,
        canonical_input: str,
        enrichment_input: str,
        output_file: str,
        langident_run_id: str,
        log_level: str = "INFO",
        log_file: Optional[str] = None,
    ) -> None:
        """
        Initialize the ConsolidatedCanonicalProcessor.

        Args:
            canonical_input: Path to canonical issue file (S3 or local)
            enrichment_input: Path to langident/OCRQA enrichment file (S3 or local)
            output_file: Path to output consolidated file (S3 or local)
            langident_run_id: Run ID for langident provenance
            log_level: Logging level (default: "INFO")
            log_file: Path to log file (default: None)
        """
        self.canonical_input = canonical_input
        self.enrichment_input = enrichment_input
        self.output_file = output_file
        self.langident_run_id = langident_run_id
        self.log_level = log_level
        self.log_file = log_file

        # Configure the module-specific logger
        setup_logging(self.log_level, self.log_file, logger=log)

        # Initialize S3 client and timestamp
        self.s3_client = get_s3_client()
        self.timestamp = get_timestamp()

        log.info(f"Initialized processor with timestamp: {self.timestamp}")
        log.info(f"Langident run ID: {self.langident_run_id}")

    def load_enrichments(self) -> Dict[str, Dict[str, Any]]:
        """
        Load enrichment data from langident/OCRQA file.

        Returns:
            Dictionary mapping content item IDs to enrichment data

        Raises:
            SystemExit: If enrichment file cannot be read
        """
        enrichments = {}

        log.info(f"Loading enrichments from: {self.enrichment_input}")

        try:
            with smart_open(
                self.enrichment_input,
                "rt",
                encoding="utf-8",
                transport_params=get_transport_params(self.enrichment_input),
            ) as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        data = json.loads(line)
                        ci_id = data.get("id")

                        if not ci_id:
                            log.error(f"Enrichment line {line_num} missing 'id' field")
                            sys.exit(1)

                        enrichments[ci_id] = {
                            "lg": data.get("lg"),
                            "ocrqa": data.get("ocrqa"),
                            "lg_decision": data.get("lg_decision"),
                            "systems": data.get("systems", {}),
                            "alphabetical_ratio": data.get("alphabetical_ratio"),
                        }

                    except json.JSONDecodeError as e:
                        log.error(f"Invalid JSON in enrichment line {line_num}: {e}")
                        sys.exit(1)

        except Exception as e:
            log.error(f"Error reading enrichment file: {e}", exc_info=True)
            sys.exit(1)

        log.info(f"Loaded {len(enrichments)} enrichment records")
        return enrichments

    def consolidate_content_item(
        self, ci_metadata: Dict[str, Any], enrichments: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Consolidate a single content item with its enrichment data.

        Args:
            ci_metadata: Content item metadata dictionary
            enrichments: Dictionary of all enrichment data

        Returns:
            Updated metadata with consolidated fields

        Raises:
            SystemExit: If enrichment data is missing for this CI
        """
        ci_id = ci_metadata.get("id")

        if not ci_id:
            log.error("Content item missing 'id' field")
            sys.exit(1)

        # Strict matching: enrichment must exist
        if ci_id not in enrichments:
            log.error(f"Missing enrichment data for content item: {ci_id}")
            log.error(
                "Consolidation requires complete enrichment data for all content items"
            )
            sys.exit(1)

        enrichment = enrichments[ci_id]

        # Rename lg → lg_original if it exists
        if "lg" in ci_metadata:
            ci_metadata["lg_original"] = ci_metadata.pop("lg")
            log.debug(f"Renamed lg → lg_original for {ci_id}")
        elif "l" in ci_metadata:
            # Handle legacy 'l' field
            ci_metadata["lg_original"] = ci_metadata.pop("l")
            log.debug(f"Renamed l → lg_original for {ci_id}")

        # Add consolidated fields
        ci_metadata["consolidated_lg"] = enrichment["lg"]
        ci_metadata["consolidated_ocrqa"] = enrichment["ocrqa"]
        ci_metadata["consolidated_langident_run_id"] = self.langident_run_id

        # Note: consolidated_reocr_applied and consolidated_reocr_run_id
        # should be added here if re-OCR information is available

        return ci_metadata

    def process_issue(
        self,
        issue_data: Dict[str, Any],
        enrichments: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Process a single issue, consolidating all its content items.

        Args:
            issue_data: Issue data dictionary
            enrichments: Dictionary of all enrichment data

        Returns:
            Consolidated issue data

        Raises:
            SystemExit: If any content item is missing enrichment data
        """
        issue_id = issue_data.get("id", "UNKNOWN")
        log.debug(f"Processing issue: {issue_id}")

        # Store original timestamp before updating
        original_ts = issue_data.get("ts") or issue_data.get("cdt")
        if not original_ts:
            log.error(f"Issue {issue_id} missing both 'ts' and 'cdt' fields")
            sys.exit(1)

        # Set consolidated flag and timestamps
        issue_data["consolidated"] = True
        issue_data["consolidated_ts_original"] = original_ts
        issue_data["ts"] = self.timestamp

        # Process all content items
        content_items = issue_data.get("i", [])
        if not content_items:
            log.warning(f"Issue {issue_id} has no content items")

        processed_count = 0
        for ci in content_items:
            ci_metadata = ci.get("m", {})
            if ci_metadata:
                self.consolidate_content_item(ci_metadata, enrichments)
                processed_count += 1

        log.info(f"Consolidated {processed_count} content items in issue {issue_id}")

        return issue_data

    def run(self) -> None:
        """
        Run the consolidation processor.

        Reads canonical issues, merges with enrichments, and writes consolidated output.
        """
        log.info("Starting consolidation process")
        log.info(f"Canonical input: {self.canonical_input}")
        log.info(f"Enrichment input: {self.enrichment_input}")
        log.info(f"Output: {self.output_file}")

        # Load all enrichments first
        enrichments = self.load_enrichments()

        if not enrichments:
            log.error("No enrichment data loaded - cannot proceed")
            sys.exit(1)

        # Process canonical issues
        try:
            issues_processed = 0

            with smart_open(
                self.canonical_input,
                "rt",
                encoding="utf-8",
                transport_params=get_transport_params(self.canonical_input),
            ) as input_f, smart_open(
                self.output_file,
                "wt",
                encoding="utf-8",
                transport_params=get_transport_params(self.output_file),
            ) as output_f:

                for line_num, line in enumerate(input_f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        issue_data = json.loads(line)
                        consolidated_issue = self.process_issue(issue_data, enrichments)

                        # Write consolidated issue
                        output_f.write(
                            json.dumps(consolidated_issue, ensure_ascii=False) + "\n"
                        )
                        issues_processed += 1

                    except json.JSONDecodeError as e:
                        log.error(f"Invalid JSON in canonical line {line_num}: {e}")
                        sys.exit(1)

            log.info(f"Successfully processed {issues_processed} issues")

        except Exception as e:
            log.error(f"Error during consolidation: {e}", exc_info=True)
            sys.exit(1)


def main(args: Optional[List[str]] = None) -> None:
    """
    Main function to run the Consolidated Canonical Processor.

    Args:
        args: Command-line arguments (uses sys.argv if None)
    """
    options: argparse.Namespace = parse_arguments(args)

    processor: ConsolidatedCanonicalProcessor = ConsolidatedCanonicalProcessor(
        canonical_input=options.canonical_input,
        enrichment_input=options.enrichment_input,
        output_file=options.output,
        langident_run_id=options.langident_run_id,
        log_level=options.log_level,
        log_file=options.log_file,
    )

    # Log the parsed options after logger is configured
    log.info("Consolidation options: %s", options)

    processor.run()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"Processing error: {e}", exc_info=True)
        sys.exit(2)
