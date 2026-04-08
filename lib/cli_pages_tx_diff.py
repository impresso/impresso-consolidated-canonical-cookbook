#!/usr/bin/env python3
"""
Compare line text between matching canonical pages files stored on S3.

The script accepts two S3 prefixes, finds the common objects whose names end with
``pages.jsonl.bz2``, selects one random file, and reports the lines whose text is
different. Line text is reconstructed from the nested ``r -> p -> l -> t[*].tx``
structure while respecting the optional ``gn`` token flag. It can also compare all
matching page files and print a short sampled summary of the differing lines.

Example:
    python lib/cli_pages_tx_diff.py \
        s3://112-canonical-final/BL/volkfreu/pages/ \
        s3://118-canonical-consolidated-final/v2025-12-04/BL/volkfreu/pages/ \
        --seed 42
"""

import argparse
import difflib
import json
import logging
import math
import random
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from smart_open import open as smart_open  # type: ignore

from impresso_cookbook import (  # type: ignore
    get_s3_client,
    get_transport_params,
    setup_logging,
)

log = logging.getLogger(__name__)


DiffEntry = Dict[str, str]


def emit_progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def parse_subsample(value: str) -> float:
    try:
        subsample = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid subsample value: {value}") from exc

    if subsample <= 0 or subsample > 1:
        raise argparse.ArgumentTypeError("--subsample must be > 0 and <= 1")

    return subsample


def parse_arguments(args: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pick a random common pages.jsonl.bz2 file under two S3 prefixes and "
            "report the lines whose text differs."
        )
    )
    parser.add_argument("left", help="Left S3 prefix, e.g. s3://bucket/path/")
    parser.add_argument("right", help="Right S3 prefix, e.g. s3://bucket/path/")
    parser.add_argument(
        "--file",
        dest="relative_file",
        help=(
            "Relative object path below both prefixes to compare directly. "
            "If omitted, a random common file is selected."
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Random seed for reproducible file selection.",
    )
    parser.add_argument(
        "--all-files",
        action="store_true",
        help=(
            "Compare all matching pages.jsonl.bz2 files instead of choosing a "
            "single random one."
        ),
    )
    parser.add_argument(
        "--subsample",
        type=parse_subsample,
        default=1.0,
        help=(
            "When used with --all-files, compare only this fraction of matching "
            "files, selected at random (default: %(default)s)."
        ),
    )
    parser.add_argument(
        "--max-diffs",
        type=int,
        default=100,
        help="Maximum number of differing lines to print (default: %(default)s).",
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
    return parser.parse_args(args)


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Expected an S3 URI, got: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def is_pages_object(key: str) -> bool:
    return key.endswith("pages.jsonl.bz2")


def normalize_prefix(prefix: str) -> str:
    return prefix.rstrip("/") + "/"


def strip_prefix(text: str, prefix: str) -> str:
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text


def list_pages_objects(base_uri: str, s3_client: Any) -> Dict[str, str]:
    bucket, key_prefix = parse_s3_uri(base_uri)
    normalized_prefix = normalize_prefix(key_prefix)
    paginator = s3_client.get_paginator("list_objects_v2")
    objects: Dict[str, str] = {}
    emit_progress(f"Listing pages under {base_uri}")

    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for entry in page.get("Contents", []):
            key = entry["Key"]
            if not is_pages_object(key):
                continue
            relative_key = strip_prefix(key, normalized_prefix)
            objects[relative_key] = f"s3://{bucket}/{key}"

    emit_progress(f"Found {len(objects)} page files under {base_uri}")
    return objects


def list_relative_objects(
    left_uri: str, right_uri: str, s3_client: Any
) -> Tuple[Dict[str, str], Dict[str, str]]:
    return (
        list_pages_objects(left_uri, s3_client),
        list_pages_objects(right_uri, s3_client),
    )


def choose_file(
    left_uri: str,
    right_uri: str,
    explicit_file: Optional[str],
    seed: Optional[int],
    s3_client: Any,
) -> Tuple[str, str, str]:
    left_objects, right_objects = list_relative_objects(left_uri, right_uri, s3_client)

    if explicit_file:
        relative_key = explicit_file.lstrip("/")
        if relative_key not in left_objects:
            raise FileNotFoundError(
                f"{relative_key} was not found under left prefix {left_uri}"
            )
        if relative_key not in right_objects:
            raise FileNotFoundError(
                f"{relative_key} was not found under right prefix {right_uri}"
            )
        return relative_key, left_objects[relative_key], right_objects[relative_key]

    common_files = sorted(set(left_objects) & set(right_objects))
    if not common_files:
        raise FileNotFoundError(
            "No common pages.jsonl.bz2 files were found under the provided prefixes."
        )

    picker = random.Random(seed)
    relative_key = picker.choice(common_files)
    return relative_key, left_objects[relative_key], right_objects[relative_key]


def load_pages(file_path: str) -> Dict[str, dict]:
    pages: Dict[str, dict] = {}
    with smart_open(
        file_path,
        "r",
        encoding="utf-8",
        transport_params=get_transport_params(file_path),
    ) as handle:
        for raw_line in handle:
            stripped = raw_line.strip()
            if not stripped:
                continue
            page = json.loads(stripped)
            page_id = page.get("id")
            if not page_id:
                raise ValueError(f"Encountered a page without an id in {file_path}")
            pages[page_id] = page
    return pages


def render_line_text(tokens: Sequence[dict]) -> str:
    pieces: List[str] = []
    for index, token in enumerate(tokens):
        pieces.append(token.get("tx", ""))
        is_last = index == len(tokens) - 1
        if not is_last and not token.get("gn", False):
            pieces.append(" ")
    return "".join(pieces).strip()


def extract_lines(page: dict) -> List[Tuple[str, str]]:
    lines: List[Tuple[str, str]] = []
    for region_index, region in enumerate(page.get("r", [])):
        for paragraph_index, paragraph in enumerate(region.get("p", [])):
            for line_index, line in enumerate(paragraph.get("l", [])):
                path = f"r[{region_index}].p[{paragraph_index}].l[{line_index}]"
                lines.append((path, render_line_text(line.get("t", []))))
    return lines


def unified_text_diff(left_text: str, right_text: str) -> List[str]:
    return list(
        difflib.unified_diff(
            [left_text + "\n"],
            [right_text + "\n"],
            fromfile="left",
            tofile="right",
            lineterm="",
        )
    )


def compare_pages(
    left_pages: Dict[str, dict],
    right_pages: Dict[str, dict],
    relative_key: str,
) -> List[DiffEntry]:
    report: List[DiffEntry] = []

    left_page_ids = set(left_pages)
    right_page_ids = set(right_pages)

    missing_left = sorted(right_page_ids - left_page_ids)
    missing_right = sorted(left_page_ids - right_page_ids)

    for page_id in missing_left:
        report.append(
            {
                "file": relative_key,
                "page": page_id,
                "path": "<page>",
                "kind": "missing-left-page",
                "left": "<missing>",
                "right": "present on right",
            }
        )
    for page_id in missing_right:
        report.append(
            {
                "file": relative_key,
                "page": page_id,
                "path": "<page>",
                "kind": "missing-right-page",
                "left": "present on left",
                "right": "<missing>",
            }
        )

    for page_id in sorted(left_page_ids & right_page_ids):
        left_lines = extract_lines(left_pages[page_id])
        right_lines = extract_lines(right_pages[page_id])

        max_len = max(len(left_lines), len(right_lines))
        for index in range(max_len):
            left_entry = left_lines[index] if index < len(left_lines) else None
            right_entry = right_lines[index] if index < len(right_lines) else None

            if left_entry is None:
                assert right_entry is not None
                report.append(
                    {
                        "file": relative_key,
                        "page": page_id,
                        "path": right_entry[0],
                        "kind": "extra-right-line",
                        "left": "<missing>",
                        "right": right_entry[1],
                    }
                )
                continue

            if right_entry is None:
                report.append(
                    {
                        "file": relative_key,
                        "page": page_id,
                        "path": left_entry[0],
                        "kind": "extra-left-line",
                        "left": left_entry[1],
                        "right": "<missing>",
                    }
                )
                continue

            left_path, left_text = left_entry
            right_path, right_text = right_entry

            if left_text == right_text:
                continue

            path = (
                left_path if left_path == right_path else f"{left_path} != {right_path}"
            )
            report.append(
                {
                    "file": relative_key,
                    "page": page_id,
                    "path": path,
                    "kind": "line-text-diff",
                    "left": left_text,
                    "right": right_text,
                }
            )

    return report


def format_diff_entry(diff: DiffEntry) -> List[str]:
    lines = [
        f"File {diff['file']} | page {diff['page']} | {diff['path']} | {diff['kind']}",
        f"  left : {diff['left']}",
        f"  right: {diff['right']}",
    ]
    if diff["kind"] == "line-text-diff":
        lines.extend(
            f"  {line}" for line in unified_text_diff(diff["left"], diff["right"])
        )
    return lines


def compare_all_files(
    left_uri: str,
    right_uri: str,
    seed: Optional[int],
    subsample: float,
    s3_client: Any,
) -> Tuple[List[DiffEntry], List[str], List[str], int, int]:
    left_objects, right_objects = list_relative_objects(left_uri, right_uri, s3_client)
    left_keys = set(left_objects)
    right_keys = set(right_objects)

    extra_left_files = sorted(left_keys - right_keys)
    extra_right_files = sorted(right_keys - left_keys)
    common_keys = sorted(left_keys & right_keys)

    selected_keys = common_keys
    if common_keys and subsample < 1.0:
        picker = random.Random(seed)
        sample_count = max(1, math.ceil(len(common_keys) * subsample))
        selected_keys = sorted(picker.sample(common_keys, sample_count))

    emit_progress(
        f"Comparing {len(selected_keys)}/{len(common_keys)} matching page files"
    )

    all_diffs: List[DiffEntry] = []
    for index, relative_key in enumerate(selected_keys, start=1):
        if index == 1 or index % 10 == 0 or index == len(selected_keys):
            emit_progress(
                f"Processing file {index}/{len(selected_keys)}: {relative_key}"
            )
        left_pages = load_pages(left_objects[relative_key])
        right_pages = load_pages(right_objects[relative_key])
        all_diffs.extend(compare_pages(left_pages, right_pages, relative_key))

    return (
        all_diffs,
        extra_left_files,
        extra_right_files,
        len(selected_keys),
        len(common_keys),
    )


def print_all_files_report(
    left_uri: str,
    right_uri: str,
    seed: Optional[int],
    subsample: float,
    s3_client: Any,
) -> int:
    (
        all_diffs,
        extra_left_files,
        extra_right_files,
        selected_count,
        common_count,
    ) = compare_all_files(left_uri, right_uri, seed, subsample, s3_client)

    if not all_diffs and not extra_left_files and not extra_right_files:
        print("EQUAL")
        if subsample < 1.0:
            print(f"Checked {selected_count}/{common_count} matching files")
        return 0

    print("DIFFERENT")
    if subsample < 1.0:
        print(f"Checked {selected_count}/{common_count} matching files")
    if extra_left_files:
        print(f"Extra files on left: {len(extra_left_files)}")
        for relative_key in extra_left_files[:3]:
            print(f"  left only : {relative_key}")
    if extra_right_files:
        print(f"Extra files on right: {len(extra_right_files)}")
        for relative_key in extra_right_files[:3]:
            print(f"  right only: {relative_key}")

    print(f"Differing line entries: {len(all_diffs)}")
    sample_size = min(3, len(all_diffs))
    if sample_size:
        picker = random.Random(seed)
        print(f"Sampled differing lines: {sample_size}")
        for diff in picker.sample(all_diffs, sample_size):
            for line in format_diff_entry(diff):
                print(line)

    return 1


def main(args: Optional[List[str]] = None) -> int:
    options = parse_arguments(args)
    setup_logging(options.log_level, options.log_file, logger=log)

    s3_client = get_s3_client()

    if options.all_files:
        return print_all_files_report(
            options.left,
            options.right,
            options.seed,
            options.subsample,
            s3_client,
        )

    relative_key, left_file, right_file = choose_file(
        options.left,
        options.right,
        options.relative_file,
        options.seed,
        s3_client,
    )

    log.info("Selected common file: %s", relative_key)
    log.info("Left file: %s", left_file)
    log.info("Right file: %s", right_file)

    left_pages = load_pages(left_file)
    right_pages = load_pages(right_file)
    report = compare_pages(left_pages, right_pages, relative_key)

    print(f"Selected file: {relative_key}")
    print(f"Left:  {left_file}")
    print(f"Right: {right_file}")

    if not report:
        print("EQUAL")
        return 0

    print("DIFFERENT")
    print(f"Differing lines: {min(len(report), options.max_diffs)} shown")
    for diff in report[: options.max_diffs]:
        for line in format_diff_entry(diff):
            print(line)

    if len(report) > options.max_diffs:
        print(
            f"... truncated {len(report) - options.max_diffs} additional report lines"
        )

    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        log.error("Failed to diff page text: %s", exc, exc_info=True)
        sys.exit(2)
