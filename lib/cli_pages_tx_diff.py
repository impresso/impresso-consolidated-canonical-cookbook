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
LineEntry = Dict[str, Any]
LineStats = Dict[str, int]
MIN_LINE_OVERLAP = 0.3


def emit_progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def make_line_stats() -> LineStats:
    return {"equal_lines": 0, "different_lines": 0}


def merge_line_stats(target: LineStats, source: LineStats) -> None:
    target["equal_lines"] += source["equal_lines"]
    target["different_lines"] += source["different_lines"]


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


def extract_lines(page: dict) -> List[LineEntry]:
    lines: List[LineEntry] = []
    for region_index, region in enumerate(page.get("r", [])):
        for paragraph_index, paragraph in enumerate(region.get("p", [])):
            for line_index, line in enumerate(paragraph.get("l", [])):
                path = f"r[{region_index}].p[{paragraph_index}].l[{line_index}]"
                lines.append(
                    {
                        "path": path,
                        "text": render_line_text(line.get("t", [])),
                        "coords": line.get("c", []),
                    }
                )
    return lines


def intersection_size(first: Sequence[int], second: Sequence[int]) -> int:
    if len(first) != 4 or len(second) != 4:
        return 0

    first_x1, first_y1, first_width, first_height = first
    second_x1, second_y1, second_width, second_height = second

    first_x2 = first_x1 + first_width
    first_y2 = first_y1 + first_height
    second_x2 = second_x1 + second_width
    second_y2 = second_y1 + second_height

    overlap_width = min(first_x2, second_x2) - max(first_x1, second_x1)
    overlap_height = min(first_y2, second_y2) - max(first_y1, second_y1)
    if overlap_width <= 0 or overlap_height <= 0:
        return 0

    return overlap_width * overlap_height


def rectangle_area(coords: Sequence[int]) -> int:
    if len(coords) != 4:
        return 0
    return max(0, coords[2]) * max(0, coords[3])


def line_overlap_score(left_line: LineEntry, right_line: LineEntry) -> float:
    left_area = rectangle_area(left_line["coords"])
    right_area = rectangle_area(right_line["coords"])
    if left_area == 0 or right_area == 0:
        return 0.0

    overlap_area = intersection_size(left_line["coords"], right_line["coords"])
    if overlap_area == 0:
        return 0.0

    return overlap_area / min(left_area, right_area)


def line_center_distance(
    left_line: LineEntry, right_line: LineEntry
) -> Tuple[float, float]:
    left_coords = left_line["coords"]
    right_coords = right_line["coords"]
    if len(left_coords) != 4 or len(right_coords) != 4:
        return (float("inf"), float("inf"))

    left_center_y = left_coords[1] + (left_coords[3] / 2)
    right_center_y = right_coords[1] + (right_coords[3] / 2)
    left_center_x = left_coords[0] + (left_coords[2] / 2)
    right_center_x = right_coords[0] + (right_coords[2] / 2)
    return (abs(left_center_y - right_center_y), abs(left_center_x - right_center_x))


def match_lines_by_overlap(
    left_lines: List[LineEntry], right_lines: List[LineEntry]
) -> Tuple[List[Tuple[int, int]], List[int], List[int]]:
    candidates: List[Tuple[float, float, float, int, int]] = []
    for left_index, left_line in enumerate(left_lines):
        for right_index, right_line in enumerate(right_lines):
            overlap_score = line_overlap_score(left_line, right_line)
            if overlap_score < MIN_LINE_OVERLAP:
                continue
            center_y_distance, center_x_distance = line_center_distance(
                left_line, right_line
            )
            candidates.append(
                (
                    -overlap_score,
                    center_y_distance,
                    center_x_distance,
                    left_index,
                    right_index,
                )
            )

    candidates.sort()
    matches: List[Tuple[int, int]] = []
    used_left: set[int] = set()
    used_right: set[int] = set()

    for _, _, _, left_index, right_index in candidates:
        if left_index in used_left or right_index in used_right:
            continue
        used_left.add(left_index)
        used_right.add(right_index)
        matches.append((left_index, right_index))

    unmatched_left = [
        index for index in range(len(left_lines)) if index not in used_left
    ]
    unmatched_right = [
        index for index in range(len(right_lines)) if index not in used_right
    ]
    return matches, unmatched_left, unmatched_right


def compare_pages(
    left_pages: Dict[str, dict],
    right_pages: Dict[str, dict],
    relative_key: str,
) -> Tuple[List[DiffEntry], LineStats]:
    report: List[DiffEntry] = []
    stats = make_line_stats()

    left_page_ids = set(left_pages)
    right_page_ids = set(right_pages)

    missing_left = sorted(right_page_ids - left_page_ids)
    missing_right = sorted(left_page_ids - right_page_ids)

    for page_id in missing_left:
        stats["different_lines"] += len(extract_lines(right_pages[page_id]))
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
        stats["different_lines"] += len(extract_lines(left_pages[page_id]))
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

        matches, unmatched_left, unmatched_right = match_lines_by_overlap(
            left_lines, right_lines
        )

        for left_index, right_index in matches:
            left_entry = left_lines[left_index]
            right_entry = right_lines[right_index]
            left_text = left_entry["text"]
            right_text = right_entry["text"]

            if left_text == right_text:
                stats["equal_lines"] += 1
                continue

            stats["different_lines"] += 1
            path = (
                left_entry["path"]
                if left_entry["path"] == right_entry["path"]
                else f"{left_entry['path']} ~= {right_entry['path']}"
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

        for left_index in unmatched_left:
            left_entry = left_lines[left_index]
            stats["different_lines"] += 1
            report.append(
                {
                    "file": relative_key,
                    "page": page_id,
                    "path": left_entry["path"],
                    "kind": "extra-left-line",
                    "left": left_entry["text"],
                    "right": "<missing>",
                }
            )

        for right_index in unmatched_right:
            right_entry = right_lines[right_index]
            stats["different_lines"] += 1
            report.append(
                {
                    "file": relative_key,
                    "page": page_id,
                    "path": right_entry["path"],
                    "kind": "extra-right-line",
                    "left": "<missing>",
                    "right": right_entry["text"],
                }
            )

    return report, stats


def format_diff_entry(diff: DiffEntry) -> List[str]:
    return [
        f"File {diff['file']} | page {diff['page']} | {diff['path']} | {diff['kind']}",
        f"  left : {diff['left']}",
        f"  right: {diff['right']}",
    ]


def compare_all_files(
    left_uri: str,
    right_uri: str,
    seed: Optional[int],
    subsample: float,
    s3_client: Any,
) -> Tuple[List[DiffEntry], List[str], List[str], int, int, LineStats]:
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
    all_stats = make_line_stats()
    for index, relative_key in enumerate(selected_keys, start=1):
        if index == 1 or index % 10 == 0 or index == len(selected_keys):
            emit_progress(
                f"Processing file {index}/{len(selected_keys)}: {relative_key}"
            )
        left_pages = load_pages(left_objects[relative_key])
        right_pages = load_pages(right_objects[relative_key])
        page_diffs, page_stats = compare_pages(left_pages, right_pages, relative_key)
        all_diffs.extend(page_diffs)
        merge_line_stats(all_stats, page_stats)

    return (
        all_diffs,
        extra_left_files,
        extra_right_files,
        len(selected_keys),
        len(common_keys),
        all_stats,
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
        line_stats,
    ) = compare_all_files(left_uri, right_uri, seed, subsample, s3_client)

    inspected_lines = line_stats["equal_lines"] + line_stats["different_lines"]

    if not all_diffs and not extra_left_files and not extra_right_files:
        print("EQUAL")
        if subsample < 1.0:
            print(f"Checked {selected_count}/{common_count} matching files")
        print(f"Inspected lines: {inspected_lines}")
        print(f"Equal lines: {line_stats['equal_lines']}")
        print(f"Different lines: {line_stats['different_lines']}")
        return 0

    print("DIFFERENT")
    if subsample < 1.0:
        print(f"Checked {selected_count}/{common_count} matching files")
    print(f"Inspected lines: {inspected_lines}")
    print(f"Equal lines: {line_stats['equal_lines']}")
    print(f"Different lines: {line_stats['different_lines']}")
    if extra_left_files:
        print(f"Extra files on left: {len(extra_left_files)}")
        for relative_key in extra_left_files[:3]:
            print(f"  left only : {relative_key}")
    if extra_right_files:
        print(f"Extra files on right: {len(extra_right_files)}")
        for relative_key in extra_right_files[:3]:
            print(f"  right only: {relative_key}")

    print(f"Differing line entries: {len(all_diffs)}")
    sample_size = min(20, len(all_diffs))
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
    report, line_stats = compare_pages(left_pages, right_pages, relative_key)
    inspected_lines = line_stats["equal_lines"] + line_stats["different_lines"]

    print(f"Selected file: {relative_key}")
    print(f"Left:  {left_file}")
    print(f"Right: {right_file}")
    print(f"Inspected lines: {inspected_lines}")
    print(f"Equal lines: {line_stats['equal_lines']}")
    print(f"Different lines: {line_stats['different_lines']}")

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
