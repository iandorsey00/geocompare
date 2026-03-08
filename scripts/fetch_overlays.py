#!/usr/bin/env python3
"""Fetch and normalize optional overlay files for geocompare.

Writes canonical overlay CSVs under:
  <out-dir>/overlays/{crime_data.csv,voter_data.csv}
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, List, Optional

CANONICAL_FILES = {
    "crime": "crime_data.csv",
    "voter": "voter_data.csv",
}


def _read_text_from_source(source: str) -> str:
    if source.startswith("http://") or source.startswith("https://"):
        with urllib.request.urlopen(source) as response:  # nosec - user-provided source
            return response.read().decode("utf-8")
    return Path(source).read_text(encoding="utf-8")


def _normalize_key(value: str) -> str:
    return value.strip().lower().replace(" ", "_")


def _parse_records(source: str) -> List[Dict[str, str]]:
    text = _read_text_from_source(source)
    stripped = text.lstrip()
    if stripped.startswith("[") or stripped.startswith("{"):
        payload = json.loads(text)
        if isinstance(payload, dict):
            payload = payload.get("rows", [])
        if not isinstance(payload, list):
            raise ValueError("JSON payload must be a list or object with 'rows'.")
        out = []
        for row in payload:
            if isinstance(row, dict):
                out.append({str(k): str(v) for k, v in row.items() if v is not None})
        return out

    reader = csv.DictReader(text.splitlines())
    rows = []
    for row in reader:
        rows.append({str(k): ("" if v is None else str(v)) for k, v in row.items() if k})
    return rows


def _find_col(record: Dict[str, str], aliases: Iterable[str]) -> Optional[str]:
    key_map = {_normalize_key(k): k for k in record.keys()}
    for alias in aliases:
        if alias in key_map:
            return key_map[alias]
    return None


def _as_float(value: str) -> Optional[float]:
    text = value.strip()
    if not text:
        return None
    text = text.replace(",", "")
    if text.endswith("%"):
        text = text[:-1]
    try:
        return float(text)
    except ValueError:
        return None


def _canonicalize_crime(rows: List[Dict[str, str]]) -> List[Dict[str, object]]:
    out = []
    for row in rows:
        geoid_col = _find_col(row, ("geoid", "geoid20", "geoid10"))
        if not geoid_col:
            continue
        geoid = row.get(geoid_col, "").strip()
        if not geoid:
            continue
        item: Dict[str, object] = {"GEOID": geoid}
        mappings = {
            "violent_crime_count": ("violent_crime_count", "violent_crime", "violent"),
            "property_crime_count": ("property_crime_count", "property_crime", "property"),
            "total_crime_count": ("total_crime_count", "total_crime", "crime_total"),
        }
        has_metric = False
        for canonical, aliases in mappings.items():
            col = _find_col(row, aliases)
            if not col:
                continue
            value = _as_float(row.get(col, ""))
            if value is None:
                continue
            item[canonical] = value
            has_metric = True
        if has_metric:
            out.append(item)
    return out


def _canonicalize_voter(rows: List[Dict[str, str]]) -> List[Dict[str, object]]:
    out = []
    for row in rows:
        geoid_col = _find_col(row, ("geoid", "geoid20", "geoid10"))
        if not geoid_col:
            continue
        geoid = row.get(geoid_col, "").strip()
        if not geoid:
            continue
        item: Dict[str, object] = {"GEOID": geoid}
        mappings = {
            "registered_voters": ("registered_voters", "total_registered", "registered"),
            "democratic_voters": ("democratic_voters", "dem_voters", "democratic"),
            "republican_voters": ("republican_voters", "rep_voters", "republican"),
            "other_voters": ("other_voters", "oth_voters", "other"),
        }
        has_metric = False
        for canonical, aliases in mappings.items():
            col = _find_col(row, aliases)
            if not col:
                continue
            value = _as_float(row.get(col, ""))
            if value is None:
                continue
            item[canonical] = value
            has_metric = True
        if has_metric:
            out.append(item)
    return out


def _write_csv(path: Path, rows: List[Dict[str, object]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _run_one(kind: str, source: str, out_dir: Path) -> None:
    rows = _parse_records(source)
    if kind == "crime":
        normalized = _canonicalize_crime(rows)
        fieldnames = ["GEOID", "violent_crime_count", "property_crime_count", "total_crime_count"]
    elif kind == "voter":
        normalized = _canonicalize_voter(rows)
        fieldnames = [
            "GEOID",
            "registered_voters",
            "democratic_voters",
            "republican_voters",
            "other_voters",
        ]
    else:
        raise ValueError(f"unsupported overlay kind: {kind}")
    destination = out_dir / "overlays" / CANONICAL_FILES[kind]
    _write_csv(destination, normalized, fieldnames)
    print(f"{kind}: wrote {len(normalized)} rows -> {destination}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch and normalize private overlay datasets for geocompare.",
    )
    parser.add_argument(
        "--out-dir",
        default="../000-data",
        help="data root where overlays/ will be written (default: ../000-data)",
    )
    parser.add_argument("--crime-source", help="crime source CSV/JSON path or URL")
    parser.add_argument("--voter-source", help="voter source CSV/JSON path or URL")
    args = parser.parse_args()

    if not any([args.crime_source, args.voter_source]):
        parser.error("Provide at least one source: --crime-source / --voter-source")

    out_dir = Path(args.out_dir).resolve()
    try:
        if args.crime_source:
            _run_one("crime", args.crime_source, out_dir)
        if args.voter_source:
            _run_one("voter", args.voter_source, out_dir)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
