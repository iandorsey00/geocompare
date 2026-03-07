#!/usr/bin/env python3
"""Fetch latest ACS summary files and compatible gazetteer files.

This script downloads the specific raw files expected by geocompare.Database:
- ACS lookup file
- ACS geography files (g<year>5*.csv)
- ACS estimate sequence files (e<year>5*<seq>000.txt) for required tables
- Gazetteer national files for a selected year

Features:
- Auto-detect latest ACS and gazetteer years
- Resumable downloads via HTTP range requests
- Per-file progress bars
- Safe cleanup modes: archive or clean managed files only
- Dry-run mode
"""

from __future__ import annotations

import argparse
import csv
import random
import re
import shutil
import sys
import time
import zipfile
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

ACS_ROOT = "https://www2.census.gov/programs-surveys/acs/summary_file/"
GAZETTEER_ROOT = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
CENSUS_GAZ_PAGE = "https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html"
USER_AGENT = "geocompare-fetch/1.0"
CHUNK_SIZE = 1024 * 1024

REQUIRED_TABLE_IDS = {
    "B01003",
    "B01001",
    "B01002",
    "B11001",
    "B19301",
    "B02001",
    "B03002",
    "B04004",
    "B15003",
    "B17001",
    "B19013",
    "B23025",
    "B25003",
    "B25010",
    "B25035",
    "B25018",
    "B25058",
    "B25077",
}

DEFAULT_STATES = [
    "al",
    "ak",
    "az",
    "ar",
    "ca",
    "co",
    "ct",
    "dc",
    "de",
    "fl",
    "ga",
    "hi",
    "id",
    "il",
    "in",
    "ia",
    "ks",
    "ky",
    "la",
    "me",
    "md",
    "ma",
    "mi",
    "mn",
    "ms",
    "mo",
    "mt",
    "ne",
    "nv",
    "nh",
    "nj",
    "nm",
    "ny",
    "nc",
    "nd",
    "oh",
    "ok",
    "or",
    "pa",
    "ri",
    "sc",
    "sd",
    "tn",
    "tx",
    "ut",
    "vt",
    "va",
    "wa",
    "wv",
    "wi",
    "wy",
    "us",
]

GAZ_FILES = [
    "{year}_Gaz_place_national.txt",
    "{year}_Gaz_counties_national.txt",
    "{year}_Gaz_state_national.txt",
    "{year}_Gaz_cbsa_national.txt",
    "{year}_Gaz_ua_national.txt",
    "{year}_Gaz_zcta_national.txt",
]

STATE_DIR_NAMES = {
    "al": "Alabama",
    "ak": "Alaska",
    "az": "Arizona",
    "ar": "Arkansas",
    "ca": "California",
    "co": "Colorado",
    "ct": "Connecticut",
    "dc": "DistrictOfColumbia",
    "de": "Delaware",
    "fl": "Florida",
    "ga": "Georgia",
    "hi": "Hawaii",
    "id": "Idaho",
    "il": "Illinois",
    "in": "Indiana",
    "ia": "Iowa",
    "ks": "Kansas",
    "ky": "Kentucky",
    "la": "Louisiana",
    "me": "Maine",
    "md": "Maryland",
    "ma": "Massachusetts",
    "mi": "Michigan",
    "mn": "Minnesota",
    "ms": "Mississippi",
    "mo": "Missouri",
    "mt": "Montana",
    "ne": "Nebraska",
    "nv": "Nevada",
    "nh": "NewHampshire",
    "nj": "NewJersey",
    "nm": "NewMexico",
    "ny": "NewYork",
    "nc": "NorthCarolina",
    "nd": "NorthDakota",
    "oh": "Ohio",
    "ok": "Oklahoma",
    "or": "Oregon",
    "pa": "Pennsylvania",
    "ri": "RhodeIsland",
    "sc": "SouthCarolina",
    "sd": "SouthDakota",
    "tn": "Tennessee",
    "tx": "Texas",
    "ut": "Utah",
    "vt": "Vermont",
    "va": "Virginia",
    "wa": "Washington",
    "wv": "WestVirginia",
    "wi": "Wisconsin",
    "wy": "Wyoming",
    "us": "UnitedStates",
}

MANAGED_PATTERNS = [
    re.compile(r"^ACS_5yr_Seq_Table_Number_Lookup\.txt$"),
    re.compile(r"^g\d{4}5[a-z]{2}\.csv$"),
    re.compile(r"^e\d{4}5[a-z]{2}\d{4}000\.txt$"),
    re.compile(r"^\d{4}_Gaz_(place|counties|state|cbsa|ua|zcta)_national\.txt$"),
    re.compile(r"^\d{4}_Gaz_(place|counties|state|cbsa|ua|zcta)_national\.zip$"),
]


class DownloadError(RuntimeError):
    pass


class RateLimitedError(DownloadError):
    pass


def log(msg: str) -> None:
    print(msg, flush=True)


def _is_rate_limited_text(text: str) -> bool:
    lowered = text.lower()
    return (
        "error 1015" in lowered
        or "ray id" in lowered and "rate limited" in lowered
        or "you are being rate limited" in lowered
        or "temporarily banned you" in lowered
    )


def _raise_if_rate_limited_http_error(err: HTTPError) -> None:
    body = ""
    try:
        body = err.read().decode("utf-8", errors="replace")
    except Exception:
        body = ""
    if err.code == 429 or _is_rate_limited_text(body):
        raise RateLimitedError("Rate limited by remote host (HTTP 429/Cloudflare 1015).")


def fetch_text(url: str, timeout: int = 30, max_attempts: int = 3) -> str:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=timeout) as resp:
                text = resp.read().decode("utf-8", errors="replace")
                if _is_rate_limited_text(text):
                    raise RateLimitedError("Rate limited by remote host (Cloudflare 1015).")
                return text
        except RateLimitedError:
            raise
        except HTTPError as e:
            _raise_if_rate_limited_http_error(e)
            last_error = e
            if attempt < max_attempts and e.code in {408, 500, 502, 503, 504}:
                time.sleep((2 ** (attempt - 1)) + random.uniform(0, 0.3))
                continue
            break
        except URLError as e:
            last_error = e
            if attempt < max_attempts:
                time.sleep((2 ** (attempt - 1)) + random.uniform(0, 0.3))
                continue
            break
    raise DownloadError(f"Failed to fetch text from {url}: {last_error}")


def url_exists(url: str, timeout: int = 20, max_attempts: int = 2) -> bool:
    for method in ("HEAD", "GET"):
        for attempt in range(1, max_attempts + 1):
            try:
                req = Request(url, headers={"User-Agent": USER_AGENT}, method=method)
                with urlopen(req, timeout=timeout) as resp:
                    if method == "GET":
                        chunk = resp.read(2048).decode("utf-8", errors="replace")
                        if _is_rate_limited_text(chunk):
                            raise RateLimitedError("Rate limited by remote host (Cloudflare 1015).")
                    return True
            except RateLimitedError:
                raise
            except HTTPError as e:
                _raise_if_rate_limited_http_error(e)
                # 404/403/etc for probing should not retry much.
                if attempt < max_attempts and e.code in {408, 500, 502, 503, 504}:
                    time.sleep((2 ** (attempt - 1)) + random.uniform(0, 0.2))
                    continue
                break
            except URLError:
                if attempt < max_attempts:
                    time.sleep((2 ** (attempt - 1)) + random.uniform(0, 0.2))
                    continue
                break
    return False


def discover_latest_acs_year() -> str:
    years: set[int] = set()
    try:
        html = fetch_text(ACS_ROOT)
        years.update(int(y) for y in re.findall(r'href="(\d{4})/"', html))
    except RateLimitedError:
        raise
    except Exception:
        pass

    # Fallback: probe recent years directly for known ACS paths.
    current = time.gmtime().tm_year
    for year in range(current + 1, current - 15, -1):
        base = f"{ACS_ROOT}{year}/"
        candidates = [
            urljoin(base, "data/5_year_seq_by_state/"),
            urljoin(base, "documentation/tech_docs/"),
            urljoin(base, "documentation/user_tools/"),
        ]
        if any(url_exists(url) for url in candidates):
            years.add(year)

    if years:
        return str(max(years))

    raise DownloadError(f"Could not discover ACS years from {ACS_ROOT}")


def discover_latest_gazetteer_year() -> str:
    years: list[int] = []
    try:
        html = fetch_text(GAZETTEER_ROOT)
        years = [int(y) for y in re.findall(r'href="(\d{4})_Gaz_place_national\.txt"', html)]
        years += [int(y) for y in re.findall(r'href="(\d{4})_Gazetteer/"', html)]
    except RateLimitedError:
        raise
    except Exception:
        years = []

    if not years:
        try:
            html = fetch_text(CENSUS_GAZ_PAGE)
            years = [int(y) for y in re.findall(r">\s*(\d{4})\s*<", html) if 1990 <= int(y) <= 2100]
        except RateLimitedError:
            raise
        except Exception:
            years = []

    if years:
        return str(max(years))

    # Fallback: probe recent place gazetteers directly.
    current = time.gmtime().tm_year
    for year in range(current + 1, current - 20, -1):
        candidates = [
            urljoin(GAZETTEER_ROOT, f"{year}_Gaz_place_national.txt"),
            urljoin(GAZETTEER_ROOT, f"{year}_Gazetteer/{year}_Gaz_place_national.txt"),
            urljoin(GAZETTEER_ROOT, f"{year}_Gazetteer/{year}_Gaz_place_national.zip"),
        ]
        if any(url_exists(url) for url in candidates):
            return str(year)

    raise DownloadError(f"Could not discover gazetteer years from {GAZETTEER_ROOT}")


def normalize_header(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def parse_lookup_sequence_numbers(lookup_path: Path, required_table_ids: set[str]) -> list[str]:
    with lookup_path.open("r", encoding="utf-8", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t|")
        except csv.Error:
            dialect = csv.excel

        reader = csv.reader(f, dialect)
        header = next(reader, None)
        if not header:
            raise DownloadError(f"Lookup file has no header: {lookup_path}")

        normalized = [normalize_header(h) for h in header]

        def idx(options: Iterable[str]) -> int:
            for opt in options:
                if opt in normalized:
                    return normalized.index(opt)
            raise DownloadError(f"Lookup file missing expected columns: {lookup_path}")

        table_idx = idx(["table_id", "tableid"])
        seq_idx = idx(["sequence_number", "sequence_number_and_table_number", "sequence_number_and_table_id"])

        sequences = set()
        for row in reader:
            if len(row) <= max(table_idx, seq_idx):
                continue
            table_id = row[table_idx].strip()
            seq = row[seq_idx].strip()
            if table_id in required_table_ids and seq:
                sequences.add(seq.zfill(4))

    if not sequences:
        raise DownloadError(f"No sequence numbers found for required tables in {lookup_path}")

    return sorted(sequences)


def progress_line(name: str, downloaded: int, total: int | None) -> str:
    width = 28
    if not total or total <= 0:
        mb = downloaded / (1024 * 1024)
        return f"{name:<34} {mb:8.1f} MB"

    frac = min(1.0, downloaded / total)
    filled = int(width * frac)
    bar = "#" * filled + "-" * (width - filled)
    pct = frac * 100.0
    return f"{name:<34} [{bar}] {pct:6.2f}%"


def download_file(
    url: str,
    dest: Path,
    *,
    overwrite: bool,
    resume: bool,
    dry_run: bool,
    timeout: int,
    max_attempts: int,
) -> str:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and not overwrite:
        return "exists"

    if dry_run:
        log(f"DRY-RUN: {url} -> {dest}")
        return "planned"

    part = dest.with_suffix(dest.suffix + ".part")
    start = part.stat().st_size if (resume and part.exists()) else 0

    headers = {"User-Agent": USER_AGENT}
    if start > 0:
        headers["Range"] = f"bytes={start}-"

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        req = Request(url, headers=headers)
        try:
            with urlopen(req, timeout=timeout) as resp:
                code = getattr(resp, "status", 200)
                accept_ranges = (resp.headers.get("Accept-Ranges") or "").lower()
                content_length = resp.headers.get("Content-Length")

                if start > 0 and code != 206:
                    # Server did not honor range; restart from zero.
                    start = 0
                    if part.exists():
                        part.unlink()

                if code == 206 and content_length is not None:
                    total = start + int(content_length)
                elif content_length is not None:
                    total = int(content_length)
                else:
                    total = None

                mode = "ab" if start > 0 else "wb"
                downloaded = start
                display_name = dest.name[:34]
                last_update = 0.0
                scanned_prefix = False

                with part.open(mode) as out:
                    while True:
                        chunk = resp.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        if not scanned_prefix:
                            prefix = chunk[:2048].decode("utf-8", errors="ignore")
                            if _is_rate_limited_text(prefix):
                                raise RateLimitedError(
                                    "Rate limited by remote host (Cloudflare 1015)."
                                )
                            scanned_prefix = True
                        out.write(chunk)
                        downloaded += len(chunk)
                        now = time.time()
                        if now - last_update > 0.08:
                            sys.stdout.write("\r" + progress_line(display_name, downloaded, total))
                            sys.stdout.flush()
                            last_update = now

                sys.stdout.write("\r" + progress_line(display_name, downloaded, total) + "\n")
                sys.stdout.flush()

                if part.exists():
                    part.replace(dest)

                if start > 0 and "bytes" not in accept_ranges and code != 206:
                    return "downloaded (resume unsupported; restarted)"
                if start > 0:
                    return "resumed"
                return "downloaded"
        except RateLimitedError:
            raise
        except HTTPError as e:
            _raise_if_rate_limited_http_error(e)
            last_error = e
            if attempt < max_attempts and e.code in {408, 500, 502, 503, 504}:
                time.sleep((2 ** (attempt - 1)) + random.uniform(0, 0.4))
                continue
            break
        except (URLError, TimeoutError) as e:
            last_error = e
            if attempt < max_attempts:
                time.sleep((2 ** (attempt - 1)) + random.uniform(0, 0.4))
                continue
            break
    raise DownloadError(f"Failed to download {url}: {last_error}") from last_error


def find_lookup_url(acs_year: str) -> str:
    base = f"{ACS_ROOT}{acs_year}/"
    lookup_names = [
        "ACS_5yr_Seq_Table_Number_Lookup.txt",
        "Sequence_Number_and_Table_Number_Lookup.txt",
        f"{acs_year}_ACS_5yr_Seq_Table_Number_Lookup.txt",
        f"{acs_year}_Sequence_Number_and_Table_Number_Lookup.txt",
        "ACS_5yr_Seq_Table_Number_Lookup.csv",
        "Sequence_Number_and_Table_Number_Lookup.csv",
        f"{acs_year}_ACS_5yr_Seq_Table_Number_Lookup.csv",
        f"{acs_year}_Sequence_Number_and_Table_Number_Lookup.csv",
    ]
    candidates = []
    for lookup_name in lookup_names:
        candidates.extend(
            [
                urljoin(base, f"data/5_year_seq_by_state/{lookup_name}"),
                urljoin(base, f"documentation/tech_docs/{lookup_name}"),
                urljoin(base, f"documentation/user_tools/{lookup_name}"),
                urljoin(base, f"sequence-based-SF/documentation/user_tools/{lookup_name}"),
            ]
        )
    for url in candidates:
        if url_exists(url):
            return url

    # Fallback: scrape likely documentation directories for lookup-like files.
    doc_dirs = [
        urljoin(base, "documentation/tech_docs/"),
        urljoin(base, "documentation/user_tools/"),
        urljoin(base, "sequence-based-SF/documentation/tech_docs/"),
        urljoin(base, "sequence-based-SF/documentation/user_tools/"),
    ]
    for doc_dir in doc_dirs:
        try:
            html = fetch_text(doc_dir)
        except RateLimitedError:
            raise
        except Exception:
            continue
        hrefs = re.findall(r'href="([^"]+)"', html)
        for href in hrefs:
            filename = href.split("/")[-1]
            lower = filename.lower()
            is_lookupish = (
                ("lookup" in lower and "table" in lower)
                or ("sequence" in lower and "table" in lower)
            )
            if not is_lookupish:
                continue
            if not (lower.endswith(".txt") or lower.endswith(".csv")):
                continue
            candidate = urljoin(doc_dir, filename)
            if url_exists(candidate):
                return candidate
    raise DownloadError(f"Could not locate ACS lookup file for {acs_year}")


def find_lookup_zip_url(acs_year: str) -> str | None:
    base = f"{ACS_ROOT}{acs_year}/"
    zip_names = [
        f"{acs_year}_5yr_Summary_FileTemplates.zip",
        f"{acs_year}_5yr_SummaryFileTemplates.zip",
    ]
    candidates = []
    for zip_name in zip_names:
        candidates.extend(
            [
                urljoin(base, f"documentation/tech_docs/{zip_name}"),
                urljoin(base, f"documentation/user_tools/{zip_name}"),
                urljoin(base, f"sequence-based-SF/documentation/tech_docs/{zip_name}"),
                urljoin(base, f"sequence-based-SF/documentation/user_tools/{zip_name}"),
                urljoin(base, zip_name),
            ]
        )
    for url in candidates:
        if url_exists(url):
            return url
    return None


def extract_lookup_from_zip(zip_path: Path, lookup_dest: Path) -> None:
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        target = None
        for member in members:
            name = member.split("/")[-1].lower()
            is_lookupish = (
                ("lookup" in name and "table" in name)
                or ("sequence" in name and "table" in name)
            )
            if (name.endswith(".txt") or name.endswith(".csv")) and is_lookupish:
                target = member
                break
        if target is None:
            raise DownloadError(f"No lookup file found in template zip: {zip_path}")
        with zf.open(target) as src, lookup_dest.open("wb") as out:
            shutil.copyfileobj(src, out)


def fetch_lookup_file(
    acs_year: str,
    lookup_dest: Path,
    *,
    overwrite: bool,
    resume: bool,
    dry_run: bool,
    timeout: int,
    max_attempts: int,
) -> str:
    try:
        lookup_url = find_lookup_url(acs_year)
        return download_file(
            lookup_url,
            lookup_dest,
            overwrite=overwrite,
            resume=resume,
            dry_run=dry_run,
            timeout=timeout,
            max_attempts=max_attempts,
        )
    except RateLimitedError:
        raise
    except DownloadError:
        zip_url = find_lookup_zip_url(acs_year)
        if zip_url is None:
            raise DownloadError(
                f"Could not locate ACS lookup file for {acs_year}. "
                "Try --acs-year <previous-year>."
            )
        zip_dest = lookup_dest.with_suffix(".zip")
        zip_status = download_file(
            zip_url,
            zip_dest,
            overwrite=overwrite,
            resume=resume,
            dry_run=dry_run,
            timeout=timeout,
            max_attempts=max_attempts,
        )
        if dry_run:
            return f"{zip_status} (lookup zip planned)"
        if lookup_dest.exists() and not overwrite:
            return "exists"
        extract_lookup_from_zip(zip_dest, lookup_dest)
        return f"{zip_status} + extracted"


def extract_first_txt_from_zip(zip_path: Path, dest: Path) -> None:
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".txt")]
        if not members:
            raise DownloadError(f"No .txt payload in zip: {zip_path}")
        member = members[0]
        with zf.open(member) as src, dest.open("wb") as out:
            shutil.copyfileobj(src, out)


def fetch_acs_geography_file(
    acs_year: str,
    state: str,
    dest: Path,
    *,
    overwrite: bool,
    resume: bool,
    dry_run: bool,
    timeout: int,
    max_attempts: int,
) -> str:
    base = f"{ACS_ROOT}{acs_year}/"
    filename = f"g{acs_year}5{state}.csv"
    candidates = [
        urljoin(base, f"data/5_year_seq_by_state/{filename}"),
        urljoin(base, f"sequence-based-SF/documentation/geography/5yr_year_geo/{filename}"),
    ]
    for url in candidates:
        if not url_exists(url):
            continue
        return download_file(
            url,
            dest,
            overwrite=overwrite,
            resume=resume,
            dry_run=dry_run,
            timeout=timeout,
            max_attempts=max_attempts,
        )
    raise DownloadError(f"Could not locate geography file for {filename}")


def fetch_acs_estimate_file(
    acs_year: str,
    state: str,
    seq: str,
    dest: Path,
    *,
    overwrite: bool,
    resume: bool,
    dry_run: bool,
    timeout: int,
    max_attempts: int,
) -> str:
    base = f"{ACS_ROOT}{acs_year}/"
    txt_name = f"e{acs_year}5{state}{seq}000.txt"
    old_url = urljoin(base, f"data/5_year_seq_by_state/{txt_name}")
    if url_exists(old_url):
        return download_file(
            old_url,
            dest,
            overwrite=overwrite,
            resume=resume,
            dry_run=dry_run,
            timeout=timeout,
            max_attempts=max_attempts,
        )

    state_dir = STATE_DIR_NAMES.get(state)
    if not state_dir:
        raise DownloadError(f"Unsupported state abbreviation for ACS sequence path: {state}")

    zip_name = f"{acs_year}5{state}{seq}000.zip"
    zip_url = urljoin(
        base,
        "sequence-based-SF/data/5_year_seq_by_state/"
        f"{state_dir}/All_Geographies_Not_Tracts_Block_Groups/{zip_name}",
    )
    if not url_exists(zip_url):
        raise DownloadError(f"Could not locate estimate file for {txt_name}")

    zip_dest = dest.with_suffix(".zip")
    zip_status = download_file(
        zip_url,
        zip_dest,
        overwrite=overwrite,
        resume=resume,
        dry_run=dry_run,
        timeout=timeout,
        max_attempts=max_attempts,
    )
    if dry_run:
        return f"{zip_status} (acs zip planned)"
    if dest.exists() and not overwrite:
        return "exists"
    extract_first_txt_from_zip(zip_dest, dest)
    return f"{zip_status} + extracted"


def verify_required_files(paths: list[Path], dry_run: bool) -> None:
    if dry_run:
        log("Dry-run: verification skipped.")
        return
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        preview = "\n".join(missing[:10])
        raise DownloadError(f"Missing required files ({len(missing)}):\n{preview}")


def parse_states(raw: str | None) -> list[str]:
    if not raw:
        return list(DEFAULT_STATES)
    states = [s.strip().lower() for s in raw.split(",") if s.strip()]
    if "us" not in states:
        states.append("us")
    return states


def is_managed_filename(name: str) -> bool:
    candidate = name[:-5] if name.endswith(".part") else name
    return any(pattern.match(candidate) for pattern in MANAGED_PATTERNS)


def list_managed_files(out_dir: Path) -> list[Path]:
    if not out_dir.exists():
        return []
    return sorted(
        path for path in out_dir.iterdir() if path.is_file() and is_managed_filename(path.name)
    )


def archive_existing(out_dir: Path, dry_run: bool) -> None:
    files = list_managed_files(out_dir)
    if not files:
        log("No managed files to archive.")
        return

    stamp = time.strftime("%Y%m%d-%H%M%S")
    archive_dir = out_dir / "archive" / stamp
    log(f"Archiving {len(files)} managed files to {archive_dir}")
    for src in files:
        dst = archive_dir / src.name
        if dry_run:
            log(f"DRY-RUN: move {src} -> {dst}")
            continue
        archive_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))


def clean_existing(out_dir: Path, dry_run: bool) -> None:
    files = list_managed_files(out_dir)
    if not files:
        log("No managed files to clean.")
        return

    log(f"Cleaning {len(files)} managed files from {out_dir}")
    for path in files:
        if dry_run:
            log(f"DRY-RUN: delete {path}")
            continue
        path.unlink()


def gazetteer_source_candidates(year: str, txt_name: str) -> list[tuple[str, bool]]:
    zip_name = txt_name.replace(".txt", ".zip")
    return [
        (urljoin(GAZETTEER_ROOT, txt_name), False),
        (urljoin(GAZETTEER_ROOT, f"{year}_Gazetteer/{txt_name}"), False),
        (urljoin(GAZETTEER_ROOT, f"{year}_Gazetteer/{zip_name}"), True),
    ]


def fetch_gazetteer_file(
    txt_name: str,
    dest: Path,
    *,
    gaz_year: str,
    overwrite: bool,
    resume: bool,
    dry_run: bool,
    timeout: int,
    max_attempts: int,
) -> str:
    last_error = None
    for url, is_zip in gazetteer_source_candidates(gaz_year, txt_name):
        try:
            if not is_zip:
                return download_file(
                    url,
                    dest,
                    overwrite=overwrite,
                    resume=resume,
                    dry_run=dry_run,
                    timeout=timeout,
                    max_attempts=max_attempts,
                )

            zip_dest = dest.with_suffix(".zip")
            zip_status = download_file(
                url,
                zip_dest,
                overwrite=overwrite,
                resume=resume,
                dry_run=dry_run,
                timeout=timeout,
                max_attempts=max_attempts,
            )
            if dry_run:
                return f"{zip_status} (zip planned)"

            if dest.exists() and not overwrite:
                return "exists"

            with zipfile.ZipFile(zip_dest, "r") as zf:
                members = zf.namelist()
                member = None
                if txt_name in members:
                    member = txt_name
                else:
                    for item in members:
                        if item.endswith("/" + txt_name) or item.endswith(txt_name):
                            member = item
                            break
                if member is None:
                    raise DownloadError(f"Zip file does not contain {txt_name}: {zip_dest}")
                with zf.open(member) as src, dest.open("wb") as out:
                    shutil.copyfileobj(src, out)

            return f"{zip_status} + extracted"
        except RateLimitedError:
            raise
        except (DownloadError, zipfile.BadZipFile) as e:
            last_error = e
            continue

    if last_error:
        raise DownloadError(f"Could not fetch gazetteer file {txt_name}: {last_error}")
    raise DownloadError(f"Could not fetch gazetteer file {txt_name}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch latest ACS + gazetteer data files.")
    parser.add_argument("--out-dir", default="../000-data", help="output directory (default: ../000-data)")
    parser.add_argument("--acs-year", help="ACS year to fetch (default: latest discovered)")
    parser.add_argument("--gazetteer-year", help="gazetteer year (default: latest discovered)")
    parser.add_argument("--states", help="comma-separated states (default: all expected by geodata + us)")
    parser.add_argument("--overwrite", action="store_true", help="overwrite existing complete files")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--archive-existing",
        action="store_true",
        help="archive managed files to out-dir/archive/<timestamp> before downloading",
    )
    mode_group.add_argument(
        "--clean",
        action="store_true",
        help="delete managed files before downloading",
    )
    parser.add_argument("--no-resume", action="store_true", help="disable resumable downloads")
    parser.add_argument("--dry-run", action="store_true", help="print plan without downloading")
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds")
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="max retry attempts for network fetches (default: 3)",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir).expanduser().resolve()
    states = parse_states(args.states)

    try:
        lookup_dest = out_dir / "ACS_5yr_Seq_Table_Number_Lookup.txt"
        acs_start_year = int(args.acs_year) if args.acs_year else int(discover_latest_acs_year())
        acs_year = str(acs_start_year)
        status = None
        last_lookup_error = None

        # If auto-discovering, step back a few years until lookup fetch works.
        fallback_years = [acs_start_year] if args.acs_year else list(
            range(acs_start_year, max(2009, acs_start_year - 4) - 1, -1)
        )

        if args.archive_existing:
            archive_existing(out_dir, dry_run=args.dry_run)
        elif args.clean:
            clean_existing(out_dir, dry_run=args.dry_run)

        for year in fallback_years:
            try:
                acs_year = str(year)
                status = fetch_lookup_file(
                    acs_year,
                    lookup_dest,
                    overwrite=args.overwrite,
                    resume=not args.no_resume,
                    dry_run=args.dry_run,
                    timeout=args.timeout,
                    max_attempts=args.max_attempts,
                )
                break
            except RateLimitedError:
                raise
            except DownloadError as e:
                last_lookup_error = e
                continue

        if status is None:
            if args.acs_year:
                raise DownloadError(str(last_lookup_error))
            raise DownloadError(
                f"Could not locate ACS lookup for years {fallback_years}. "
                "Try --acs-year <year> explicitly."
            )

        if args.gazetteer_year:
            gaz_year = args.gazetteer_year
        else:
            try:
                gaz_year = discover_latest_gazetteer_year()
            except RateLimitedError:
                raise
            except DownloadError:
                # Fallback when gazetteer listing endpoints are unavailable.
                gaz_year = str(min(int(acs_year) + 1, time.gmtime().tm_year))

        log(f"ACS year: {acs_year}")
        log(f"Gazetteer year: {gaz_year}")
        log(f"Output dir: {out_dir}")
        log(f"States: {len(states)} entries")
        log(f"lookup: {status}")

        sequence_numbers: list[str]
        if args.dry_run and not lookup_dest.exists():
            # Planned run cannot parse lookup without local file.
            sequence_numbers = ["<derived-from-lookup>"]
            log("Dry-run note: sequence numbers will be derived after lookup download.")
        else:
            sequence_numbers = parse_lookup_sequence_numbers(lookup_dest, REQUIRED_TABLE_IDS)
            log(f"sequence files needed: {len(sequence_numbers)}")

        planned_geo: list[tuple[str, Path]] = []
        planned_est: list[tuple[str, str, Path]] = []

        for state in states:
            filename = f"g{acs_year}5{state}.csv"
            planned_geo.append((state, out_dir / filename))

        for state in states:
            for seq in sequence_numbers:
                if seq.startswith("<"):
                    continue
                filename = f"e{acs_year}5{state}{seq}000.txt"
                planned_est.append((state, seq, out_dir / filename))

        gaz_targets = [template.format(year=gaz_year) for template in GAZ_FILES]

        log(f"files planned: {len(planned_geo) + len(planned_est) + len(gaz_targets) + 1}")  # +1 lookup

        failures = 0
        required_paths = [lookup_dest]
        for state, dest in planned_geo:
            try:
                result = fetch_acs_geography_file(
                    acs_year,
                    state,
                    dest,
                    overwrite=args.overwrite,
                    resume=not args.no_resume,
                    dry_run=args.dry_run,
                    timeout=args.timeout,
                    max_attempts=args.max_attempts,
                )
                if result != "exists":
                    log(f"{dest.name}: {result}")
                required_paths.append(dest)
            except RateLimitedError:
                raise
            except DownloadError as e:
                failures += 1
                log(f"ERROR: {e}")

        for state, seq, dest in planned_est:
            try:
                result = fetch_acs_estimate_file(
                    acs_year,
                    state,
                    seq,
                    dest,
                    overwrite=args.overwrite,
                    resume=not args.no_resume,
                    dry_run=args.dry_run,
                    timeout=args.timeout,
                    max_attempts=args.max_attempts,
                )
                if result != "exists":
                    log(f"{dest.name}: {result}")
                required_paths.append(dest)
            except RateLimitedError:
                raise
            except DownloadError as e:
                failures += 1
                log(f"ERROR: {e}")

        for txt_name in gaz_targets:
            dest = out_dir / txt_name
            try:
                result = None
                last_err = None
                for year_offset in range(0, 5):
                    try_year = str(int(gaz_year) - year_offset)
                    try_name = txt_name.replace(gaz_year + "_", try_year + "_", 1)
                    try_dest = dest if try_year == gaz_year else (out_dir / try_name)
                    try:
                        result = fetch_gazetteer_file(
                            try_name,
                            try_dest,
                            gaz_year=try_year,
                            overwrite=args.overwrite,
                            resume=not args.no_resume,
                            dry_run=args.dry_run,
                            timeout=args.timeout,
                            max_attempts=args.max_attempts,
                        )
                        if try_year != gaz_year and not args.dry_run:
                            # Normalize filename to the year actually fetched.
                            dest = try_dest
                        break
                    except RateLimitedError:
                        raise
                    except DownloadError as e:
                        last_err = e
                        continue
                if result is None:
                    raise DownloadError(f"Could not fetch gazetteer {txt_name}: {last_err}")
                if result != "exists":
                    log(f"{dest.name}: {result}")
                required_paths.append(dest)
            except RateLimitedError:
                raise
            except DownloadError as e:
                failures += 1
                log(f"ERROR: {e}")

        if failures:
            raise DownloadError(f"Download completed with {failures} failures")

        verify_required_files(required_paths, dry_run=args.dry_run)
        log("Done. Data directory is ready for `geocompare build <out-dir>`." )
        return 0

    except RateLimitedError as e:
        log(f"ERROR: {e}")
        log("Stopping immediately to avoid further rate limiting. Try again later.")
        return 3
    except DownloadError as e:
        log(f"ERROR: {e}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
