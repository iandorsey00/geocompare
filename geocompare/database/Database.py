import csv
import json
import logging
import re
import sqlite3
from collections import defaultdict

# from initialize_sqlalchemy import Base, engine, session
from itertools import islice
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path

import numpy as np
import pandas as pd

from geocompare.models.demographic_profile import DemographicProfile
from geocompare.models.geovector import GeoVector
from geocompare.tools.geography_names import humanized_tract_name
from geocompare.tools.numeric import parse_number
from geocompare.tools.state_lookup import StateLookup

logger = logging.getLogger(__name__)


class Database:
    """Creates data products for use by geocompare."""

    LINE_NUMBERS_DICT = {
        "B01003": ["1"],  # TOTAL POPULATION
        "B01001": [  # SEX BY AGE (selected lines)
            "3",
            "4",
            "5",
            "6",  # Male under 18
            "20",
            "21",
            "22",
            "23",
            "24",
            "25",  # Male 65+
            "27",
            "28",
            "29",
            "30",  # Female under 18
            "44",
            "45",
            "46",
            "47",
            "48",
            "49",  # Female 65+
        ],
        "B01002": ["1"],  # MEDIAN AGE
        "B11001": ["1"],  # HOUSEHOLD TYPE - total households
        "B19301": ["1"],  # PER CAPITA INCOME IN THE PAST 12 MONTHS
        "B02001": ["2", "3", "5"],  # RACE
        "B03002": ["3", "12"],  # HISPANIC OR LATINO ORIGIN BY RACE
        "B04004": ["51"],  # PEOPLE REPORTING SINGLE ANCESTRY - Italian
        "B15003": ["1", "22", "23", "24", "25"],  # EDUCATIONAL ATTAINMENT
        "B17001": ["1", "2"],  # POVERTY STATUS
        "B19013": ["1"],  # MEDIAN HOUSEHOLD INCOME
        "B23025": ["3", "5"],  # EMPLOYMENT STATUS (labor force, unemployed)
        "B25003": ["1", "2"],  # TENURE (occupied, owner occupied)
        "B25010": ["1"],  # AVERAGE HOUSEHOLD SIZE
        "B25035": ["1"],  # Median year structure built
        "B25018": ["1"],  # Median number of rooms
        "B25058": ["1"],  # Median contract rent
        "B25077": ["1"],  # Median value
    }

    CRIME_METRIC_DEFS = [
        ("violent_crime_count", "Violent crimes"),
        ("property_crime_count", "Property crimes"),
        ("total_crime_count", "Total crimes"),
        ("violent_crime_rate", "Violent crime rate per 100k"),
        ("property_crime_rate", "Property crime rate per 100k"),
        ("total_crime_rate", "Total crime rate per 100k"),
    ]

    VOTER_PERCENT_METRIC_DEFS = [
        ("democratic_voters_pct", "Democratic voters (%)"),
        ("republican_voters_pct", "Republican voters (%)"),
        ("other_voters_pct", "Other voters (%)"),
    ]
    OVERLAY_GEO_LEVEL_TO_SUMLEVEL = {
        "40": "040",
        "50": "050",
        "160": "160",
        "860": "860",
    }
    BASE_PROFILE_SECTIONS = {
        "GEOGRAPHY",
        "POPULATION",
        "AGE",
        "RACE",
        "Hispanic or Latino (of any race)",
        "EDUCATION",
        "INCOME",
        "ECONOMY",
        "HOUSING",
    }

    ###########################################################################
    # Helper methods for __init__

    def _haversine_miles(self, lat1, lon1, lat2, lon2):
        r_miles = 3958.7613
        phi1 = radians(lat1)
        phi2 = radians(lat2)
        dphi = radians(lat2 - lat1)
        dlambda = radians(lon2 - lon1)

        a = sin(dphi / 2.0) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2.0) ** 2
        c = 2.0 * atan2(sqrt(a), sqrt(1.0 - a))
        return r_miles * c

    def _humanize_tract_names(self):
        county_places = defaultdict(list)
        state_places = defaultdict(list)

        for dp in self.demographicprofiles:
            if dp.sumlevel != "160":
                continue
            latitude = dp.rc.get("latitude")
            longitude = dp.rc.get("longitude")
            if latitude is None or longitude is None:
                continue
            state_places[dp.state].append(dp)
            for county_geoid in getattr(dp, "counties", []):
                county_places[county_geoid].append(dp)

        tract_aliases = {}
        for dp in self.demographicprofiles:
            if dp.sumlevel != "140":
                continue
            latitude = dp.rc.get("latitude")
            longitude = dp.rc.get("longitude")
            if latitude is None or longitude is None:
                continue

            county_geoid = dp.counties[0] if getattr(dp, "counties", None) else None
            candidates = county_places.get(county_geoid, []) if county_geoid else []
            if not candidates:
                candidates = state_places.get(dp.state, [])

            nearest_place = None
            nearest_distance = None
            for place in candidates:
                distance = self._haversine_miles(
                    latitude,
                    longitude,
                    place.rc.get("latitude"),
                    place.rc.get("longitude"),
                )
                if nearest_distance is None or distance < nearest_distance:
                    nearest_place = place
                    nearest_distance = distance

            dp.canonical_name = dp.name
            dp.name = humanized_tract_name(
                dp.geoid,
                nearby_place_name=getattr(nearest_place, "name", None),
                state_abbrev=dp.state,
            )
            tract_aliases[dp.geoid] = dp.name

        for gv in self.geovectors:
            if gv.sumlevel != "140":
                continue
            gv.canonical_name = gv.name
            gv.name = tract_aliases.get(gv.geoid, gv.name)

    def _progress(self, message, current=None, total=None):
        cb = getattr(self, "_progress_callback", None)
        if cb is None:
            return
        if current is not None and total:
            pct = int((current / total) * 100)
            cb(f"[{pct:3d}%] {message} ({current}/{total})")
        else:
            cb(message)

    def get_tm_columns(self, path):
        """Obtain columns for table_metadata"""
        columns = list(
            pd.read_csv(path / "ACS_5yr_Seq_Table_Number_Lookup.txt", nrows=1, dtype="str").columns
        )

        # Convert column headers to snake_case
        columns = list(map(lambda x: x.lower(), columns))
        columns = list(map(lambda x: x.replace(" ", "_"), columns))

        return columns

    def get_gh_columns(self, gh_year, path):
        """Obtain columns for the geoheaders table."""
        place_path = path / f"{gh_year}_Gaz_place_national.txt"
        with open(place_path, "rt", newline="") as f:
            header_line = f.readline()
        delimiter = "|" if "|" in header_line else "\t"
        return list(pd.read_csv(place_path, sep=delimiter, nrows=1, dtype="str").columns)

    def normalize_tract_gazetteer_rows(self, rows):
        """Normalize tract gazetteer rows to the place-gazetteer shape."""
        normalized_rows = []
        for row in rows:
            if not row:
                continue

            if row[0] == "USPS":
                normalized_rows.append(
                    [
                        "USPS",
                        "GEOID",
                        "GEOIDFQ",
                        "ANSICODE",
                        "NAME",
                        "LSAD",
                        "FUNCSTAT",
                        "ALAND",
                        "AWATER",
                        "ALAND_SQMI",
                        "AWATER_SQMI",
                        "INTPTLAT",
                        "INTPTLONG",
                    ]
                )
                continue

            padded = list(row) + [""] * max(0, 9 - len(row))
            usps = padded[0]
            geoid = padded[1]
            geoidfq = padded[2] or (f"1400000US{geoid}" if geoid and "US" not in geoid else geoid)
            normalized_rows.append(
                [
                    usps,
                    geoidfq,
                    geoidfq,
                    "",
                    "",
                    "",
                    "",
                    padded[3],
                    padded[4],
                    padded[5],
                    padded[6],
                    padded[7],
                    padded[8],
                ]
            )

        return normalized_rows

    def get_state_gazetteer_path(self, gh_year, path):
        """Resolve the state gazetteer file path with backward compatibility."""
        candidates = [
            path / f"{gh_year}_Gaz_state_national.txt",
            path / "2019_Gaz_state_national.txt",
        ]

        for candidate in candidates:
            if candidate.exists():
                return candidate

        dynamic_candidates = sorted(path.glob("*_Gaz_state_national.txt"))
        if dynamic_candidates:
            return dynamic_candidates[-1]

        candidate_list = ", ".join(str(p.name) for p in candidates)
        raise FileNotFoundError(
            f"Unable to find a state gazetteer file. Expected one of: {candidate_list}"
        )

    def detect_latest_acs_year(self, path):
        years = []
        for candidate in path.glob("Geos*5YR.txt"):
            match = re.match(r"^Geos(\d{4})5YR\.txt$", candidate.name)
            if match:
                years.append(int(match.group(1)))
        for pattern, regex in (
            ("g*5us.csv", r"^g(\d{4})5us\.csv$"),
            ("g*5us.txt", r"^g(\d{4})5us\.txt$"),
        ):
            for candidate in path.glob(pattern):
                match = re.match(regex, candidate.name)
                if match:
                    years.append(int(match.group(1)))
        if not years:
            raise FileNotFoundError(
                "Unable to detect ACS year. Expected Geos<YEAR>5YR.txt, "
                "g<YEAR>5us.csv, or g<YEAR>5us.txt."
            )
        return str(max(years))

    def detect_acs_layout(self, path, year):
        if (path / f"Geos{year}5YR.txt").exists():
            return "table"
        for candidate in (
            path / f"g{year}5us.csv",
            path / f"g{year}5us.txt",
        ):
            if candidate.exists():
                return "sequence"
        raise FileNotFoundError(
            f"Unable to detect ACS layout for {year}. "
            f"Expected Geos{year}5YR.txt or g{year}5us.csv/txt."
        )

    def resolve_table_geography_path(self, year):
        candidate = self.data_dir / f"Geos{year}5YR.txt"
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"Missing table-based geography file: {candidate.name}")

    def resolve_table_data_path(self, year, table_id):
        candidate = self.data_dir / f"acsdt5y{year}-{table_id.lower()}.dat"
        if candidate.exists():
            return candidate
        raise FileNotFoundError(
            f"Missing table-based ACS data file for {table_id}: {candidate.name}"
        )

    def resolve_geo_file_path(self, year, state):
        candidates = [
            self.data_dir / f"g{year}5{state}.csv",
            self.data_dir / f"g{year}5{state}.txt",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        raise FileNotFoundError(
            f"Missing ACS geography file for {state}: expected one of "
            + ", ".join(path.name for path in candidates)
        )

    def detect_latest_gazetteer_year(self, path):
        years = []
        for candidate in path.glob("*_Gaz_place_national.txt"):
            match = re.match(r"^(\d{4})_Gaz_place_national\.txt$", candidate.name)
            if match:
                years.append(int(match.group(1)))
        if not years:
            raise FileNotFoundError(
                "Unable to detect gazetteer year. Expected a <YEAR>_Gaz_place_national.txt file."
            )
        return str(max(years))

    def _normalize_geoid_keys(self, geoid):
        geoid = geoid.strip()
        keys = {geoid}
        if "US" in geoid:
            keys.add(geoid.split("US", 1)[1])
        if len(geoid) >= 7:
            keys.add(geoid[7:])
        return keys

    def _iter_overlay_candidates(self, path):
        overlay_dir = path / "overlays"
        if not overlay_dir.exists():
            return

        preferred = [
            overlay_dir / "crime_data.csv",
            overlay_dir / "voter_data.csv",
            overlay_dir / "project_data.csv",
        ]
        emitted = set()
        for candidate in preferred:
            if candidate.exists() and candidate.is_file():
                emitted.add(candidate.name)
                yield candidate

        for candidate in sorted(overlay_dir.iterdir(), key=lambda p: p.name):
            lowered_name = candidate.name.lower()
            if (
                not candidate.is_file()
                or candidate.suffix.lower() not in {".csv", ".json"}
                or candidate.name in emitted
                or candidate.name in {"overlay_manifest.json", "manifest.json"}
                or lowered_name.endswith("_manifest.json")
                or lowered_name.endswith("_coverage.json")
            ):
                continue
            yield candidate

    def _load_overlay_manifest(self, path):
        overlay_dir = path / "overlays"
        self.overlay_manifest_stats = {"path": None, "metrics": 0}
        if not overlay_dir.exists():
            return {}

        for name in ("overlay_manifest.json", "manifest.json"):
            manifest_path = overlay_dir / name
            if not manifest_path.exists():
                continue
            try:
                with open(manifest_path, "rt") as f:
                    payload = json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                logger.warning("Unable to load overlay manifest %s: %s", manifest_path.name, e)
                return {}
            if not isinstance(payload, dict):
                logger.warning("Skipping overlay manifest %s: expected object.", manifest_path.name)
                return {}

            metric_defs = payload.get("metrics", [])
            if not isinstance(metric_defs, list):
                logger.warning(
                    "Skipping overlay manifest %s: metrics must be a list.", manifest_path.name
                )
                return {}

            by_key = {}
            for metric in metric_defs:
                if not isinstance(metric, dict):
                    continue
                key = str(metric.get("key", "")).strip().lower().replace(" ", "_")
                if not key:
                    continue
                by_key[key] = metric

            self.overlay_manifest_stats = {"path": manifest_path.name, "metrics": len(by_key)}
            return by_key
        return {}

    def _overlay_meta(self, metric_key):
        key = str(metric_key or "").strip().lower().replace(" ", "_")
        if not key:
            return None
        meta_by_key = getattr(self, "overlay_metric_meta", {})
        if key in meta_by_key:
            return meta_by_key[key]
        if key.startswith("project_"):
            return meta_by_key.get(key[8:])
        return meta_by_key.get(f"project_{key}")

    def _load_csv_overlay(self, overlay_path):
        with open(overlay_path, "rt", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        if not rows:
            return {}

        geoid_col = None
        for col in rows[0].keys():
            if col and col.strip().lower() == "geoid":
                geoid_col = col
                break
        if geoid_col is None:
            logger.warning("Skipping overlay %s: missing GEOID column.", overlay_path.name)
            return {}

        overlays = {}
        for row in rows:
            geoid = (row.get(geoid_col) or "").strip()
            if not geoid:
                continue

            metric_values = {}
            for key, value in row.items():
                if not key or key == geoid_col:
                    continue
                text = (value or "").strip()
                if text == "":
                    continue
                try:
                    metric_values[key.strip()] = float(text)
                except ValueError:
                    continue
            if metric_values:
                storage_key = self._overlay_storage_key(geoid, metric_values)
                overlays[storage_key] = metric_values
        return overlays

    def _load_json_overlay(self, overlay_path):
        with open(overlay_path, "rt") as f:
            payload = json.load(f)
        if not isinstance(payload, list):
            logger.warning("Skipping overlay %s: expected a list of records.", overlay_path.name)
            return {}

        overlays = {}
        for row in payload:
            if not isinstance(row, dict):
                continue
            geoid = str(row.get("GEOID") or row.get("geoid") or "").strip()
            if not geoid:
                continue
            metric_values = {}
            for key, value in row.items():
                if key in ("GEOID", "geoid"):
                    continue
                if isinstance(value, (int, float)):
                    metric_values[str(key)] = float(value)
            if metric_values:
                storage_key = self._overlay_storage_key(geoid, metric_values)
                overlays[storage_key] = metric_values
        return overlays

    def _overlay_storage_key(self, geoid, metrics):
        sumlevel = self._overlay_sumlevel(metrics)
        if sumlevel:
            return f"{geoid}__sl{sumlevel}"
        return geoid

    def _overlay_storage_geoid(self, storage_key):
        key = str(storage_key)
        if "__sl" in key:
            return key.split("__sl", 1)[0]
        return key

    def _load_overlays(self, path):
        merged = {}
        self.overlay_load_stats = {
            "files_loaded": [],
            "files_failed": [],
            "rows_loaded": 0,
            "geoids_loaded": 0,
            "metrics_loaded": 0,
        }
        for overlay_path in self._iter_overlay_candidates(path):
            try:
                if overlay_path.suffix.lower() == ".json":
                    overlay_values = self._load_json_overlay(overlay_path)
                else:
                    overlay_values = self._load_csv_overlay(overlay_path)
            except (OSError, ValueError, json.JSONDecodeError) as e:
                logger.warning("Unable to load overlay %s: %s", overlay_path.name, e)
                self.overlay_load_stats["files_failed"].append(overlay_path.name)
                continue

            geoid_count = len(overlay_values)
            metric_count = sum(len(metrics) for metrics in overlay_values.values())
            self.overlay_load_stats["files_loaded"].append(
                {
                    "name": overlay_path.name,
                    "geoids": geoid_count,
                    "metrics": metric_count,
                }
            )
            self.overlay_load_stats["rows_loaded"] += geoid_count
            self.overlay_load_stats["metrics_loaded"] += metric_count
            for geoid, metrics in overlay_values.items():
                merged.setdefault(geoid, {}).update(metrics)

        self.overlay_load_stats["geoids_loaded"] = len(merged)
        return merged

    def _read_gaz_rows(self, file_path):
        with open(file_path, "rt", newline="") as f:
            header_line = f.readline()
            f.seek(0)
            delimiter = "|" if "|" in header_line else "\t"
            return list(csv.reader(f, delimiter=delimiter))

    def _add_overlay_metric(self, dp, section_title, metric_key, metric_value):
        raw_key = metric_key.lower().strip()
        key = raw_key.replace(" ", "_")
        meta = self._overlay_meta(key)
        if section_title == "PROJECT DATA" and not key.startswith("project_"):
            key = f"project_{key}"
        label = raw_key.replace("_", " ").title()
        if isinstance(meta, dict) and meta.get("label"):
            label = str(meta["label"]).strip()
        value_display = None
        compound_value = None
        compound_display = None
        compound_suffix = "%"

        for known_key, known_label in self.CRIME_METRIC_DEFS:
            if key == known_key:
                label = known_label
                if key.endswith("_rate"):
                    value_display = f"{metric_value:,.1f}"
                else:
                    value_display = f"{metric_value:,.0f}"
                break
        for known_key, known_label in self.VOTER_PERCENT_METRIC_DEFS:
            if key == known_key:
                label = known_label
                value_display = f"{metric_value:,.1f}%"
                break

        if key == "registered_voters":
            label = "Registered voters"
            value_display = f"{metric_value:,.0f}"
        elif key == "democratic_voters":
            label = "Democratic voters"
        elif key == "republican_voters":
            label = "Republican voters"
        elif key == "other_voters":
            label = "Other voters"

        if value_display is None and isinstance(meta, dict):
            metric_type = str(meta.get("type", "")).strip().lower()
            if metric_type in {"pct", "percent", "percentage"}:
                value_display = f"{metric_value:,.1f}%"
            elif metric_type in {"count", "integer"}:
                value_display = f"{metric_value:,.0f}"
            elif metric_type in {"rate", "rate_per_100k", "score"}:
                value_display = f"{metric_value:,.3f}"

        # For voter registration metrics, show percentages inline in the same row.
        if key == "registered_voters" and dp.rc.get("population", 0):
            compound_value = metric_value / dp.rc["population"] * 100.0
            compound_display = f"{compound_value:,.1f}%"
            compound_suffix = None

        if key in {"democratic_voters", "republican_voters", "other_voters"} and dp.rc.get(
            "registered_voters", 0
        ):
            registered = dp.rc["registered_voters"]
            if registered:
                compound_value = metric_value / registered * 100.0
                compound_display = f"{compound_value:,.1f}%"
                compound_suffix = None

        if value_display is None:
            if float(metric_value).is_integer():
                value_display = f"{metric_value:,.0f}"
            else:
                value_display = f"{metric_value:,.3f}"

        if key.endswith("_count") and dp.rc.get("population", 0):
            compound_value = metric_value / dp.rc["population"] * 100000.0
            compound_display = f"{compound_value:,.1f}/100k"
            compound_suffix = None

        show_in_profile = True
        if section_title == "CRIME" and key.endswith("_rate"):
            show_in_profile = False
        if section_title == "VOTER REGISTRATION" and (
            key in {"democratic_voters_pct", "republican_voters_pct", "other_voters_pct"}
        ):
            show_in_profile = False

        indent = 0
        if key in {"democratic_voters", "republican_voters", "other_voters"}:
            indent = 2

        dp.add_custom_metric(
            section_title=section_title,
            key=key,
            label=label,
            value=metric_value,
            indent=indent,
            value_display=value_display,
            compound_value=compound_value,
            compound_display=compound_display,
            compound_suffix=compound_suffix,
            show_in_profile=show_in_profile,
        )

    def _derive_crime_rate_metrics(self, metrics, population):
        if not population:
            return {}

        derived = {}
        for count_key in (
            "violent_crime_count",
            "property_crime_count",
            "total_crime_count",
        ):
            rate_key = count_key.replace("_count", "_rate")
            if rate_key in metrics:
                continue

            count_value = metrics.get(count_key)
            if count_value is None:
                continue
            try:
                count_value = float(count_value)
            except (TypeError, ValueError):
                continue

            derived[rate_key] = count_value / population * 100000.0

        return derived

    def _derive_voter_share_metrics(self, metrics):
        registered = metrics.get("registered_voters")
        if not registered:
            return {}
        try:
            registered = float(registered)
        except (TypeError, ValueError):
            return {}
        if registered <= 0:
            return {}

        derived = {}
        for party in ("democratic", "republican", "other"):
            percent_key = f"{party}_voters_pct"
            if percent_key in metrics:
                continue

            count_key = f"{party}_voters"
            count_value = metrics.get(count_key)
            if count_value is None:
                continue
            try:
                count_value = float(count_value)
            except (TypeError, ValueError):
                continue

            derived[percent_key] = count_value / registered * 100.0

        return derived

    def _overlay_section(self, metric_key):
        meta = self._overlay_meta(metric_key)
        if isinstance(meta, dict) and meta.get("section"):
            return str(meta["section"]).strip()
        lowered = metric_key.lower()
        if "crime" in lowered:
            return "CRIME"
        if "voter" in lowered:
            return "VOTER REGISTRATION"
        return "PROJECT DATA"

    def _overlay_sumlevel(self, metrics):
        if not isinstance(metrics, dict):
            return None
        value = metrics.get("social_geo_level_code")
        if value is None:
            return None
        try:
            numeric = int(float(value))
        except (TypeError, ValueError):
            return None
        return self.OVERLAY_GEO_LEVEL_TO_SUMLEVEL.get(str(numeric))

    def _overlay_row_sort_key(self, dp, row):
        _, key = row
        meta = self._overlay_meta(key)
        display_label = dp.rh.get(key, key).strip().lower()
        order = 1_000_000
        voter_order = {
            "registered_voters": 0,
            "democratic_voters": 1,
            "republican_voters": 2,
            "other_voters": 3,
        }
        if key in voter_order:
            return (voter_order[key], display_label, key)
        if isinstance(meta, dict):
            try:
                order = float(meta.get("order", order))
            except (TypeError, ValueError):
                order = 1_000_000
        return (order, display_label, key)

    def _overlay_section_sort_key(self, section_title):
        if section_title == "CRIME":
            return (0, section_title)
        if section_title == "VOTER REGISTRATION":
            return (1, section_title)
        if section_title == "PROJECT DATA":
            return (2, section_title)
        return (3, section_title.lower())

    def _finalize_overlay_display_sections(self):
        for dp in self.demographicprofiles:
            if not hasattr(dp, "display_sections"):
                continue
            base_sections = []
            overlay_sections = []
            for section_title, rows in dp.display_sections:
                if section_title in self.BASE_PROFILE_SECTIONS:
                    base_sections.append((section_title, rows))
                else:
                    sorted_rows = sorted(rows, key=lambda row: self._overlay_row_sort_key(dp, row))
                    overlay_sections.append((section_title, sorted_rows))
            overlay_sections.sort(key=lambda item: self._overlay_section_sort_key(item[0]))
            dp.display_sections = base_sections + overlay_sections

    def apply_overlays(self):
        if not self.overlays:
            return {
                "overlay_geoids": 0,
                "matched_profiles": 0,
                "metrics_added": 0,
                "unmatched_overlay_geoids": 0,
            }

        dp_index = defaultdict(list)
        for dp in self.demographicprofiles:
            for key in self._normalize_geoid_keys(dp.geoid):
                dp_index[key].append(dp)

        matched_geoids = 0
        unmatched_geoids = 0
        matched_profiles = set()
        metrics_added = 0

        for storage_key, metrics in self.overlays.items():
            target_sumlevel = self._overlay_sumlevel(metrics)
            geoid = self._overlay_storage_geoid(storage_key)
            matches = {}
            for key in self._normalize_geoid_keys(geoid):
                for dp in dp_index.get(key, []):
                    if target_sumlevel and getattr(dp, "sumlevel", None) != target_sumlevel:
                        continue
                    matches[dp.geoid] = dp
            if not matches:
                unmatched_geoids += 1
                continue
            matched_geoids += 1

            for dp in matches.values():
                matched_profiles.add(dp.geoid)
                effective_metrics = dict(metrics)
                effective_metrics.update(
                    self._derive_crime_rate_metrics(
                        effective_metrics,
                        dp.rc.get("population", 0),
                    )
                )
                effective_metrics.update(self._derive_voter_share_metrics(effective_metrics))

                for metric_key, metric_value in effective_metrics.items():
                    section = self._overlay_section(metric_key)
                    self._add_overlay_metric(dp, section, metric_key, metric_value)
                    metrics_added += 1

        self._finalize_overlay_display_sections()

        return {
            "overlay_geoids": len(self.overlays),
            "matched_overlay_geoids": matched_geoids,
            "unmatched_overlay_geoids": unmatched_geoids,
            "matched_profiles": len(matched_profiles),
            "metrics_added": metrics_added,
        }

    def dbapi_qm_substr(self, columns_len):
        """Get the DBAPI question mark substring"""
        return ", ".join(["?"] * columns_len)

    def dbapi_update_qm_substr(self, columns_len):
        """Get the DBAPI question mark substring for UPDATE stmts"""
        return ", ".join(["? = ?"] * columns_len)

    # ido = id_offset: Set it to one if there is an id that columns should
    # ignore. Otherwise, if there is no seperate id column, set it 0.
    def create_table(self, table_name, columns, column_defs, rows, ido=1):
        """Create a staging table for geocompare."""
        # DBAPI question mark substring
        columns_len = len(column_defs) - ido
        question_mark_substr = self.dbapi_qm_substr(columns_len)

        # CREATE TABLE statement
        self.c.execute(
            """CREATE TABLE %s
                          (%s)"""
            % (table_name, ", ".join(column_defs))
        )

        # Insert rows into table
        self.c.executemany(
            "INSERT INTO %s(%s) VALUES (%s)"
            % (table_name, ", ".join(columns), question_mark_substr),
            rows,
        )

    def debug_output_table(self, table_name):
        """Print debug information for a table"""
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug("%s table:", table_name)
        for row in self.c.execute("SELECT * FROM %s LIMIT 5" % table_name):
            logger.debug("%s", row)

    def debug_output_list(self, list_name):
        """Print debug information for a list"""
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug("%s:", list_name)
        for row in getattr(self, list_name)[:5]:
            logger.debug("%s", row)

    def take(self, n, iterable):
        """Return first n items of the iterable as a list"""
        return list(islice(iterable, n))

    def debug_output_dict(self, dict_name):
        """Print debug information for a dictionary"""
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug("%s:", dict_name)
        for key, value in self.take(5, getattr(self, dict_name).items()):
            logger.debug("%s: %s", key, value)

    def get_geo_csv_rows(self):
        """Get normalized geography rows from ACS geography source files."""
        rows = []

        if getattr(self, "acs_layout", "sequence") == "table":
            table_geo_path = self.resolve_table_geography_path(self.year)
            with open(table_geo_path, "rt", newline="") as f:
                reader = csv.DictReader(f, delimiter="|")
                for row in reader:
                    geoid = (row.get("GEO_ID") or "").strip()
                    if not geoid:
                        continue
                    rows.append(
                        [
                            (row.get("STUSAB") or "US").strip().lower(),
                            (row.get("SUMLEVEL") or "").strip(),
                            geoid,
                            geoid,
                            (row.get("NAME") or "").strip(),
                        ]
                    )
            return rows

        def parse_geo_txt_line(line):
            if not line:
                return None

            stusab = line[6:8].strip().lower() if len(line) >= 8 else ""
            sumlevel = line[8:11].strip() if len(line) >= 11 else ""
            logrecno = line[13:20].strip() if len(line) >= 20 else ""
            geoid_match = re.search(r"\b(\d{3}[A-Z0-9]{2}US[0-9A-Z]+)\b", line)
            if not (stusab and sumlevel and logrecno and geoid_match):
                return None

            geoid = geoid_match.group(1).strip()
            name = line[geoid_match.end() :].strip()
            if not name:
                return None

            return [stusab, sumlevel, logrecno, geoid, name]

        def parse_geo_csv_row(row):
            if len(row) < 50:
                return None
            return [
                row[1].lower().strip(),
                row[2].strip(),
                row[4].strip(),
                row[48].strip(),
                row[49].strip(),
            ]

        def add_rows_from_file(this_path):
            if this_path.suffix.lower() == ".txt":
                with open(this_path, "rt", encoding="iso-8859-1") as f:
                    for line in f:
                        parsed = parse_geo_txt_line(line.rstrip("\n"))
                        if parsed is not None:
                            rows.append(parsed)
            else:
                with open(this_path, "rt", encoding="iso-8859-1") as f:
                    for raw_row in csv.reader(f):
                        parsed = parse_geo_csv_row(raw_row)
                        if parsed is not None:
                            rows.append(parsed)

        # Get rows from files for each state.
        for state in self.st.get_abbrevs(lowercase=True):
            this_path = self.resolve_geo_file_path(self.year, state)
            add_rows_from_file(this_path)

        # Also, get rows for the national file (for ZCTA support).
        this_path = self.resolve_geo_file_path(self.year, "us")
        add_rows_from_file(this_path)

        return rows

    def _table_data_column_candidates(self, table_id, line_number):
        normalized = str(int(line_number)).zfill(3)
        compact = str(int(line_number))
        return [
            f"{table_id}_E{normalized}",
            f"{table_id}_E{compact}",
            f"{table_id}_{normalized}E",
            f"{table_id}_{compact}E",
        ]

    def _load_table_based_data(self):
        self.data_identifiers = {}
        self.data_identifiers_list = ["STATE", "LOGRECNO"]

        for table_id, line_numbers in self.line_numbers_dict.items():
            self.data_identifiers[table_id] = ["STATE", "LOGRECNO"]
            for line_number in line_numbers:
                this_data_identifier = table_id + "_" + line_number
                self.data_identifiers[table_id].append(this_data_identifier)
                self.data_identifiers_list.append(this_data_identifier)

        columns = self.data_identifiers_list
        self.data_columns = columns
        column_defs = list(map(lambda x: x + " TEXT", columns))
        column_defs.append("PRIMARY KEY(STATE, LOGRECNO)")

        this_table_name = "data"
        self.c.execute(
            """CREATE TABLE %s
                          (%s)"""
            % (this_table_name, ", ".join(column_defs))
        )

        geographies = self.c.execute("SELECT STUSAB, LOGRECNO, GEOID FROM geographies").fetchall()
        geoid_to_key = {row[2]: (row[0], row[1]) for row in geographies if row[2]}

        rows_by_key = {}
        for table_id, line_numbers in self.line_numbers_dict.items():
            this_path = self.resolve_table_data_path(self.year, table_id)
            with open(this_path, "rt", newline="") as f:
                reader = csv.DictReader(f, delimiter="|")
                if reader.fieldnames is None:
                    continue
                field_map = {name.upper(): name for name in reader.fieldnames}
                for row in reader:
                    geoid = (row.get(field_map.get("GEO_ID", "GEO_ID")) or "").strip()
                    if not geoid:
                        continue
                    key = geoid_to_key.get(geoid)
                    if key is None:
                        continue
                    state, logrecno = key
                    record = rows_by_key.setdefault(
                        key,
                        {
                            "STATE": state,
                            "LOGRECNO": logrecno,
                        },
                    )
                    for line_number in line_numbers:
                        target = f"{table_id}_{line_number}"
                        value = ""
                        for candidate in self._table_data_column_candidates(table_id, line_number):
                            source_col = field_map.get(candidate.upper())
                            if source_col is None:
                                continue
                            value = (row.get(source_col) or "").strip()
                            break
                        record[target] = value

        if rows_by_key:
            insert_rows = []
            for record in rows_by_key.values():
                insert_rows.append([record.get(column, "") for column in columns])
            self.c.executemany(
                "INSERT INTO data(%s) VALUES (%s)"
                % (
                    ", ".join(columns),
                    self.dbapi_qm_substr(len(columns)),
                ),
                insert_rows,
            )

    def _load_sequence_based_data(self):
        # Get needed table metadata.
        self.table_metadata = []

        for table_id, line_numbers in self.line_numbers_dict.items():
            self.table_metadata += self.c.execute(
                """SELECT * FROM table_metadata
                WHERE table_id = ? AND (line_number IN (%s) OR line_number = '')"""
                % (self.dbapi_qm_substr(len(line_numbers))),
                [table_id] + line_numbers,
            )

        self.debug_output_list("table_metadata")

        # Obtain needed sequence numbers                                  #####
        self.sequence_numbers = dict()

        for table_metadata_row in self.table_metadata:
            table_id = table_metadata_row[2]
            sequence_number = table_metadata_row[3]

            # Create the key for the table_id if it doesn't exist.
            if table_id not in self.sequence_numbers.keys():
                self.sequence_numbers[table_id] = []

            self.sequence_numbers[table_id].append(sequence_number)

        # Remove duplicate sequence numbers
        for key, value in self.sequence_numbers.items():
            self.sequence_numbers[key] = list(dict.fromkeys(value))

        self.debug_output_dict("sequence_numbers")

        # Obtain needed files                                             #####
        self.files = dict()

        for table_id, sequence_numbers in self.sequence_numbers.items():
            if table_id not in self.files.keys():
                self.files[table_id] = []

            for sequence_number in sequence_numbers:
                for state in self.st.get_abbrevs(lowercase=True, inc_us=True):
                    this_path = self.data_dir / f"e{self.year}5{state}{sequence_number}000.txt"
                    self.files[table_id].append(this_path)

        self.debug_output_dict("files")

        # Obtain needed positions                                         #####
        self.positions = dict()
        last_start_position = ""
        last_line_number = ""

        for table_metadata_row in self.table_metadata:
            table_id = table_metadata_row[2]
            start_position = table_metadata_row[5]
            line_number = table_metadata_row[4]

            # If the table_id hasn't been added to the keys yet, set the key
            # to a list containing 5 (the position for LOGRECNO).
            if table_id not in self.positions.keys():
                self.positions[table_id] = [2, 5]

            # Once we hit our start_position, get it and subtract one since
            # they start at one, not zero.
            if start_position:
                last_start_position = int(start_position) - 1

            # If we hit a line number and it's a line number we need, get it,
            # add it to the start_position, then subtract one again since
            # line numbers also start at zero.
            elif line_number in self.line_numbers_dict[table_id]:
                last_line_number = int(line_number)
                self.positions[table_id].append(last_start_position + last_line_number - 1)

        self.debug_output_dict("positions")

        # Obtain needed data_identifiers                                  #####
        self.data_identifiers = dict()
        self.data_identifiers_list = ["STATE", "LOGRECNO"]

        for table_id, line_numbers in self.line_numbers_dict.items():
            # If there is no such key, start with 'LOGRECNO'
            if table_id not in self.data_identifiers.keys():
                self.data_identifiers[table_id] = ["STATE", "LOGRECNO"]

            # Add the data_identifiers.
            # Format: <table_id>_<line_number>
            for line_number in line_numbers:
                this_data_identifier = table_id + "_" + line_number
                self.data_identifiers[table_id].append(this_data_identifier)
                self.data_identifiers_list.append(this_data_identifier)

        self.debug_output_dict("data_identifiers")
        self.debug_output_list("data_identifiers_list")

        # data ################################################################
        this_table_name = "data"

        columns = self.data_identifiers_list
        self.data_columns = columns
        column_defs = list(map(lambda x: x + " TEXT", columns))
        column_defs.append("PRIMARY KEY(STATE, LOGRECNO)")

        # CREATE TABLE statement
        self.c.execute(
            """CREATE TABLE %s
                          (%s)"""
            % (this_table_name, ", ".join(column_defs))
        )

        # Map indices (idx) to elements from list
        def idx_map(idxs, list):
            ld = dict(enumerate(list))
            return [ld[i] for i in idxs]

        # Assist with changing the order of the elements around for the
        # INSERT statement below.
        def flip_els(rows):
            return list(map(lambda x: x[2:] + x[:2], rows))

        # Record whether or not we're on the first statement of the function
        # below.
        first_table_id = True

        # Iterate through table_ids
        total_tables = len(self.line_numbers_dict)
        for table_index, (table_id, line_numbers) in enumerate(
            self.line_numbers_dict.items(), start=1
        ):
            columns = self.data_identifiers[table_id]
            rows = []

            # Iterate through files
            files_for_table = self.files[table_id]
            total_files = len(files_for_table)
            for file_index, file in enumerate(files_for_table, start=1):
                # Read from each CSV file
                with open(file, "rt") as f:
                    csv_rows = csv.reader(f)

                    for csv_row in csv_rows:
                        # Get elements at self.positions[table_id] for each row
                        rows.append(idx_map(self.positions[table_id], csv_row))
                if file_index == 1 or file_index % 20 == 0 or file_index == total_files:
                    self._progress(
                        f"Reading {table_id} sequence files",
                        current=file_index,
                        total=total_files,
                    )

            if first_table_id:
                question_mark_substr = self.dbapi_qm_substr(len(columns))
                # Insert rows into table
                self.c.executemany(
                    "INSERT INTO %s(%s) VALUES (%s)"
                    % (this_table_name, ", ".join(columns), question_mark_substr),
                    rows,
                )

                first_table_id = False
            else:
                set_clause = list(map(lambda x: x + " = ?", self.data_identifiers[table_id][2:]))
                self.c.executemany(
                    """UPDATE %s SET %s
                    WHERE STATE = ? AND LOGRECNO = ?"""
                    % (this_table_name, ", ".join(set_clause)),
                    flip_els(rows),
                )

            # Print the count for debug purposes. Should be around ~200,000
            for debug in self.c.execute("SELECT COUNT(*) FROM data"):
                display_data_identifier = table_id
                logger.info(
                    "Processing for %s complete (%s rows).",
                    display_data_identifier,
                    debug[0],
                )
            self._progress(
                f"Loaded ACS table {table_id}",
                current=table_index,
                total=total_tables,
            )
        # Debug output
        self.debug_output_table(this_table_name)

    ###########################################################################
    # __init__

    def __init__(self, path, progress_callback=None):
        """Create the database"""
        # Initialize ##########################################################

        self._progress_callback = progress_callback
        self.data_dir = Path(path).expanduser().resolve()
        self._progress(f"Build start: {self.data_dir}")
        self.year = self.detect_latest_acs_year(self.data_dir)
        self.acs_layout = self.detect_acs_layout(self.data_dir, self.year)
        self.gh_year = self.detect_latest_gazetteer_year(self.data_dir)
        self._progress(
            f"Detected ACS year {self.year} ({self.acs_layout}); " f"gazetteer year {self.gh_year}"
        )
        self.overlay_metric_meta = self._load_overlay_manifest(self.data_dir)
        manifest_stats = getattr(self, "overlay_manifest_stats", {})
        if manifest_stats.get("path"):
            self._progress(
                f"Loaded overlay manifest: {manifest_stats.get('path')} "
                f"({manifest_stats.get('metrics', 0)} metrics)"
            )
        self.overlays = self._load_overlays(self.data_dir)
        overlay_stats = getattr(self, "overlay_load_stats", {})
        loaded_files = overlay_stats.get("files_loaded", [])
        failed_files = overlay_stats.get("files_failed", [])
        if loaded_files:
            loaded_desc = ", ".join(
                f"{item['name']} ({item['geoids']} geoids)" for item in loaded_files
            )
            self._progress(
                f"Loaded overlays: {loaded_desc}; merged geoids={overlay_stats.get('geoids_loaded', 0)}"
            )
        else:
            self._progress("No overlay files found (optional).")
        if failed_files:
            self._progress(f"Overlay files failed to load: {', '.join(failed_files)}")

        self.st = StateLookup()

        # Connect to SQLite3
        self.conn = sqlite3.connect(":memory:")
        self.c = self.conn.cursor()

        # table_metadata ######################################################
        this_table_name = "table_metadata"
        if self.acs_layout == "sequence":
            self._progress("Loading table metadata")

            # Process column definitions
            columns = self.get_tm_columns(self.data_dir)
            column_defs = list(map(lambda x: x + " TEXT", columns))
            column_defs.insert(0, "id INTEGER PRIMARY KEY")

            # Get rows from CSV
            this_path = self.data_dir / "ACS_5yr_Seq_Table_Number_Lookup.txt"
            rows = []

            with open(this_path, "rt") as f:
                rows = list(csv.reader(f))

            # Create table
            self.create_table(this_table_name, columns, column_defs, rows)

            # Debug output
            self.debug_output_table(this_table_name)

        # geographies #########################################################
        this_table_name = "geographies"
        self._progress("Loading geographies")

        # Process column definitions
        columns = [
            "STUSAB",
            "SUMLEVEL",
            "LOGRECNO",
            "STATE",
            "GEOID",
            "NAME",
        ]
        self.geographies_columns = columns
        column_defs = list(map(lambda x: x + " TEXT", columns))
        column_defs.insert(0, "id INTEGER PRIMARY KEY")

        # Get rows from CSV
        rows = self.get_geo_csv_rows()

        # Filter for summary levels
        # 010 = United States
        # 040 = State
        # 050 = State-County
        # 140 = Census tract
        # 160 = State-Place
        # 310 = Metro/Micro Area
        # 400 = Urban Area
        # 860 = ZCTA
        allowed_sumlevels = {"010", "040", "050", "140", "160", "310", "400", "860"}

        def _keep_geography_row(row):
            sumlevel = row[1]
            geoid = row[3]
            if sumlevel not in allowed_sumlevels or len(geoid) < 5:
                return False

            # Table-based ACS uses non-"00" GEOID middle codes for some
            # summary levels (notably ZCTA = 860Z200USxxxxx).
            if self.acs_layout == "table":
                return "US" in geoid

            return geoid[3:5] == "00"

        rows = [row for row in rows if _keep_geography_row(row)]
        rows = [
            [
                row[0],  # STUSAB [lowercase]
                row[1],  # SUMLEVEL
                row[2],  # LOGRECNO
                self.st.get_state(row[4]),  # STATE
                row[3],  # GEOID
                row[4],  # NAME
            ]
            for row in rows
        ]

        # Create table
        self.create_table(this_table_name, columns, column_defs, rows)
        self._progress(f"Loaded geographies rows: {len(rows):,}")

        # Debug output
        self.debug_output_table(this_table_name)

        # geoheaders ##########################################################
        this_table_name = "geoheaders"
        self._progress("Loading geoheaders")

        # The primary reason we are interested in the 2019 National Gazetteer
        # is that we need to get the land area so that we can calculate
        # population and housing unit densities.

        columns = self.get_gh_columns(self.gh_year, self.data_dir)
        columns[-1] = columns[-1].strip()
        self.geoheaders_columns = columns
        column_defs = list(map(lambda x: x + " TEXT", columns))
        column_defs.insert(0, "id INTEGER PRIMARY KEY")

        # Get rows for places (160) from CSV
        this_path = self.data_dir / f"{self.gh_year}_Gaz_place_national.txt"
        rows = self._read_gaz_rows(this_path)

        # Get rows for counties (050) from CSV
        this_path = self.data_dir / f"{self.gh_year}_Gaz_counties_national.txt"
        c_rows = self._read_gaz_rows(this_path)

        # Get rows for tracts (140) from CSV
        this_path = self.data_dir / f"{self.gh_year}_Gaz_tracts_national.txt"
        t_rows = self.normalize_tract_gazetteer_rows(self._read_gaz_rows(this_path))

        # County geoheaders lack two columns that places have, so insert
        # them as empty strings.
        for c_row in c_rows:
            if len(c_row) >= 11:
                # USPS,GEOID,GEOIDFQ,ANSICODE,NAME,ALAND,... -> add LSAD,FUNCSTAT
                c_row.insert(5, "")
                c_row.insert(6, "")

        # Get rows for states (040) from CSV
        this_path = self.get_state_gazetteer_path(self.gh_year, self.data_dir)
        s_rows = self._read_gaz_rows(this_path)

        # Get rows for Metro/micro areas (310) from CSV
        this_path = self.data_dir / f"{self.gh_year}_Gaz_cbsa_national.txt"
        cbsa_rows = self._read_gaz_rows(this_path)

        # Get rows for urban areas (400) from CSV
        this_path = self.data_dir / f"{self.gh_year}_Gaz_ua_national.txt"
        ua_rows = self._read_gaz_rows(this_path)

        # Normalize state rows to match place schema:
        # USPS,GEOID,GEOIDFQ,NAME,ALAND,... -> insert ANSICODE, LSAD, FUNCSTAT
        for s_row in s_rows:
            if len(s_row) >= 10:
                s_row.insert(3, "")
                s_row.insert(5, "")
                s_row.insert(6, "")

        # Normalize urban area rows to match place schema:
        # GEOID,GEOIDFQ,NAME,ALAND,... -> add USPS, ANSICODE, LSAD, FUNCSTAT
        for ua_row in ua_rows:
            if len(ua_row) >= 9:
                normalized = [
                    "US",
                    ua_row[0],
                    ua_row[1],
                    "",
                    ua_row[2],
                    "",
                    "",
                    ua_row[3],
                    ua_row[4],
                    ua_row[5],
                    ua_row[6],
                    ua_row[7],
                    ua_row[8],
                ]
                ua_row[:] = normalized

        # Get rows for ZCTAs (860) from CSV
        this_path = self.data_dir / f"{self.gh_year}_Gaz_zcta_national.txt"
        z_rows = self._read_gaz_rows(this_path)

        # Normalize ZCTA rows to match place schema:
        # GEOID,GEOIDFQ,ALAND,... -> add USPS, ANSICODE, NAME, LSAD, FUNCSTAT
        for z_row in z_rows:
            if len(z_row) >= 8:
                geoid = z_row[0]
                normalized = [
                    "US",
                    geoid,
                    z_row[1],
                    "",
                    f"ZCTA5 {geoid}" if geoid != "GEOID" else "NAME",
                    "",
                    "",
                    z_row[2],
                    z_row[3],
                    z_row[4],
                    z_row[5],
                    z_row[6],
                    z_row[7],
                ]
                z_row[:] = normalized

        # Normalize metro/micro rows to match place schema:
        # CSAFP,GEOID,GEOIDFQ,NAME,CBSA_TYPE,ALAND,... -> keep only shared shape.
        for cbsa_row in cbsa_rows:
            if len(cbsa_row) >= 11:
                normalized = [
                    "US",
                    cbsa_row[1],
                    cbsa_row[2],
                    "",
                    cbsa_row[3],
                    "",
                    "",
                    cbsa_row[5],
                    cbsa_row[6],
                    cbsa_row[7],
                    cbsa_row[8],
                    cbsa_row[9],
                    cbsa_row[10],
                ]
                cbsa_row[:] = normalized

        def complete_geoids(sumlev_code, rows):
            for row in rows:
                if len(row) > 2 and "US" in row[2]:
                    row[1] = row[2]
                else:
                    row[1] = sumlev_code + "00US" + row[1]

        # Complete GEOIDs

        complete_geoids("160", rows)
        complete_geoids("040", s_rows)
        complete_geoids("050", c_rows)
        complete_geoids("140", t_rows)
        complete_geoids("310", cbsa_rows)
        complete_geoids("400", ua_rows)
        complete_geoids("860", z_rows)

        # Add a national geoheader row (010) so the ACS national geography row
        # can join through to geocompare_data.
        state_rows_for_agg = [
            row for row in s_rows if len(row) >= 13 and row[4] not in {"NAME", "United States"}
        ]
        if state_rows_for_agg:
            total_aland = 0.0
            total_awater = 0.0
            total_aland_sqmi = 0.0
            total_awater_sqmi = 0.0
            lat_weighted_sum = 0.0
            lon_weighted_sum = 0.0

            for row in state_rows_for_agg:
                try:
                    aland = float(row[7])
                    awater = float(row[8])
                    aland_sqmi = float(row[9])
                    awater_sqmi = float(row[10])
                    lat = float(row[11])
                    lon = float(row[12])
                except (TypeError, ValueError):
                    continue

                total_aland += aland
                total_awater += awater
                total_aland_sqmi += aland_sqmi
                total_awater_sqmi += awater_sqmi
                lat_weighted_sum += lat * aland
                lon_weighted_sum += lon * aland

            us_lat = lat_weighted_sum / total_aland if total_aland else 0.0
            us_lon = lon_weighted_sum / total_aland if total_aland else 0.0
            us_geoid = "0100000US"
            us_row = [
                "US",
                us_geoid,
                us_geoid,
                "",
                "United States",
                "",
                "",
                f"{total_aland:.0f}",
                f"{total_awater:.0f}",
                f"{total_aland_sqmi:.1f}",
                f"{total_awater_sqmi:.1f}",
                f"{us_lat:.6f}",
                f"{us_lon:.6f}",
            ]
            rows.append(us_row)

        # Merge rows together
        rows = rows + c_rows + t_rows + s_rows + z_rows + ua_rows + cbsa_rows

        for row in rows:
            row[-1] = row[-1].strip()

        # Create table
        self.create_table(this_table_name, columns, column_defs, rows)
        self._progress(f"Loaded geoheaders rows: {len(rows):,}")

        # Debug output
        self.debug_output_table(this_table_name)

        # Specify what data we need ###########################################

        # Specify table_ids and line numbers that have the data we need.
        # These table line references have remained stable across recent ACS releases.
        self.line_numbers_dict = self.LINE_NUMBERS_DICT
        logger.info("Processing data table. This might take a while.")
        if self.acs_layout == "table":
            self._progress("Loading ACS table-based files")
            self._load_table_based_data()
        else:
            self._progress("Loading ACS estimate sequences")
            self._load_sequence_based_data()

        # geocompare_data ######################################################
        this_table_name = "geocompare_data"
        self._progress("Merging geographies, geoheaders, and ACS data")

        # Combine data from places, geoheaders, and data into a single table.

        # Combine columns
        columns = self.geographies_columns + self.geoheaders_columns + self.data_columns

        # Unambiguous columns
        ub_geographies_columns = list(map(lambda x: "geographies." + x, self.geographies_columns))
        ub_geoheaders_columns = list(map(lambda x: "geoheaders." + x, self.geoheaders_columns))
        ub_data_columns = list(map(lambda x: "data." + x, self.data_columns))
        ub_columns = ub_geographies_columns + ub_geoheaders_columns + ub_data_columns

        # Make columns names unambigious
        def deambigify(column):
            if column in self.geographies_columns:
                return "geographies." + column
            elif column in self.geoheaders_columns:
                return "geoheaders." + column
            elif column in self.data_columns:
                return "data." + column

        # Remove duplicates
        columns = list(dict.fromkeys(columns))
        self.columns = columns
        ub_columns = list(map(deambigify, columns))

        # Column definitions
        column_defs = list(map(lambda x: x + " TEXT", columns))
        column_defs.insert(0, "id INTEGER PRIMARY KEY")

        # CREATE TABLE statement
        self.c.execute(
            """CREATE TABLE %s
                          (%s)"""
            % (this_table_name, ", ".join(column_defs))
        )

        # Insert rows into merged table
        self.c.execute(
            """INSERT INTO %s(%s)
        SELECT %s FROM geographies
        JOIN geoheaders ON geographies.GEOID = geoheaders.GEOID
        JOIN data ON geographies.LOGRECNO = data.LOGRECNO AND geographies.STUSAB = data.STATE"""
            % (this_table_name, ", ".join(columns), ", ".join(ub_columns))
        )

        # Debug output
        self.debug_output_list("columns")
        self.debug_output_table(this_table_name)

        # Database: Apply changes #############################################

        # Commit changes
        self.conn.commit()
        self._progress("Build complete")

        # Row factory
        self.conn.row_factory = sqlite3.Row
        self.c = self.conn.cursor()

        # DemographicProfiles #################################################

        # Create a placeholder for DemographicProfiles
        self.demographicprofiles = []

        for row in self.c.execute("SELECT * from geocompare_data"):
            try:
                self.demographicprofiles.append(DemographicProfile(row))
            except AttributeError as e:
                logger.warning("AttributeError while creating DemographicProfile: %s", e)
                logger.debug("Bad row: %s", tuple(row))
        self._progress(f"Created demographic profiles: {len(self.demographicprofiles):,}")

        # Debug output
        self.debug_output_list("demographicprofiles")

        # Optional overlay enrichment (built-in + custom/project metrics).
        overlay_apply_stats = self.apply_overlays()
        if overlay_apply_stats.get("overlay_geoids", 0):
            self._progress(
                "Applied overlays: "
                f"geoids={overlay_apply_stats.get('overlay_geoids', 0):,}, "
                f"matched_geoids={overlay_apply_stats.get('matched_overlay_geoids', 0):,}, "
                f"matched_profiles={overlay_apply_stats.get('matched_profiles', 0):,}, "
                f"metrics_added={overlay_apply_stats.get('metrics_added', 0):,}, "
                f"unmatched_geoids={overlay_apply_stats.get('unmatched_overlay_geoids', 0):,}"
            )

        # Medians and standard deviations #####################################

        # Prepare a DataFrame into which we can insert rows.
        metric_columns = [
            "ALAND_SQMI",
            "B01003_1",
            "B19301_1",
            "B02001_2",
            "B02001_3",
            "B02001_5",
            "B03002_3",
            "B03002_12",
            "B04004_51",
            "B15003_1",
            "B15003_22",
            "B15003_23",
            "B15003_24",
            "B15003_25",
            "B19013_1",
            "B25018_1",
            "B25035_1",
            "B25058_1",
            "B25077_1",
        ]
        rows = []
        for row in self.c.execute("SELECT * from geocompare_data"):
            try:
                rows.append([parse_number(row[column]) for column in metric_columns])
            except AttributeError:
                logger.exception("AttributeError while preparing medians/std dev dataframe")

        df = pd.DataFrame(rows, columns=metric_columns)

        # Adjustments for better calculations of medians and
        # standard deviations, and better results for highest and lowest values

        # median_year_structure_built value of 0 were causing problems because
        # all values for available data are between 1939 and the present year.
        # Replace all 0 values with numpy.nan
        df = df.replace({"B25035_1": {0: np.nan}})

        # Print some debug information.
        logger.debug("DataFrames:\n%s", df.head())

        medians = df.median()
        logger.debug("Medians:\n%s", dict(medians))

        standard_deviations = df.std()
        logger.debug("Standard deviations:\n%s", dict(standard_deviations))

        # GeoVectors ##########################################################

        self.geovectors = []
        geovector_failures = 0
        geovector_failure_names = []

        for row in self.c.execute("SELECT * from geocompare_data"):
            try:
                # Construct a GeoVector and append it to self.geovectors.
                self.geovectors.append(GeoVector(row, dict(medians), dict(standard_deviations)))
            # If a TypeError is thrown because some data is unavailable, just
            # don't make that GeoVector and print a debugging message.
            except (TypeError, ValueError, AttributeError):
                geovector_failures += 1
                if len(geovector_failure_names) < 25:
                    geovector_failure_names.append(row["NAME"])

        if geovector_failures:
            sample_count = min(10, len(geovector_failure_names))
            logger.warning(
                "Skipped GeoVector creation for %s geographies due to inadequate data "
                "(sample %s): %s",
                f"{geovector_failures:,}",
                sample_count,
                "; ".join(geovector_failure_names[:sample_count]),
            )
            if len(geovector_failure_names) > sample_count:
                logger.info(
                    "Additional skipped GeoVector samples (%s): %s",
                    len(geovector_failure_names) - sample_count,
                    "; ".join(geovector_failure_names[sample_count:]),
                )
        self._progress(
            f"Created geovectors: {len(self.geovectors):,} " f"(skipped: {geovector_failures:,})"
        )

        self._humanize_tract_names()

        # Debug output
        self.debug_output_list("geovectors")

    def get_products(self):
        """Return a dictionary of products."""
        # Use list(set(...)) to remove duplicates
        return {
            "demographicprofiles": self.demographicprofiles,
            "geovectors": self.geovectors,
        }
