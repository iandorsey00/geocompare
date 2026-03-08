import csv
import difflib
import heapq
import logging
import operator
import re
import sys
import time
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path

import geopy.distance
import numpy
from rapidfuzz import fuzz

from geocompare.database.Database import Database
from geocompare.identity.place_identity import PlaceIdentityIndex
from geocompare.repository.sqlite_repository import SQLiteRepository
from geocompare.tools.county_key_index import CountyKeyIndex
from geocompare.tools.county_lookup import CountyLookup
from geocompare.tools.numeric import parse_number
from geocompare.tools.query_syntax import parse_geofilter
from geocompare.tools.state_lookup import StateLookup
from geocompare.tools.summary_level_parser import SummaryLevelParser


class Engine:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.ct = CountyLookup()
        self.st = StateLookup()
        self.kt = CountyKeyIndex()
        self.slt = SummaryLevelParser()

        self.PROJECT_ROOT = Path(__file__).resolve().parents[1]
        self.sqlite_path = self.PROJECT_ROOT / "bin" / "default.sqlite"

        self.sqlite_repository = SQLiteRepository(self.sqlite_path)
        self.primary_repository = self.sqlite_repository

        self.d = None
        self._dp_by_name = {}
        self._gv_by_name = {}
        self._data_identifier_index = {}
        self.identity_index = None

    def create_data_products(self, data_path):
        """Generate and save data products."""
        start_time = time.monotonic()
        interactive = sys.stderr.isatty()
        status_state = {"last_len": 0}

        def format_elapsed(seconds):
            total = int(seconds)
            h, rem = divmod(total, 3600)
            m, s = divmod(rem, 60)
            if h:
                return f"{h:02d}:{m:02d}:{s:02d}"
            return f"{m:02d}:{s:02d}"

        def render_status(message, finalize=False):
            elapsed = time.monotonic() - start_time
            text = f"[elapsed {format_elapsed(elapsed)}] {message}"
            if interactive:
                pad = max(0, status_state["last_len"] - len(text))
                ending = "\n" if finalize else ""
                print(f"\r{text}{' ' * pad}", end=ending, file=sys.stderr, flush=True)
                status_state["last_len"] = 0 if finalize else len(text)
                return
            print(text, file=sys.stderr, flush=True)

        def progress_with_elapsed(message):
            render_status(message, finalize=False)

        progress_with_elapsed(f"Starting data build from {data_path}")
        products = Database(
            data_path,
            progress_callback=progress_with_elapsed,
        ).get_products()

        # Write data products to SQLite.
        progress_with_elapsed("Writing products to SQLite")
        self.primary_repository.save_data_products(products)

        self._set_data_products(products)
        self.logger.info("Data product write completed.")
        total_elapsed = time.monotonic() - start_time
        render_status("Data product write completed.", finalize=True)
        print(
            f"Total build time: {format_elapsed(total_elapsed)}",
            file=sys.stderr,
            flush=True,
        )

    def load_data_products(self):
        """Load data products."""
        return self.primary_repository.load_data_products()

    def _set_data_products(self, data_products):
        """Set in-memory data products and query indexes."""
        self.d = data_products

        demographicprofiles = self.d.get("demographicprofiles", [])
        geovectors = self.d.get("geovectors", [])

        self._dp_by_name = {dp.name: dp for dp in demographicprofiles}
        self._gv_by_name = {gv.name: gv for gv in geovectors}
        self._data_identifier_index = self._build_data_identifier_index(demographicprofiles)
        self.identity_index = PlaceIdentityIndex.from_demographic_profiles(demographicprofiles)

    def _build_data_identifier_index(self, demographicprofiles):
        index = {}

        for dp in demographicprofiles:
            for key in dp.rc.keys():
                index.setdefault(
                    key,
                    {
                        "key": key,
                        "store": "rc",
                        "display_store": "fc",
                        "label": dp.rh.get(key, self._format_identifier_label(key)),
                    },
                )

            for key in dp.c.keys():
                if key in dp.rc:
                    identifier = f"{key}_pct"
                    label = dp.rh.get(key, self._format_identifier_label(key))
                    index.setdefault(
                        identifier,
                        {
                            "key": key,
                            "store": "c",
                            "display_store": "fcd",
                            "label": f"{label} (%)",
                        },
                    )
                else:
                    index.setdefault(
                        key,
                        {
                            "key": key,
                            "store": "c",
                            "display_store": "fcd",
                            "label": dp.rh.get(key, self._format_identifier_label(key)),
                        },
                    )

        return index

    def refresh_cache(self):
        """Reload data products and query indexes."""
        self._set_data_products(self.load_data_products())

    def get_data_products(self):
        if self.d is None:
            self.refresh_cache()
        return self.d

    def _lookup_dp(self, display_label):
        dp = self._dp_by_name.get(display_label)
        if dp is None:
            raise ValueError(f"No geography found for display label: {display_label}")
        return dp

    def resolve_geography(self, query, state=None, sumlevel=None, population=None, n=5, **kwargs):
        """Resolve an input geography string to likely canonical matches."""
        self.get_data_products()
        return self.identity_index.resolve(
            query,
            state=state,
            sumlevel=sumlevel,
            population=population,
            limit=n,
        )

    def _lookup_gv(self, display_label):
        gv = self._gv_by_name.get(display_label)
        if gv is None:
            raise ValueError(f"No geography found for display label: {display_label}")
        return gv

    def _repo_supports(self, method_name):
        return hasattr(self.primary_repository, method_name)

    def _get_county_geoid(self, state_abbrev):
        if re.match(r"^\d{5}:county$", state_abbrev):
            return state_abbrev.split(":", 1)[0]
        key = "us:" + state_abbrev + "/county"
        county_name = self.kt.key_to_county_name[key]
        return self.ct.county_name_to_geoid[county_name]

    def _build_sql_geofilter_conditions(self, geofilter, fetch_one):
        if not geofilter:
            return []

        conditions = []
        for criteria in parse_geofilter(geofilter):
            resolved = self.resolve_data_identifier(criteria["comp"], fetch_one)
            conditions.append(
                {
                    "column": f"{resolved['store']}_{resolved['key']}",
                    "operator": criteria["operator"],
                    "value": parse_number(criteria["value"]),
                }
            )

        return conditions

    def _build_sql_query_params(self, context, geofilter, fetch_one):
        universe_sl, group_sl, group = self.slt.unpack_context(context)
        county_geoid = None
        if group_sl == "050":
            county_geoid = self._get_county_geoid(group)

        return {
            "universe_sl": universe_sl,
            "group_sl": group_sl,
            "group": group,
            "county_geoid": county_geoid,
            "geofilter_conditions": self._build_sql_geofilter_conditions(geofilter, fetch_one),
        }

    def list_data_identifiers(self, fetch_one):
        if hasattr(self, "d"):
            self.get_data_products()
        if getattr(self, "_data_identifier_index", None):
            return sorted(self._data_identifier_index.keys())
        return []

    def _format_identifier_label(self, identifier):
        return identifier.replace("_", " ")

    def resolve_data_identifier(self, data_identifier, fetch_one):
        requested = str(data_identifier or "").strip().lower()
        if not requested:
            raise ValueError("Missing data identifier.")

        if hasattr(self, "d"):
            self.get_data_products()
        if requested in getattr(self, "_data_identifier_index", {}):
            return self._data_identifier_index[requested]

        if requested in fetch_one.rc:
            return {
                "requested": requested,
                "key": requested,
                "store": "rc",
                "display_store": "fc",
                "label": fetch_one.rh.get(requested, self._format_identifier_label(requested)),
            }

        if requested in fetch_one.c and requested not in fetch_one.rc:
            return {
                "requested": requested,
                "key": requested,
                "store": "c",
                "display_store": "fcd",
                "label": fetch_one.rh.get(requested, self._format_identifier_label(requested)),
            }

        if requested.endswith("_pct"):
            base_key = requested[:-4]
            if base_key in fetch_one.c:
                base_label = fetch_one.rh.get(base_key, self._format_identifier_label(base_key))
                return {
                    "requested": requested,
                    "key": base_key,
                    "store": "c",
                    "display_store": "fcd",
                    "label": f"{base_label} (%)",
                }

        choices = self.list_data_identifiers(fetch_one)
        suggestions = difflib.get_close_matches(requested, choices, n=5, cutoff=0.6)
        suggestion_suffix = ""
        if suggestions:
            suggestion_suffix = " Did you mean: " + ", ".join(suggestions) + "?"
        raise ValueError(f"Unknown data identifier: {data_identifier}.{suggestion_suffix}")

    def context_filter(self, input_instances, context, geofilter, gv=False):
        """Filters instances and leaves those that match the context."""
        if len(input_instances) == 0:
            return []

        universe_sl, group_sl, group = self.slt.unpack_context(context)
        instances = input_instances
        fetch_one = instances[0]

        # Filter by summary level
        if universe_sl:
            instances = list(filter(lambda x: x.sumlevel == universe_sl, instances))

        # Filter by group summary level
        if group_sl == "050":
            key = "us:" + group + "/county"
            county_name = self.kt.key_to_county_name[key]
            county_geoid = self.ct.county_name_to_geoid[county_name]

            instances = list(filter(lambda x: county_geoid in x.counties, instances))
        elif group_sl == "040":
            instances = list(filter(lambda x: x.state == group, instances))
        elif group_sl == "860":
            instances = list(filter(lambda x: x.name.startswith("ZCTA5 " + group), instances))

        # Filtering
        if geofilter:
            operators = {
                "gt": operator.gt,
                "gteq": operator.ge,
                "eq": operator.eq,
                "lteq": operator.le,
                "lt": operator.lt,
            }

            for filter_criterium in parse_geofilter(geofilter):
                resolved = self.resolve_data_identifier(filter_criterium["comp"], fetch_one)
                operator_key = filter_criterium["operator"]

                value = parse_number(filter_criterium["value"])

                # Now, filter by operator at index 1.
                compare = operators.get(operator_key)
                if compare is None:
                    raise ValueError("filter: Invalid operator")

                instances = list(
                    filter(
                        lambda x: compare(
                            getattr(x, resolved["store"])[resolved["key"]],
                            value,
                        ),
                        instances,
                    )
                )

        return instances

    def compare_geovectors(self, display_label, context="", n=10, mode="std", **kwargs):
        """Compare GeoVectors."""
        d = self.get_data_products()

        gv_list = d["geovectors"]

        # Obtain the GeoVector for which we entered a name.
        comparison_gv = self._lookup_gv(display_label)

        # If a context was specified, filter GeoVector instances
        if context:
            gv_list = self.context_filter(gv_list, context, False)
        else:
            gv_list = list(filter(lambda x: x.sumlevel == comparison_gv.sumlevel, gv_list))

        if n <= 0:
            return []

        # Get the closest GeoVectors.
        # In other words, get the most demographically similar places.
        return heapq.nsmallest(
            n,
            gv_list,
            key=lambda x: comparison_gv.distance(x, mode=mode),
        )

    def compare_geovectors_app(self, display_label, context="", n=10):
        return self.compare_geovectors(display_label, context=context, n=n, mode="app")

    def get_dp(self, display_label, **kwargs):
        """Get DemographicProfiles."""
        self.get_data_products()

        if self._repo_supports("get_demographic_profile"):
            try:
                dp = self.primary_repository.get_demographic_profile(display_label)
                if dp is not None:
                    return [dp]
            except RuntimeError:
                pass

        return [self._lookup_dp(display_label)]

    def extreme_values(self, data_identifier, context="", geofilter="", n=10, lowest=False, **kwargs):
        """Get highest and lowest values."""
        d = self.get_data_products()

        dpi_instances = d["demographicprofiles"]
        fetch_one = dpi_instances[0]

        resolved = self.resolve_data_identifier(data_identifier, fetch_one)
        key = resolved["key"]
        sort_by = resolved["store"]
        if n <= 0:
            n = len(dpi_instances)

        if self._repo_supports("query_extreme_profile_names"):
            sql_params = self._build_sql_query_params(context, geofilter, fetch_one)

            exclude_values = []
            if key == "median_year_structure_built" and sort_by == "rc":
                exclude_values = [0, 18]

            try:
                names = self.primary_repository.query_extreme_profile_names(
                    comp_column=f"{sort_by}_{key}",
                    universe_sl=sql_params["universe_sl"],
                    group_sl=sql_params["group_sl"],
                    group=sql_params["group"],
                    county_geoid=sql_params["county_geoid"],
                    geofilter_conditions=sql_params["geofilter_conditions"],
                    n=n,
                    lowest=lowest,
                    exclude_values=exclude_values,
                )
                return [self._lookup_dp(name) for name in names]
            except RuntimeError:
                pass

        # Remove numpy.nans because they interfere with sorted()
        dpi_instances = list(
            filter(lambda x: not numpy.isnan(getattr(x, sort_by)[key]), dpi_instances)
        )

        # Filter instances
        dpi_instances = self.context_filter(dpi_instances, context, geofilter)

        # For the median_year_structure_built component, remove values of zero and
        # 18...
        if key == "median_year_structure_built":
            dpi_instances = list(
                filter(lambda x: not x.rc["median_year_structure_built"] == 0, dpi_instances)
            )
            dpi_instances = list(
                filter(lambda x: not x.rc["median_year_structure_built"] == 18, dpi_instances)
            )

        # Sort our DemographicProfile instances by component or compound specified.
        if lowest:
            return heapq.nsmallest(n, dpi_instances, key=lambda x: getattr(x, sort_by)[key])
        return heapq.nlargest(n, dpi_instances, key=lambda x: getattr(x, sort_by)[key])

    def lowest_values(self, data_identifier, context="", geofilter="", n=10, **kwargs):
        """Wrapper function for lowest values."""
        return self.extreme_values(
            data_identifier,
            context=context,
            geofilter=geofilter,
            n=n,
            lowest=True,
        )

    def display_label_search(self, query, n=10, **kwargs):
        """Search display labels (place names)."""
        if n <= 0:
            return []

        if self._repo_supports("search_demographic_profiles"):
            try:
                return self.primary_repository.search_demographic_profiles(query, n)
            except RuntimeError:
                pass

        d = self.get_data_products()

        dpi_instances = d["demographicprofiles"]
        return heapq.nlargest(
            n,
            dpi_instances,
            key=lambda x: fuzz.token_set_ratio(query, x.name),
        )

    def rows(self, comps, context="", geofilter="", n=0, **kwargs):
        """Output data identifiers to CSV."""
        d = self.get_data_products()
        dpi_instances = d["demographicprofiles"]
        fetch_one = dpi_instances[0]

        # Categories: Groups of >= 1 data identifiers.
        categories = {
            ":geography": ["land_area"],
            ":population": ["population", "population_density"],
            ":race": [
                "white_alone",
                "white_alone_not_hispanic_or_latino",
                "black_alone",
                "asian_alone",
                "other_race",
                "hispanic_or_latino",
            ],
            ":education": [
                "population_25_years_and_older",
                "bachelors_degree_or_higher",
                "graduate_degree_or_higher",
            ],
            ":income": ["per_capita_income", "median_household_income"],
            ":housing": [
                "median_year_structure_built",
                "median_rooms",
                "median_value",
                "median_rent",
            ],
        }

        comps = comps.split(" ")
        comp_list = list()

        # Replace categories with identifiers and validate identifiers.
        for comp in comps:
            if comp in categories.keys():
                comp_list += categories[comp]
            elif comp:
                comp_list += [comp]

        resolved_identifiers = [self.resolve_data_identifier(comp, fetch_one) for comp in comp_list]

        # Filter instances
        if self._repo_supports("query_profile_names"):
            try:
                sql_params = self._build_sql_query_params(context, geofilter, fetch_one)
                names = self.primary_repository.query_profile_names(
                    universe_sl=sql_params["universe_sl"],
                    group_sl=sql_params["group_sl"],
                    group=sql_params["group"],
                    county_geoid=sql_params["county_geoid"],
                    geofilter_conditions=sql_params["geofilter_conditions"],
                )
                dpi_instances = [self._lookup_dp(name) for name in names]
            except RuntimeError:
                dpi_instances = self.context_filter(dpi_instances, context, geofilter)
        else:
            dpi_instances = self.context_filter(dpi_instances, context, geofilter)

        if len(dpi_instances) == 0:
            raise ValueError("Sorry, no geographies match your criteria.")

        # Keep export ordering deterministic regardless of source path.
        dpi_instances = sorted(dpi_instances, key=lambda x: x.name)
        if n and n > 0:
            dpi_instances = dpi_instances[:n]

        # Intialize csvwriter
        csvwriter = csv.writer(sys.stdout, quoting=csv.QUOTE_MINIMAL)

        # Header row
        this_row = ["Geography", "County"]

        for identifier in resolved_identifiers:
            this_row += [identifier["label"]]

        csvwriter.writerow(this_row)

        # All other rows
        for dpi_instance in dpi_instances:
            this_row = [dpi_instance.name, ", ".join(dpi_instance.counties_display)]

            for identifier in resolved_identifiers:
                key = identifier["key"]
                display_store = identifier["display_store"]
                this_row += [getattr(dpi_instance, display_store)[key]]

            csvwriter.writerow(this_row)

    def get_csv_dp(self, display_label, profile_view="full", **kwargs):
        """Output a DemographicProfile in CSV format"""
        self.get_data_products()
        dp = self._lookup_dp(display_label)
        dp.tocsv(view=profile_view)

    def get_distance(self, dp1, dp2, kilometers=False):
        """Distance between two sets of lat/long coords from DemographicProfiles"""

        coords1 = (dp1.rc["latitude"], dp1.rc["longitude"])
        coords2 = (dp2.rc["latitude"], dp2.rc["longitude"])

        if kilometers:
            distance = geopy.distance.distance(coords1, coords2).km
        else:
            distance = geopy.distance.distance(coords1, coords2).mi

        return distance

    def _haversine_miles(self, lat1, lon1, lat2, lon2):
        """Fast great-circle distance in miles."""
        r_miles = 3958.7613
        phi1 = radians(lat1)
        phi2 = radians(lat2)
        dphi = radians(lat2 - lat1)
        dlambda = radians(lon2 - lon1)

        a = sin(dphi / 2.0) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2.0) ** 2
        c = 2.0 * atan2(sqrt(a), sqrt(1.0 - a))
        return r_miles * c

    def distance(self, display_label_1, display_label_2, kilometers=False, **kwargs):
        """Get the distance between two geographies"""
        self.get_data_products()

        if self._repo_supports("get_coordinates"):
            try:
                coords1 = self.primary_repository.get_coordinates(display_label_1)
                coords2 = self.primary_repository.get_coordinates(display_label_2)

                if coords1 is not None and coords2 is not None:
                    if kilometers:
                        return geopy.distance.distance(coords1, coords2).km
                    return geopy.distance.distance(coords1, coords2).mi
            except RuntimeError:
                pass

        dp1 = self._lookup_dp(display_label_1)
        dp2 = self._lookup_dp(display_label_2)

        return self.get_distance(dp1, dp2, kilometers)

    def closest_geographies(self, display_label, context="", geofilter="", n=10, **kwargs):
        """Display the closest geographies"""
        d = self.get_data_products()

        target_geo = self._lookup_dp(display_label)
        dpi_instances = d["demographicprofiles"]
        # Remove numpy.nans because they interfere with sorted()
        # dpi_instances = list(filter(lambda x: not \
        #                numpy.isnan(getattr(x, sort_by)[comp]), dpi_instances))

        if n <= 0:
            return []

        if self._repo_supports("query_profile_coordinates"):
            try:
                sql_params = self._build_sql_query_params(context, geofilter, target_geo)
                target_coords = (target_geo.rc["latitude"], target_geo.rc["longitude"])
                target_lat, target_lon = target_coords

                # Progressively widen bounding box until we have enough candidates.
                expansion_degrees = [0.5, 1.0, 2.0, 5.0, 12.0, None]
                coords_rows = []
                for degrees in expansion_degrees:
                    kwargs = {}
                    if degrees is not None:
                        # Longitude span shrinks with latitude.
                        lon_scale = max(0.2, cos(radians(target_lat)))
                        lon_delta = degrees / lon_scale
                        kwargs = {
                            "min_latitude": target_lat - degrees,
                            "max_latitude": target_lat + degrees,
                            "min_longitude": target_lon - lon_delta,
                            "max_longitude": target_lon + lon_delta,
                        }

                    coords_rows = self.primary_repository.query_profile_coordinates(
                        universe_sl=sql_params["universe_sl"],
                        group_sl=sql_params["group_sl"],
                        group=sql_params["group"],
                        county_geoid=sql_params["county_geoid"],
                        geofilter_conditions=sql_params["geofilter_conditions"],
                        exclude_name=target_geo.name,
                        **kwargs,
                    )
                    if len(coords_rows) >= n:
                        break

                dp_distances = []
                for name, latitude, longitude in coords_rows:
                    if latitude is None or longitude is None:
                        continue
                    distance = self._haversine_miles(target_lat, target_lon, latitude, longitude)
                    dp_distances.append((self._lookup_dp(name), distance))

                return heapq.nsmallest(n, dp_distances, key=lambda x: x[1])
            except RuntimeError:
                pass

        # Filter instances
        dpi_instances = self.context_filter(dpi_instances, context, geofilter)

        # Get distances
        dp_distances = []
        for dp in dpi_instances:
            if dp.name != target_geo.name:
                distance = self._haversine_miles(
                    target_geo.rc["latitude"],
                    target_geo.rc["longitude"],
                    dp.rc["latitude"],
                    dp.rc["longitude"],
                )
                dp_distances.append((dp, distance))

        # Sort our DemographicProfile instances by component or compound specified.
        return heapq.nsmallest(n, dp_distances, key=lambda x: x[1])
