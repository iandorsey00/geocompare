import copy
import csv
import difflib
import heapq
import logging
import math
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
        self._us_dp_cache = None

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
        self._us_dp_cache = None

    def _weighted_mean(self, values, weights):
        total_weight = sum(weights)
        if total_weight <= 0:
            return float(sum(values)) / float(len(values)) if values else 0.0
        return sum(v * w for v, w in zip(values, weights)) / total_weight

    def _format_profile_component(self, key, value):
        if key == "land_area":
            return f"{value:,.1f} sqmi"
        if key in {
            "per_capita_income",
            "median_household_income",
            "median_value",
            "median_rent",
        }:
            if key == "median_household_income" and int(round(value)) == 250001:
                return "$250,000+"
            return "$" + f"{int(round(value)):,}"
        if key == "median_year_structure_built":
            return str(int(round(value)))
        if key in {"median_age", "average_household_size"}:
            return f"{value:,.1f}"
        if float(value).is_integer():
            return f"{int(round(value)):,}"
        return f"{value:,.3f}"

    def _recompute_compounds(self, dp):
        rc = dp.rc
        c = {}

        population = rc.get("population", 0) or 0
        land_area = rc.get("land_area", 0) or 0
        if land_area:
            c["population_density"] = population / land_area
        else:
            c["population_density"] = 0.0

        if population:
            for key in [
                "white_alone",
                "black_alone",
                "asian_alone",
                "other_race",
                "hispanic_or_latino",
                "white_alone_not_hispanic_or_latino",
                "italian_alone",
                "under_18",
                "population_18_to_64",
                "age_65_plus",
            ]:
                if key in rc:
                    c[key] = rc[key] / population * 100.0
            for key, value in rc.items():
                if key.endswith("_count"):
                    c[key] = value / population * 100000.0
        else:
            for key in [
                "white_alone",
                "black_alone",
                "asian_alone",
                "other_race",
                "hispanic_or_latino",
                "white_alone_not_hispanic_or_latino",
                "italian_alone",
                "under_18",
                "population_18_to_64",
                "age_65_plus",
            ]:
                if key in rc:
                    c[key] = 0.0

        pop_25 = rc.get("population_25_years_and_older", 0) or 0
        if pop_25 and population:
            c["population_25_years_and_older"] = pop_25 / population * 100.0
            if "bachelors_degree_or_higher" in rc:
                c["bachelors_degree_or_higher"] = rc["bachelors_degree_or_higher"] / pop_25 * 100.0
            if "graduate_degree_or_higher" in rc:
                c["graduate_degree_or_higher"] = rc["graduate_degree_or_higher"] / pop_25 * 100.0
        else:
            if "population_25_years_and_older" in rc:
                c["population_25_years_and_older"] = 0.0
            if "bachelors_degree_or_higher" in rc:
                c["bachelors_degree_or_higher"] = 0.0
            if "graduate_degree_or_higher" in rc:
                c["graduate_degree_or_higher"] = 0.0

        poverty_universe = rc.get("poverty_universe", 0) or 0
        if poverty_universe and "population_below_poverty_level" in rc:
            c["population_below_poverty_level"] = (
                rc["population_below_poverty_level"] / poverty_universe * 100.0
            )
        elif "population_below_poverty_level" in rc:
            c["population_below_poverty_level"] = 0.0

        labor_force = rc.get("labor_force", 0) or 0
        if labor_force and "unemployed_population" in rc:
            c["unemployed_population"] = rc["unemployed_population"] / labor_force * 100.0
        elif "unemployed_population" in rc:
            c["unemployed_population"] = 0.0

        occupied = rc.get("occupied_housing_units", 0) or 0
        if occupied and "homeowner_occupied_housing_units" in rc:
            c["homeowner_occupied_housing_units"] = (
                rc["homeowner_occupied_housing_units"] / occupied * 100.0
            )
        elif "homeowner_occupied_housing_units" in rc:
            c["homeowner_occupied_housing_units"] = 0.0

        registered = rc.get("registered_voters", 0) or 0
        if population and "registered_voters" in rc:
            c["registered_voters"] = rc["registered_voters"] / population * 100.0
        if registered:
            for key in ("democratic_voters", "republican_voters", "other_voters"):
                if key in rc:
                    c[key] = rc[key] / registered * 100.0

        dp.c = c
        fcd = {}
        for key, value in c.items():
            if key == "population_density":
                fcd[key] = f"{value:,.1f}/sqmi"
            elif key.endswith("_count"):
                fcd[key] = f"{value:,.1f}/100k"
            else:
                fcd[key] = f"{value:,.1f}%"
        dp.fcd = fcd

    def _build_united_states_profile(self):
        if self._us_dp_cache is not None:
            return self._us_dp_cache

        d = self.get_data_products()
        states = [dp for dp in d["demographicprofiles"] if dp.sumlevel == "040"]
        if not states:
            raise ValueError("No state-level profiles available to synthesize United States.")

        us_dp = copy.deepcopy(states[0])
        us_dp.name = "United States"
        us_dp.state = "US"
        us_dp.sumlevel = "040"
        us_dp.geoid = "04000US00"
        us_dp.counties = []
        us_dp.counties_display = []

        population_weights = [float(dp.rc.get("population", 0) or 0) for dp in states]
        household_weights = [float(dp.rc.get("households", 0) or 0) for dp in states]
        housing_weights = [float(dp.rc.get("occupied_housing_units", 0) or 0) for dp in states]

        weighted_by_population = {
            "median_age",
            "per_capita_income",
            "latitude",
            "longitude",
            "social_ai_score",
            "social_acs_score",
            "social_overlap_coverage_pct",
        }
        weighted_by_households = {"median_household_income", "average_household_size"}
        weighted_by_housing = {
            "median_year_structure_built",
            "median_rooms",
            "median_value",
            "median_rent",
        }

        keys = sorted({key for dp in states for key in dp.rc.keys()})
        aggregated = {}
        for key in keys:
            values = [float(dp.rc.get(key, 0) or 0) for dp in states]
            if key == "land_area":
                aggregated[key] = sum(values)
            elif key in weighted_by_population or key.endswith("_score") or key.endswith("_pct"):
                aggregated[key] = self._weighted_mean(values, population_weights)
            elif key in weighted_by_households:
                aggregated[key] = self._weighted_mean(values, household_weights)
            elif key in weighted_by_housing:
                aggregated[key] = self._weighted_mean(values, housing_weights)
            else:
                aggregated[key] = sum(values)

        if "population" in aggregated and "under_18" in aggregated and "age_65_plus" in aggregated:
            aggregated["population_18_to_64"] = (
                aggregated["population"] - aggregated["under_18"] - aggregated["age_65_plus"]
            )

        us_dp.rc = aggregated
        us_dp.fc = {
            key: self._format_profile_component(key, value) for key, value in us_dp.rc.items()
        }
        self._recompute_compounds(us_dp)

        self._us_dp_cache = us_dp
        return us_dp

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
        if dp is not None:
            return dp
        normalized = str(display_label or "").strip().lower()
        if normalized in {"united states", "united states of america", "us", "u.s."}:
            return self._build_united_states_profile()
        raise ValueError(f"No geography found for display label: {display_label}")

    def _fetch_profile_by_name(self, display_label):
        dp = self._dp_by_name.get(display_label)
        if dp is not None:
            return dp

        if self._repo_supports("get_demographic_profile"):
            try:
                dp = self.primary_repository.get_demographic_profile(display_label)
                if dp is not None:
                    return dp
            except RuntimeError:
                pass

        return self._lookup_dp(display_label)

    def _fetch_profile_by_geoid(self, geoid):
        if not geoid:
            raise ValueError("Missing geoid.")

        if self.d is not None:
            for dp in self.d.get("demographicprofiles", []):
                if getattr(dp, "geoid", None) == geoid:
                    return dp

        if self._repo_supports("get_demographic_profile_by_geoid"):
            try:
                dp = self.primary_repository.get_demographic_profile_by_geoid(geoid)
                if dp is not None:
                    return dp
            except RuntimeError:
                pass

        raise ValueError(f"No geography found for geoid: {geoid}")

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

    def _identifier_probe_profile(self):
        if self.d is not None:
            demographicprofiles = self.d.get("demographicprofiles", [])
            if demographicprofiles:
                return demographicprofiles[0]

        if self._repo_supports("get_any_demographic_profile"):
            try:
                dp = self.primary_repository.get_any_demographic_profile()
                if dp is not None:
                    return dp
            except RuntimeError:
                pass

        d = self.get_data_products()
        demographicprofiles = d.get("demographicprofiles", [])
        if not demographicprofiles:
            raise ValueError("No demographic profiles are available.")
        return demographicprofiles[0]

    def _repo_supports(self, method_name):
        return hasattr(getattr(self, "primary_repository", None), method_name)

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
        if getattr(self, "d", None) is not None:
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

        if getattr(self, "d", None) is not None:
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
        if self._repo_supports("get_demographic_profile"):
            try:
                dp = self.primary_repository.get_demographic_profile(display_label)
                if dp is not None:
                    return [dp]
            except RuntimeError:
                pass

        self.get_data_products()
        return [self._lookup_dp(display_label)]

    def extreme_values(
        self, data_identifier, context="", geofilter="", n=10, lowest=False, **kwargs
    ):
        """Get highest and lowest values."""
        fetch_one = self._identifier_probe_profile()

        resolved = self.resolve_data_identifier(data_identifier, fetch_one)
        key = resolved["key"]
        sort_by = resolved["store"]
        if n <= 0:
            if self.d is None:
                self.get_data_products()
            n = len(self.d["demographicprofiles"])

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
                return [self._fetch_profile_by_name(name) for name in names]
            except RuntimeError:
                pass

        d = self.get_data_products()
        dpi_instances = d["demographicprofiles"]

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

    def remoteness(
        self,
        data_identifier,
        threshold,
        target="below",
        context="tracts+",
        geofilter="",
        n=10,
        county_population_min=None,
        county_density_min=None,
        one_per_county=False,
        **kwargs,
    ):
        """Rank geographies by distance to the nearest geography across a threshold."""
        fetch_one = self._identifier_probe_profile()
        resolved = self.resolve_data_identifier(data_identifier, fetch_one)
        key = resolved["key"]
        store = resolved["store"]
        threshold_value = parse_number(threshold)
        target_key = str(target or "below").strip().lower()
        if target_key not in {"below", "above"}:
            raise ValueError("target must be either 'below' or 'above'.")

        if n <= 0:
            return []

        allowed_county_geoids = self._build_allowed_county_geoids(
            county_population_min=county_population_min,
            county_density_min=county_density_min,
        )

        if self._repo_supports("query_profile_metric_rows"):
            try:
                sql_params = self._build_sql_query_params(context, geofilter, fetch_one)
                rows = self.primary_repository.query_profile_metric_rows(
                    comp_column=f"{store}_{key}",
                    universe_sl=sql_params["universe_sl"],
                    group_sl=sql_params["group_sl"],
                    group=sql_params["group"],
                    county_geoid=sql_params["county_geoid"],
                    geofilter_conditions=sql_params["geofilter_conditions"],
                    include_counties_geoids=allowed_county_geoids is not None,
                )
                if rows:
                    results = self._remoteness_from_rows(
                        rows,
                        threshold_value,
                        target_key,
                        store,
                        key,
                        n,
                        allowed_county_geoids=allowed_county_geoids,
                    )
                    return self._limit_one_per_county(results, n) if one_per_county else results
            except RuntimeError:
                pass

        d = self.get_data_products()
        dpi_instances = d["demographicprofiles"]
        if not dpi_instances:
            return []

        filtered = self.context_filter(dpi_instances, context, geofilter)
        if allowed_county_geoids is not None:
            filtered = [
                dp for dp in filtered if any(county in allowed_county_geoids for county in dp.counties)
            ]
        filtered = [
            dp
            for dp in filtered
            if key in getattr(dp, store)
            and not numpy.isnan(getattr(dp, store)[key])
            and dp.rc.get("latitude") is not None
            and dp.rc.get("longitude") is not None
        ]
        if not filtered:
            raise ValueError("Sorry, no geographies match your criteria.")

        if target_key == "below":
            candidates = [dp for dp in filtered if getattr(dp, store)[key] >= threshold_value]
            qualifying = [dp for dp in filtered if getattr(dp, store)[key] < threshold_value]
        else:
            candidates = [dp for dp in filtered if getattr(dp, store)[key] <= threshold_value]
            qualifying = [dp for dp in filtered if getattr(dp, store)[key] > threshold_value]

        if not candidates:
            raise ValueError("Sorry, no candidate geographies remain on the opposite side of the threshold.")
        if not qualifying:
            raise ValueError("Sorry, no qualifying geographies remain for the requested threshold.")

        results = []
        for candidate in candidates:
            candidate_lat = candidate.rc["latitude"]
            candidate_lon = candidate.rc["longitude"]
            nearest = None
            nearest_distance = None

            for match in qualifying:
                if match.name == candidate.name:
                    continue
                distance = self._haversine_miles(
                    candidate_lat,
                    candidate_lon,
                    match.rc["latitude"],
                    match.rc["longitude"],
                )
                if nearest_distance is None or distance < nearest_distance:
                    nearest = match
                    nearest_distance = distance

            if nearest is None or nearest_distance is None:
                continue

            results.append(
                {
                    "candidate": candidate,
                    "candidate_value": getattr(candidate, store)[key],
                    "nearest_match": nearest,
                    "nearest_match_value": getattr(nearest, store)[key],
                    "distance_miles": nearest_distance,
                }
            )

        results.sort(key=lambda row: row["distance_miles"], reverse=True)
        results = results[:n] if not one_per_county else self._limit_one_per_county(results, n)
        return results

    def local_average(
        self,
        data_identifier,
        context="tracts+",
        geofilter="",
        n=10,
        neighbors=20,
        county_population_min=None,
        county_density_min=None,
        one_per_county=False,
        **kwargs,
    ):
        """Rank geographies by a distance-weighted local average of nearby geographies."""
        fetch_one = self._identifier_probe_profile()
        resolved = self.resolve_data_identifier(data_identifier, fetch_one)
        key = resolved["key"]
        store = resolved["store"]

        if n <= 0:
            return []
        if neighbors <= 0:
            raise ValueError("neighbors must be greater than 0.")

        allowed_county_geoids = self._build_allowed_county_geoids(
            county_population_min=county_population_min,
            county_density_min=county_density_min,
        )

        if self._repo_supports("query_profile_metric_rows"):
            try:
                sql_params = self._build_sql_query_params(context, geofilter, fetch_one)
                rows = self.primary_repository.query_profile_metric_rows(
                    comp_column=f"{store}_{key}",
                    universe_sl=sql_params["universe_sl"],
                    group_sl=sql_params["group_sl"],
                    group=sql_params["group"],
                    county_geoid=sql_params["county_geoid"],
                    geofilter_conditions=sql_params["geofilter_conditions"],
                    include_counties_geoids=allowed_county_geoids is not None,
                )
                if rows:
                    return self._local_average_from_rows(
                        rows,
                        store,
                        key,
                        n,
                        neighbors,
                        allowed_county_geoids=allowed_county_geoids,
                        one_per_county=one_per_county,
                    )
            except RuntimeError:
                pass

        d = self.get_data_products()
        dpi_instances = d["demographicprofiles"]
        if not dpi_instances:
            return []

        filtered = self.context_filter(dpi_instances, context, geofilter)
        if allowed_county_geoids is not None:
            filtered = [
                dp for dp in filtered if any(county in allowed_county_geoids for county in dp.counties)
            ]
        filtered = [
            dp
            for dp in filtered
            if key in getattr(dp, store)
            and not numpy.isnan(getattr(dp, store)[key])
            and dp.rc.get("latitude") is not None
            and dp.rc.get("longitude") is not None
        ]
        if not filtered:
            raise ValueError("Sorry, no geographies match your criteria.")

        entries = []
        for dp in filtered:
            entries.append(
                {
                    "name": dp.name,
                    "latitude": float(dp.rc["latitude"]),
                    "longitude": float(dp.rc["longitude"]),
                    "population": 0 if dp.rc.get("population") is None else float(dp.rc["population"]),
                    "metric_value": float(getattr(dp, store)[key]),
                    "counties": list(dp.counties),
                }
            )

        results = self._local_average_from_entries(entries, store, key, n, neighbors)
        return self._limit_one_per_county(results, n) if one_per_county else results[:n]

    def _build_allowed_county_geoids(self, county_population_min=None, county_density_min=None):
        if county_population_min is None and county_density_min is None:
            return None

        allowed_by_population = None
        if county_population_min is not None:
            allowed_by_population = self._county_geoids_meeting_threshold(
                comp_column="rc_population",
                min_value=float(county_population_min),
            )

        allowed_by_density = None
        if county_density_min is not None:
            allowed_by_density = self._county_geoids_meeting_threshold(
                comp_column="c_population_density",
                min_value=float(county_density_min),
            )

        allowed = None
        for candidate_set in (allowed_by_population, allowed_by_density):
            if candidate_set is None:
                continue
            allowed = set(candidate_set) if allowed is None else allowed.intersection(candidate_set)

        return allowed if allowed is not None else None

    def _county_geoids_meeting_threshold(self, comp_column, min_value):
        if self._repo_supports("query_profile_metric_rows"):
            try:
                rows = self.primary_repository.query_profile_metric_rows(
                    comp_column=comp_column,
                    universe_sl="050",
                )
                return {
                    self.ct.county_name_to_geoid[name]
                    for name, _latitude, _longitude, _population, metric_value in rows
                    if name in self.ct.county_name_to_geoid and metric_value is not None and metric_value >= min_value
                }
            except RuntimeError:
                pass

        d = self.get_data_products()
        allowed = set()
        for dp in d.get("demographicprofiles", []):
            if dp.sumlevel != "050":
                continue
            metric_value = dp.rc.get("population") if comp_column == "rc_population" else dp.c.get("population_density")
            if metric_value is None or metric_value < min_value:
                continue
            county_geoid = self.ct.county_name_to_geoid.get(dp.name)
            if county_geoid:
                allowed.add(county_geoid)
        return allowed

    def _limit_one_per_county(self, results, n):
        if not results:
            return []

        distinct_results = []
        seen_counties = set()
        for row in results:
            candidate = row["candidate"]
            county_geoids = tuple(getattr(candidate, "counties", []) or [])
            county_key = county_geoids[0] if county_geoids else candidate.name
            if county_key in seen_counties:
                continue
            seen_counties.add(county_key)
            distinct_results.append(row)
            if len(distinct_results) >= n:
                break
        return distinct_results

    def _local_average_from_rows(
        self,
        rows,
        store,
        key,
        n,
        neighbors,
        allowed_county_geoids=None,
        one_per_county=False,
    ):
        entries = []
        for row in rows:
            if len(row) >= 6:
                name, latitude, longitude, population, metric_value, counties_geoids = row[:6]
            else:
                name, latitude, longitude, population, metric_value = row
                counties_geoids = ""
            if latitude is None or longitude is None or metric_value is None:
                continue
            county_geoids = self._parse_counties_geoids(counties_geoids)
            if allowed_county_geoids is not None and not any(
                county in allowed_county_geoids for county in county_geoids
            ):
                continue
            entries.append(
                {
                    "name": name,
                    "latitude": float(latitude),
                    "longitude": float(longitude),
                    "population": 0 if population is None else float(population),
                    "metric_value": float(metric_value),
                    "counties": county_geoids,
                }
            )

        if not entries:
            raise ValueError("Sorry, no geographies match your criteria.")

        results = self._local_average_from_entries(entries, store, key, n, neighbors)
        return self._limit_one_per_county(results, n) if one_per_county else results[:n]

    def _local_average_from_entries(self, entries, store, key, n, neighbors):
        if len(entries) <= 1:
            raise ValueError("Sorry, no neighboring geographies remain for local averaging.")

        grid = self._build_spatial_grid(entries)
        results = []
        dp_cache = {}

        for candidate in entries:
            nearest_entries = self._nearest_entries(candidate, grid, neighbors)
            if not nearest_entries:
                continue

            local_average = self._distance_weighted_average(nearest_entries)
            neighbor_span_miles = nearest_entries[-1][1]

            candidate_dp = dp_cache.get(candidate["name"])
            if candidate_dp is None:
                candidate_dp = self._fetch_profile_by_name(candidate["name"])
                dp_cache[candidate["name"]] = candidate_dp

            results.append(
                {
                    "candidate": candidate_dp,
                    "candidate_value": getattr(candidate_dp, store)[key],
                    "local_average": local_average,
                    "neighbor_span_miles": neighbor_span_miles,
                }
            )

        if not results:
            raise ValueError("Sorry, no neighboring geographies remain for local averaging.")

        results.sort(key=lambda row: row["local_average"], reverse=True)
        return results

    def _distance_weighted_average(self, nearest_entries):
        weighted_total = 0.0
        total_weight = 0.0
        for entry, distance_miles in nearest_entries:
            population = max(entry["population"], 1.0)
            weight = population / max(distance_miles, 0.1)
            weighted_total += entry["metric_value"] * weight
            total_weight += weight
        return weighted_total / total_weight if total_weight else 0.0

    def _parse_counties_geoids(self, counties_geoids):
        raw = str(counties_geoids or "").strip()
        if not raw:
            return []
        return [part for part in raw.strip("|").split("|") if part]

    def _remoteness_from_rows(
        self,
        rows,
        threshold_value,
        target_key,
        store,
        key,
        n,
        allowed_county_geoids=None,
    ):
        entries = []
        for row in rows:
            if len(row) >= 6:
                name, latitude, longitude, population, metric_value, counties_geoids = row[:6]
            else:
                name, latitude, longitude, population, metric_value = row
                counties_geoids = ""
            if latitude is None or longitude is None or metric_value is None:
                continue
            county_geoids = self._parse_counties_geoids(counties_geoids)
            if allowed_county_geoids is not None and not any(
                county in allowed_county_geoids for county in county_geoids
            ):
                continue
            entry = {
                "name": name,
                "latitude": float(latitude),
                "longitude": float(longitude),
                "population": 0 if population is None else float(population),
                "metric_value": float(metric_value),
                "counties": county_geoids,
            }
            entries.append(entry)

        if not entries:
            raise ValueError("Sorry, no geographies match your criteria.")

        if target_key == "below":
            candidates = [entry for entry in entries if entry["metric_value"] >= threshold_value]
            qualifying = [entry for entry in entries if entry["metric_value"] < threshold_value]
        else:
            candidates = [entry for entry in entries if entry["metric_value"] <= threshold_value]
            qualifying = [entry for entry in entries if entry["metric_value"] > threshold_value]

        if not candidates:
            raise ValueError("Sorry, no candidate geographies remain on the opposite side of the threshold.")
        if not qualifying:
            raise ValueError("Sorry, no qualifying geographies remain for the requested threshold.")

        qualifying_names = {entry["name"] for entry in qualifying}
        grid = self._build_spatial_grid(qualifying)
        results = []
        dp_cache = {}

        for candidate in candidates:
            nearest, nearest_distance = self._nearest_qualifying_entry(candidate, grid, qualifying_names)
            if nearest is None or nearest_distance is None:
                continue

            candidate_dp = dp_cache.get(candidate["name"])
            if candidate_dp is None:
                candidate_dp = self._fetch_profile_by_name(candidate["name"])
                dp_cache[candidate["name"]] = candidate_dp
            nearest_dp = dp_cache.get(nearest["name"])
            if nearest_dp is None:
                nearest_dp = self._fetch_profile_by_name(nearest["name"])
                dp_cache[nearest["name"]] = nearest_dp

            results.append(
                {
                    "candidate": candidate_dp,
                    "candidate_value": getattr(candidate_dp, store)[key],
                    "nearest_match": nearest_dp,
                    "nearest_match_value": getattr(nearest_dp, store)[key],
                    "distance_miles": nearest_distance,
                }
            )

        results.sort(key=lambda row: row["distance_miles"], reverse=True)
        return results[:n]

    def _build_spatial_grid(self, entries, cell_degrees=0.5):
        grid = {"cell_degrees": cell_degrees, "buckets": {}}
        for entry in entries:
            key = (
                math.floor(entry["latitude"] / cell_degrees),
                math.floor(entry["longitude"] / cell_degrees),
            )
            grid["buckets"].setdefault(key, []).append(entry)
        return grid

    def _nearest_entries(self, candidate, grid, neighbors):
        cell_degrees = grid["cell_degrees"]
        buckets = grid["buckets"]
        lat = candidate["latitude"]
        lon = candidate["longitude"]
        cell = (
            math.floor(lat / cell_degrees),
            math.floor(lon / cell_degrees),
        )

        collected = []
        radius = 0
        seen_names = set()

        while radius <= 720:
            for dlat in range(-radius, radius + 1):
                for dlon in range(-radius, radius + 1):
                    if radius and max(abs(dlat), abs(dlon)) != radius:
                        continue
                    for entry in buckets.get((cell[0] + dlat, cell[1] + dlon), []):
                        if entry["name"] == candidate["name"] or entry["name"] in seen_names:
                            continue
                        distance = self._haversine_miles(
                            lat,
                            lon,
                            entry["latitude"],
                            entry["longitude"],
                        )
                        collected.append((entry, distance))
                        seen_names.add(entry["name"])

            if len(collected) >= neighbors:
                collected.sort(key=lambda item: item[1])
                worst_distance = collected[neighbors - 1][1]
                lower_bound = self._grid_outer_ring_lower_bound(lat, lon, cell, radius + 1, cell_degrees)
                if worst_distance <= lower_bound:
                    return collected[:neighbors]

            radius += 1

        collected.sort(key=lambda item: item[1])
        return collected[:neighbors]

    def _nearest_qualifying_entry(self, candidate, grid, qualifying_names):
        cell_degrees = grid["cell_degrees"]
        buckets = grid["buckets"]
        lat = candidate["latitude"]
        lon = candidate["longitude"]
        cell = (
            math.floor(lat / cell_degrees),
            math.floor(lon / cell_degrees),
        )

        best_entry = None
        best_distance = None
        radius = 0

        while radius <= 720:
            for dlat in range(-radius, radius + 1):
                for dlon in range(-radius, radius + 1):
                    if radius and max(abs(dlat), abs(dlon)) != radius:
                        continue
                    for entry in buckets.get((cell[0] + dlat, cell[1] + dlon), []):
                        if entry["name"] == candidate["name"] and entry["name"] in qualifying_names:
                            continue
                        distance = self._haversine_miles(
                            lat,
                            lon,
                            entry["latitude"],
                            entry["longitude"],
                        )
                        if best_distance is None or distance < best_distance:
                            best_entry = entry
                            best_distance = distance

            if best_distance is not None:
                lower_bound = self._grid_outer_ring_lower_bound(lat, lon, cell, radius, cell_degrees)
                if best_distance <= lower_bound:
                    return best_entry, best_distance

            radius += 1

        return best_entry, best_distance

    def _grid_outer_ring_lower_bound(self, latitude, longitude, cell, radius, cell_degrees):
        lower_lat = (cell[0] - radius) * cell_degrees
        upper_lat = (cell[0] + radius + 1) * cell_degrees
        lower_lon = (cell[1] - radius) * cell_degrees
        upper_lon = (cell[1] + radius + 1) * cell_degrees

        lat_gap = min(abs(latitude - lower_lat), abs(upper_lat - latitude))
        lon_gap = min(abs(longitude - lower_lon), abs(upper_lon - longitude))

        lat_miles = lat_gap * 69.0
        lon_miles = lon_gap * 69.0 * max(0.2, cos(radians(latitude)))
        return min(lat_miles, lon_miles)

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
        target_geo = self._fetch_profile_by_name(display_label)

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
                    dp_distances.append((self._fetch_profile_by_name(name), distance))

                return heapq.nsmallest(n, dp_distances, key=lambda x: x[1])
            except RuntimeError:
                pass

        d = self.get_data_products()
        dpi_instances = d["demographicprofiles"]

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
