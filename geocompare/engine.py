import csv
import heapq
import logging
import operator
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
from geocompare.tools.CountyTools import CountyTools
from geocompare.tools.KeyTools import KeyTools
from geocompare.tools.numeric import parse_number
from geocompare.tools.StateTools import StateTools
from geocompare.tools.SummaryLevelTools import SummaryLevelTools


class Engine:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.ct = CountyTools()
        self.st = StateTools()
        self.kt = KeyTools()
        self.slt = SummaryLevelTools()

        self.PROJECT_ROOT = Path(__file__).resolve().parents[1]
        self.sqlite_path = self.PROJECT_ROOT / "bin" / "default.sqlite"

        self.sqlite_repository = SQLiteRepository(self.sqlite_path)
        self.primary_repository = self.sqlite_repository

        self.d = None
        self._dp_by_name = {}
        self._gv_by_name = {}
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
        self.identity_index = PlaceIdentityIndex.from_demographic_profiles(demographicprofiles)

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
        key = "us:" + state_abbrev + "/county"
        county_name = self.kt.key_to_county_name[key]
        return self.ct.county_name_to_geoid[county_name]

    def _build_sql_geofilter_conditions(self, geofilter, fetch_one):
        if not geofilter:
            return []

        conditions = []
        filter_criteria = map(lambda x: x.split(":"), geofilter.split("+"))
        for criteria in filter_criteria:
            if len(criteria) < 3:
                raise ValueError("filter: Invalid criteria")

            data_type = criteria[3] if len(criteria) == 4 else False
            comp = criteria[0]
            sort_by, _ = self.get_data_types(comp, data_type, fetch_one)
            conditions.append(
                {
                    "column": f"{sort_by}_{comp}",
                    "operator": criteria[1],
                    "value": parse_number(criteria[2]),
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

    def get_data_types(self, comp, data_type, fetch_one):
        """
        Determine whether we want components (values that come straight from
        Census data files) or compounds (values that can only be obtained by
        math operations involving multiple components).

        By default, display compounds if there is one for the comp.
        Otherwise, display a component.
        """
        if not data_type:
            if comp in fetch_one.c.keys():
                sort_by = "c"
                print_ = "fcd"
            else:
                sort_by = "rc"
                print_ = "fc"
        # User input 'c', so display a component
        elif data_type == "c":
            sort_by = "rc"
            print_ = "fc"
        # User input 'cc' (or anything else...), so display a compound
        else:
            sort_by = "c"
            print_ = "fcd"

        return (sort_by, print_)

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

            # Convert pipe-delimited criteria string to a list of criteria
            filter_criteria = geofilter.split("+")
            # Covert list of criteria to lists of lists
            filter_criteria = map(lambda x: x.split(":"), filter_criteria)

            for filter_criterium in filter_criteria:
                # Determine if a data_type was specified
                if len(filter_criterium) == 4:
                    # If so, set the data_type
                    data_type = filter_criterium[3]
                else:
                    # Otherwise, set it to false
                    data_type = False

                comp = filter_criterium[0]
                filter_by, print_ = self.get_data_types(comp, data_type, fetch_one)
                operator_key = filter_criterium[1]

                value = parse_number(filter_criterium[2])

                # Now, filter by operator at index 1.
                compare = operators.get(operator_key)
                if compare is None:
                    raise ValueError("filter: Invalid operator")

                instances = list(
                    filter(lambda x: compare(getattr(x, filter_by)[comp], value), instances)
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

    def extreme_values(
        self, comp, data_type="c", context="", geofilter="", n=10, lowest=False, **kwargs
    ):
        """Get highest and lowest values."""
        d = self.get_data_products()

        comp = comp
        data_type = data_type

        dpi_instances = d["demographicprofiles"]
        fetch_one = dpi_instances[0]

        sort_by, print_ = self.get_data_types(comp, data_type, fetch_one)
        if n <= 0:
            n = len(dpi_instances)

        if self._repo_supports("query_extreme_profile_names"):
            sql_params = self._build_sql_query_params(context, geofilter, fetch_one)

            exclude_values = []
            if comp == "median_year_structure_built" and sort_by == "rc":
                exclude_values = [0, 18]

            try:
                names = self.primary_repository.query_extreme_profile_names(
                    comp_column=f"{sort_by}_{comp}",
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
            filter(lambda x: not numpy.isnan(getattr(x, sort_by)[comp]), dpi_instances)
        )

        # Filter instances
        dpi_instances = self.context_filter(dpi_instances, context, geofilter)

        # For the median_year_structure_built component, remove values of zero and
        # 18...
        if comp == "median_year_structure_built":
            dpi_instances = list(
                filter(lambda x: not x.rc["median_year_structure_built"] == 0, dpi_instances)
            )
            dpi_instances = list(
                filter(lambda x: not x.rc["median_year_structure_built"] == 18, dpi_instances)
            )

        # Sort our DemographicProfile instances by component or compound specified.
        if lowest:
            return heapq.nsmallest(n, dpi_instances, key=lambda x: getattr(x, sort_by)[comp])
        return heapq.nlargest(n, dpi_instances, key=lambda x: getattr(x, sort_by)[comp])

    def lowest_values(self, comp, data_type="c", context="", geofilter="", n=10, **kwargs):
        """Wrapper function for lowest values."""
        return self.extreme_values(
            comp, data_type=data_type, context=context, geofilter=geofilter, n=n, lowest=True
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
        """Output data to a CSV file"""
        d = self.get_data_products()
        dpi_instances = d["demographicprofiles"]
        fetch_one = dpi_instances[0]

        # Categories: Groups of >= 1 comp(s)
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

        # Replace categories with comps and validate comps
        for comp in comps:
            if comp in categories.keys():
                comp_list += categories[comp]
            elif comp in fetch_one.rh:
                comp_list += [comp]
            else:
                raise ValueError(comp + ": Invalid comp")

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

        for comp in comp_list:
            if comp in fetch_one.rc.keys() and comp in fetch_one.c.keys():
                this_row += [fetch_one.rh[comp] + " (c)"]
                this_row += [fetch_one.rh[comp] + " (cc)"]
            else:
                this_row += [fetch_one.rh[comp]]

        csvwriter.writerow(this_row)

        # All other rows
        for dpi_instance in dpi_instances:
            this_row = [dpi_instance.name, ", ".join(dpi_instance.counties_display)]

            for comp in comp_list:
                if comp in dpi_instance.rc.keys() and comp in dpi_instance.c.keys():
                    this_row += [dpi_instance.fc[comp]]
                    this_row += [dpi_instance.fcd[comp]]
                elif comp in dpi_instance.rc:
                    this_row += [dpi_instance.fc[comp]]
                else:
                    this_row += [dpi_instance.fcd[comp]]

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
