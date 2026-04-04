import argparse
import copy
import json
import logging
import re
import sys

from geocompare import __version__
from geocompare.services.query_service import QueryService
from geocompare.tools.map_links import profile_map_links
from geocompare.tools.numeric import parse_float, parse_int
from geocompare.tools.query_syntax import build_context


def _parse_cli_int(value):
    parsed = parse_int(value, default=None)
    if parsed is None:
        raise argparse.ArgumentTypeError(f"invalid integer value: {value}")
    return parsed


def _parse_cli_float(value):
    parsed = parse_float(value, default=None)
    if parsed is None:
        raise argparse.ArgumentTypeError(f"invalid numeric value: {value}")
    return parsed


class GeoCompareCLI:
    def __init__(self):
        self.engine = QueryService()

        self.ct = self.engine.ct
        self.st = self.engine.st
        self.kt = self.engine.kt
        self.slt = self.engine.slt

        parser = argparse.ArgumentParser(
            description="Explore and compare geography data from your local data products.",
            epilog=(
                'Examples: geocompare build ~/data | geocompare query search "san francisco" '
                '| geocompare query distance "San Francisco city, California" '
                '"San Jose city, California"'
            ),
            prog="geocompare",
        )
        subparsers = parser.add_subparsers(
            help="enter geocompare <command> -h for details",
            dest="command",
            required=True,
        )
        parser.add_argument(
            "--log-level",
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
            default="WARNING",
            help="set diagnostics verbosity",
        )
        parser.add_argument(
            "--version",
            action="version",
            version=f"geocompare {__version__}",
        )

        build_parser = subparsers.add_parser("build", help="build data products from source files")
        build_parser.add_argument("path", help="path to data files")
        build_parser.set_defaults(func=self.create_data_products)

        sources_parser = subparsers.add_parser(
            "sources", help="show built-in data source information"
        )
        sources_parser.add_argument(
            "--format",
            choices=["table", "json"],
            default="table",
            help="output format",
        )
        sources_parser.set_defaults(func=self.display_sources)

        query_parser = subparsers.add_parser("query", help="query and compare geographies")
        query_subparsers = query_parser.add_subparsers(
            help="enter geocompare query <command> -h for details",
            dest="query_command",
            required=True,
        )

        search_parser = query_subparsers.add_parser("search", help="search geography names")
        search_parser.add_argument("query", help="search query")
        search_parser.add_argument(
            "-n", type=_parse_cli_int, default=15, help="number of results to display"
        )
        search_parser.add_argument(
            "--format",
            choices=["table", "json", "csv"],
            default="table",
            help="output format",
        )
        search_parser.add_argument(
            "--wide", action="store_true", help="wider output without truncation"
        )
        self._add_label_arg(search_parser)
        search_parser.set_defaults(func=self.display_label_search)

        profile_parser = query_subparsers.add_parser("profile", help="show one demographic profile")
        profile_parser.add_argument("display_label", help="the exact geography name")
        profile_parser.add_argument(
            "--profile-view",
            choices=["compact", "full"],
            default="full",
            help="profile display density",
        )
        self._add_label_arg(profile_parser)
        profile_parser.set_defaults(func=self.get_dp)

        map_links_parser = query_subparsers.add_parser(
            "map-links", help="print Google Maps and Street View URLs for one geography"
        )
        map_links_parser.add_argument("display_label", help="the exact geography name")
        map_links_parser.add_argument(
            "--street-bias",
            choices=["any-road", "arterials", "local-streets"],
            default="any-road",
            help="bias random Street View toward arterials or local streets",
        )
        map_links_parser.set_defaults(func=self.map_links)

        profile_compare_parser = query_subparsers.add_parser(
            "profile-compare", help="compare multiple demographic profiles line by line"
        )
        profile_compare_parser.add_argument(
            "display_labels", nargs="+", help="two or more exact geography names"
        )
        profile_compare_parser.add_argument(
            "--profile-view",
            choices=["compact", "full"],
            default="full",
            help="profile display density",
        )
        self._add_label_arg(profile_compare_parser)
        profile_compare_parser.set_defaults(func=self.profile_compare)

        similar_parser = query_subparsers.add_parser("similar", help="show nearest GeoVectors")
        similar_parser.add_argument("display_label", help="the exact geography name")
        self._add_context_args(similar_parser)
        similar_parser.add_argument(
            "-n", type=_parse_cli_int, default=15, help="number of rows to display"
        )
        self._add_label_arg(similar_parser)
        similar_parser.set_defaults(func=self.compare_geovectors)

        similar_form_parser = query_subparsers.add_parser(
            "similar-form", help="show nearest GeoVectors (built-form mode)"
        )
        similar_form_parser.add_argument("display_label", help="the exact geography name")
        self._add_context_args(similar_form_parser)
        similar_form_parser.add_argument(
            "-n", type=_parse_cli_int, default=15, help="number of rows to display"
        )
        self._add_label_arg(similar_form_parser)
        similar_form_parser.set_defaults(func=self.compare_geovectors_form)

        top_parser = query_subparsers.add_parser(
            "top", help="show highest values by data identifier"
        )
        self._add_rank_args(top_parser)
        self._add_label_arg(top_parser)
        top_parser.set_defaults(func=self.extreme_values)

        bottom_parser = query_subparsers.add_parser(
            "bottom", help="show lowest values by data identifier"
        )
        self._add_rank_args(bottom_parser)
        self._add_label_arg(bottom_parser)
        bottom_parser.set_defaults(func=self.lowest_values)

        nearest_parser = query_subparsers.add_parser(
            "nearest", help="show closest geographies by distance"
        )
        nearest_parser.add_argument("display_label", help="the exact geography name")
        self._add_filter_arg(nearest_parser)
        self._add_context_args(nearest_parser)
        nearest_parser.add_argument(
            "-n", type=_parse_cli_int, default=15, help="number of rows to display"
        )
        self._add_label_arg(nearest_parser)
        nearest_parser.set_defaults(func=self.closest_geographies)

        remoteness_parser = query_subparsers.add_parser(
            "remoteness",
            help="rank geographies by distance to the nearest geography across a threshold",
        )
        remoteness_parser.add_argument(
            "data_identifier", help="metric used for the threshold split"
        )
        remoteness_parser.add_argument("threshold", help="numeric threshold for the metric")
        remoteness_parser.add_argument(
            "--target",
            choices=["below", "above"],
            default="below",
            help="which side of the threshold counts as the qualifying destination",
        )
        remoteness_parser.add_argument(
            "--county-population-min",
            type=_parse_cli_int,
            help="only include geographies whose containing county has at least this many residents",
        )
        remoteness_parser.add_argument(
            "--county-density-min",
            type=_parse_cli_float,
            help="only include geographies whose containing county has at least this population density",
        )
        remoteness_parser.add_argument(
            "--one-per-county",
            action="store_true",
            help="keep only the top-ranked geography from each county",
        )
        self._add_filter_arg(remoteness_parser)
        remoteness_parser.add_argument(
            "--match-where",
            dest="match_geofilter",
            help="filter criteria for qualifying geographies only",
        )
        self._add_context_args(remoteness_parser)
        remoteness_parser.add_argument(
            "-k", "--kilometers", action="store_true", help="display distance in kilometers"
        )
        remoteness_parser.add_argument(
            "--show-area",
            action="store_true",
            help="include land area in square miles",
        )
        remoteness_parser.add_argument(
            "-n", type=_parse_cli_int, default=15, help="number of rows to display"
        )
        self._add_label_arg(remoteness_parser)
        remoteness_parser.set_defaults(func=self.remoteness)

        local_average_parser = query_subparsers.add_parser(
            "local-average",
            help="rank geographies by the distance-weighted local average of a metric",
        )
        local_average_parser.add_argument(
            "data_identifier", help="metric used for the local average"
        )
        local_average_parser.add_argument(
            "--neighbors",
            type=_parse_cli_int,
            default=20,
            help="number of nearest geographies to include in the local average",
        )
        local_average_parser.add_argument(
            "--county-population-min",
            type=_parse_cli_int,
            help="only include geographies whose containing county has at least this many residents",
        )
        local_average_parser.add_argument(
            "--county-density-min",
            type=_parse_cli_float,
            help="only include geographies whose containing county has at least this population density",
        )
        local_average_parser.add_argument(
            "--one-per-county",
            action="store_true",
            help="keep only the top-ranked geography from each county",
        )
        self._add_filter_arg(local_average_parser)
        self._add_context_args(local_average_parser)
        local_average_parser.add_argument(
            "-k",
            "--kilometers",
            action="store_true",
            help="display neighborhood span in kilometers",
        )
        local_average_parser.add_argument(
            "-n", type=_parse_cli_int, default=15, help="number of rows to display"
        )
        self._add_label_arg(local_average_parser)
        local_average_parser.set_defaults(func=self.local_average)

        dist_parser = query_subparsers.add_parser(
            "distance", help="distance between two geographies"
        )
        dist_parser.add_argument("display_label_1", help="first geography")
        dist_parser.add_argument("display_label_2", help="second geography")
        dist_parser.add_argument(
            "-k", "--kilometers", action="store_true", help="display result in kilometers"
        )
        dist_parser.set_defaults(func=self.distance)

        resolve_parser = subparsers.add_parser(
            "resolve", help="resolve a geography string to canonical IDs"
        )
        resolve_parser.add_argument("query", help="input geography string to resolve")
        resolve_parser.add_argument("--state", help="optional state filter, e.g. ca")
        resolve_parser.add_argument("--sumlevel", help="optional summary level filter, e.g. 160")
        resolve_parser.add_argument(
            "--population", type=_parse_cli_int, help="optional population hint"
        )
        resolve_parser.add_argument(
            "-n", type=_parse_cli_int, default=5, help="number of matches to return"
        )
        resolve_parser.add_argument(
            "--format",
            choices=["table", "json", "csv"],
            default="table",
            help="output format",
        )
        resolve_parser.add_argument(
            "--wide", action="store_true", help="wider output without truncation"
        )
        self._add_label_arg(resolve_parser)
        resolve_parser.set_defaults(func=self.resolve_geography)

        export_parser = subparsers.add_parser("export", help="export data as CSV")
        export_subparsers = export_parser.add_subparsers(
            help="enter geocompare export <command> -h for details",
            dest="export_command",
            required=True,
        )

        export_rows_parser = export_subparsers.add_parser(
            "rows", help="export multiple rows to CSV"
        )
        export_rows_parser.add_argument("comps", help="data identifiers to output")
        self._add_filter_arg(export_rows_parser)
        self._add_context_args(export_rows_parser)
        export_rows_parser.add_argument(
            "-n", type=_parse_cli_int, default=0, help="number of rows to display (0 = all)"
        )
        export_rows_parser.set_defaults(func=self.rows)

        export_profile_parser = export_subparsers.add_parser(
            "profile", help="export one demographic profile to CSV"
        )
        export_profile_parser.add_argument("display_label", help="the exact geography name")
        export_profile_parser.add_argument(
            "--profile-view",
            choices=["compact", "full"],
            default="full",
            help="profile export density",
        )
        self._add_label_arg(export_profile_parser)
        export_profile_parser.set_defaults(func=self.get_csv_dp)

        args = parser.parse_args()
        logging.basicConfig(
            level=getattr(logging, args.log_level),
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
        args.func(args)

    def _add_rank_args(self, parser):
        parser.add_argument("data_identifier", help="the data identifier you want to rank")
        self._add_filter_arg(parser)
        self._add_context_args(parser)
        parser.add_argument("-n", type=_parse_cli_int, default=15, help="number of rows to display")

    def _add_filter_arg(self, parser):
        parser.add_argument(
            "-w",
            "--where",
            dest="geofilter",
            help="filter criteria in modern form (for example: population>=100000)",
        )

    def _add_label_arg(self, parser):
        parser.add_argument(
            "--official-labels",
            action="store_true",
            help="display official Census tract labels when available",
        )

    def _add_context_args(self, parser):
        parser.add_argument(
            "-s",
            "--scope",
            dest="context",
            help="scope string (for example: tracts+ca, 140+06075:county, 94103)",
        )
        parser.add_argument(
            "--universe",
            help="universe summary level (for example: tracts/places/counties or 140/160/050)",
        )
        parser.add_argument(
            "--universes",
            help="comma-separated universes to query across (for example: places,tracts or All)",
        )
        scope_group = parser.add_mutually_exclusive_group()
        scope_group.add_argument("--in-state", help="group scope by state abbreviation")
        scope_group.add_argument(
            "--in-county",
            help="group scope by county (06075:county, ca:losangeles, or 'Los Angeles County, California')",
        )
        scope_group.add_argument("--in-zcta", help="group scope by zcta (for example: 94103)")

    def _normalize_scope_args(self, args):
        if getattr(args, "universe", None) and getattr(args, "universes", None):
            raise ValueError("Use either --universe or --universes, not both.")
        args.context = build_context(
            context=getattr(args, "context", None),
            universe=getattr(args, "universe", None),
            in_state=getattr(args, "in_state", None),
            in_county=getattr(args, "in_county", None),
            in_zcta=getattr(args, "in_zcta", None),
        )

    def create_data_products(self, args):
        self.engine.create_data_products(args.path)
        print("Data product write completed.")

    def display_sources(self, args):
        rows = self.engine.sources()
        if args.format == "json":
            print(json.dumps(rows, indent=2))
            return

        columns = [
            ("Name", max(len("Name"), *(len(row["name"]) for row in rows))),
            ("Used for", max(len("Used for"), *(len(row["used_for"]) for row in rows))),
            ("Provider", max(len("Provider"), *(len(row["provider"]) for row in rows))),
        ]
        width = 1 + sum(column_width + 1 for _, column_width in columns) - 1
        print("-" * width)
        print(
            " "
            + " ".join(
                self._fit(header, column_width, truncate=False) for header, column_width in columns
            )
        )
        print("-" * width)
        for row in rows:
            print(
                " "
                + " ".join(
                    [
                        self._fit(row["name"], columns[0][1]),
                        self._fit(row["used_for"], columns[1][1]),
                        self._fit(row["provider"], columns[2][1]),
                    ]
                )
            )
            print(f" Notes: {row['notes']}")
        print("-" * width)

    def _eprint(self, msg):
        print(msg, file=sys.stderr)

    def _fit(self, value, width, truncate=True):
        text = str(value)
        if truncate and len(text) > width:
            return text[:width]
        return text.ljust(width)

    def _fit_right(self, value, width, truncate=True):
        text = str(value)
        if truncate and len(text) > width:
            return text[:width]
        return text.rjust(width)

    def _display_name(self, geography, official_labels=False):
        if official_labels and getattr(geography, "sumlevel", None) == "140":
            return getattr(geography, "canonical_name", geography.name)
        return geography.name

    def _display_profile(self, geography, official_labels=False):
        if not official_labels or getattr(geography, "sumlevel", None) != "140":
            return geography
        display_geo = copy.deepcopy(geography)
        display_geo.name = getattr(geography, "canonical_name", geography.name)
        return display_geo

    def _get_single_profile(self, display_label):
        try:
            dp_list = self.engine.get_dp(display_label=display_label)
        except ValueError:
            self._eprint("Sorry, there is no geography with that name.")
            return None
        if len(dp_list) == 0:
            self._eprint("Sorry, there is no geography with that name.")
            return None
        return dp_list[0]

    def _display_area(self, geography, square_kilometers=False):
        raw_value = getattr(geography, "rc", {}).get("land_area")
        if raw_value in {None, ""}:
            return ""

        try:
            area_value = float(raw_value)
            if square_kilometers:
                area_value *= 2.589988110336
            return f"{area_value:,.1f}"
        except (TypeError, ValueError):
            formatted = getattr(geography, "fc", {}).get("land_area")
            if formatted:
                return formatted.replace(" sqmi", "")
            return str(raw_value)

    def display_label_search(self, args):
        search_results = self.engine.display_label_search(**vars(args))
        if args.format == "json":
            payload = [
                {
                    "name": self._display_name(r, args.official_labels),
                    "population": r.fc["population"],
                }
                for r in search_results[: args.n]
            ]
            print(json.dumps(payload, indent=2))
            return
        if args.format == "csv":
            print("Name,Population")
            for r in search_results[: args.n]:
                print(f'"{self._display_name(r, args.official_labels)}","{r.fc["population"]}"')
            return

        name_width = 70 if args.wide else 45
        truncate = not args.wide

        def print_search_divider():
            return "-" * (name_width + 23)

        def print_search_result(dpi):
            iam = " "
            out_str = (
                iam
                + self._fit(
                    self._display_name(dpi, args.official_labels), name_width, truncate=truncate
                )
                + iam
                + getattr(dpi, "fc")["population"].rjust(20)
            )
            return out_str

        print(print_search_divider())
        iam = " "
        print(
            iam
            + self._fit("Search results", name_width, truncate=truncate)
            + iam
            + "Total population".rjust(20)
        )
        print(print_search_divider())
        for dpi_instance in search_results[: args.n]:
            print(print_search_result(dpi_instance))
        print(print_search_divider())

    def get_dp(self, args):
        dp = self._get_single_profile(args.display_label)
        if dp is None:
            return
        print(self._display_profile(dp, args.official_labels).to_table(view=args.profile_view))

    def map_links(self, args):
        dp = self._get_single_profile(args.display_label)
        if dp is None:
            return

        try:
            links = profile_map_links(dp, street_bias=args.street_bias)
        except ValueError as exc:
            self._eprint(str(exc))
            return

        print(f"Open in Google Maps URL: {links['google_maps_url']}")
        print(f"Random Google Street View URL: {links['google_street_view_url']}")

    def _profile_metric_value(self, dp, row_mode, key):
        if not dp._can_render_row(row_mode, key):
            return ""
        if row_mode == "std":
            return dp.fc[key]
        if row_mode == "co":
            return dp.fcd[key]
        return dp.fc[key]

    def _profile_metric_cell(self, dp, row_mode, key, component_width, compound_width):
        if not dp._can_render_row(row_mode, key):
            return ""
        if row_mode == "std":
            component_col = dp.fc[key]
            compound_col = dp.fcd[key]
        elif row_mode == "co":
            component_col = ""
            compound_col = dp.fcd[key]
        else:
            component_col = ""
            compound_col = dp.fc[key]
        return component_col.rjust(component_width) + " " + compound_col.rjust(compound_width)

    def profile_compare(self, args):
        if len(args.display_labels) < 2:
            self._eprint("Please provide at least two geography names to compare.")
            return

        dps = []
        for display_label in args.display_labels:
            try:
                dps.extend(self.engine.get_dp(display_label=display_label))
            except ValueError:
                self._eprint(f"No geography found for display label: {display_label}")
                return

        dps = [self._display_profile(dp, args.official_labels) for dp in dps]

        sections = dps[0]._sections_for_view(args.profile_view)
        rows = []
        for section_title, section_rows in sections:
            renderable_rows = [
                (row_mode, key)
                for row_mode, key in section_rows
                if any(dp._can_render_row(row_mode, key) for dp in dps)
            ]
            if not renderable_rows:
                continue
            rows.append(("section", section_title, None))
            for row_mode, key in renderable_rows:
                rows.append(("metric", row_mode, key))

        metric_labels = []
        for row_type, row_mode_or_section, key in rows:
            if row_type == "section":
                metric_labels.append(row_mode_or_section)
            else:
                metric_labels.append(dps[0].rh.get(key, key))
        metric_width = max([30, len("Metric")] + [len(label) for label in metric_labels])

        county_headers = [
            ", ".join(dp.counties_display) if getattr(dp, "counties_display", None) else ""
            for dp in dps
        ]
        per_geo_component_widths = []
        per_geo_compound_widths = []
        min_name_width = 24
        for dp in dps:
            component_lengths = []
            compound_lengths = []
            for row_type, row_mode_or_section, key in rows:
                if row_type == "metric":
                    if row_mode_or_section == "std":
                        if key in dp.fc:
                            component_lengths.append(len(dp.fc[key]))
                        if key in dp.fcd:
                            compound_lengths.append(len(dp.fcd[key]))
                    elif row_mode_or_section == "co":
                        if key in dp.fcd:
                            compound_lengths.append(len(dp.fcd[key]))
                    else:
                        if key in dp.fc:
                            compound_lengths.append(len(dp.fc[key]))
            per_geo_component_widths.append(max([15] + component_lengths))
            per_geo_compound_widths.append(max([15] + compound_lengths))

        shared_component_width = max(per_geo_component_widths) if per_geo_component_widths else 15
        shared_compound_width = max(per_geo_compound_widths) if per_geo_compound_widths else 15
        shared_data_width = shared_component_width + 1 + shared_compound_width
        shared_col_width = max(
            [min_name_width, shared_data_width]
            + [len(dp.name) for dp in dps]
            + [len(county_header) for county_header in county_headers]
        )
        col_widths = [shared_col_width for _ in dps]

        divider_width = metric_width + sum(col_widths) + len(dps) + 2
        print("-" * divider_width)
        print(
            " "
            + self._fit("Metric", metric_width, truncate=False)
            + " "
            + " ".join(self._fit(dp.name, col_widths[idx]) for idx, dp in enumerate(dps))
        )
        print(
            " "
            + self._fit("", metric_width, truncate=False)
            + " "
            + " ".join(self._fit(county_headers[idx], col_widths[idx]) for idx in range(len(dps)))
        )
        print("-" * divider_width)

        for row_type, row_mode_or_section, key in rows:
            if row_type == "section":
                print(
                    " "
                    + self._fit(row_mode_or_section, metric_width, truncate=False)
                    + " "
                    + " ".join("".ljust(col_widths[idx]) for idx in range(len(dps)))
                )
                continue

            metric_label = dps[0].rh.get(key, key)
            values = [
                self._fit(
                    self._profile_metric_cell(
                        dp,
                        row_mode_or_section,
                        key,
                        shared_component_width,
                        shared_compound_width,
                    ),
                    col_widths[idx],
                )
                for idx, dp in enumerate(dps)
            ]
            print(
                " " + self._fit(metric_label, metric_width, truncate=False) + " " + " ".join(values)
            )

        print("-" * divider_width)

    def resolve_geography(self, args):
        matches = self.engine.resolve_geography(**vars(args))
        if len(matches) == 0:
            self._eprint("No matches found.")
            return
        if args.format == "json":
            print(json.dumps(matches[: args.n], indent=2))
            return
        if args.format == "csv":
            print("canonical_id,sumlevel,state,population,name")
            for m in matches[: args.n]:
                pop = m.get("population")
                pop_display = "" if pop is None else int(pop)
                print(
                    f'"{m["canonical_id"]}","{m["sumlevel"]}","{m["state"]}","{pop_display}","{m["name"]}"'
                )
            return

        id_width = 54 if args.wide else 38

        print("-" * (id_width + 58))
        print(
            " Canonical ID".ljust(id_width),
            "Summary Level".ljust(15),
            "State".ljust(7),
            "Population".rjust(12),
            " Name",
        )
        print("-" * (id_width + 58))
        for match in matches:
            pop = match.get("population")
            pop_display = "" if pop is None else f"{int(pop):,}"
            cid = match["canonical_id"] if args.wide else match["canonical_id"][:36]
            display_name = (
                match.get("canonical_name")
                if args.official_labels
                and match.get("sumlevel") == "140"
                and match.get("canonical_name")
                else match["name"]
            )
            print(
                f" {cid.ljust(id_width)}"
                f" {match['sumlevel'].ljust(15)}"
                f" {match['state'].ljust(7)}"
                f" {pop_display.rjust(12)}"
                f" {display_name}"
            )
        print("-" * (id_width + 58))

    def compare_geovectors(self, args, mode="std"):
        try:
            self._normalize_scope_args(args)
        except ValueError as exc:
            self._eprint(str(exc))
            return
        if mode == "form":
            mode = "app"
        closest_gvs = self.engine.compare_geovectors(**vars(args), mode=mode)

        if len(closest_gvs) == 0:
            self._eprint("Sorry, no GeoVectors match your criteria.")
            return

        comparison_gv = closest_gvs[0]

        if mode == "std":
            print("The most demographically similar geographies are:")
            header = " ".join(
                [
                    " Geography".ljust(41),
                    "County".ljust(20),
                    "Population".rjust(11),
                    "PopD",
                    "Inc",
                    "White",
                    "Black",
                    "Asian",
                    "Hisp",
                    "Bach+",
                    "Grad+",
                    " Distance",
                ]
            )
        else:
            print("The most similar geographies by built form are:")
            header = " ".join(
                [
                    " Geography".ljust(41),
                    "County".ljust(20),
                    "Population".rjust(11),
                    "PopD",
                    "HouseD",
                    "OWN",
                    "Year",
                    "Rooms",
                    "HHSize",
                    " Distance",
                ]
            )
        print()
        divider = "-" * len(header)
        print(divider)
        print(header)
        print(divider)

        for closest_pv in closest_gvs:
            if args.official_labels and getattr(closest_pv, "sumlevel", None) == "140":
                closest_pv = copy.deepcopy(closest_pv)
                closest_pv.name = getattr(closest_pv, "canonical_name", closest_pv.name)
            print(
                "",
                closest_pv.display_row(mode),
                round(comparison_gv.distance(closest_pv, mode=mode), 2),
            )

        print(divider)

    def compare_geovectors_form(self, args):
        self.compare_geovectors(args, mode="app")

    def extreme_values(self, args, lowest=False):
        try:
            self._normalize_scope_args(args)
        except ValueError as exc:
            self._eprint(str(exc))
            return
        try:
            evs = self.engine.extreme_values(**vars(args), lowest=lowest)
        except ValueError as exc:
            self._eprint(str(exc))
            return
        if len(evs) == 0:
            self._eprint("Sorry, no geographies match your criteria.")
            return

        fetch_one = evs[0]
        resolved = self.engine.resolve_data_identifier(args.data_identifier, fetch_one)
        key = resolved["key"]
        print_ = resolved["display_store"]
        iam = " "

        def divider(dpi):
            return "-" * (68 if key == "population" else 89)

        def ev_print_headers(comp, universe_sl, group_sl, group):
            if universe_sl == "010":
                universe = "Nation"
            elif universe_sl == "040":
                universe = "State"
            elif universe_sl == "050":
                universe = "County"
            elif universe_sl == "160":
                universe = "Place"
            elif universe_sl == "310":
                universe = "Metro/micro area"
            elif universe_sl == "400":
                universe = "Urban area"
            elif universe_sl == "860":
                universe = "ZCTA"
            else:
                universe = "Geography"

            if group:
                if group_sl == "040":
                    group_name = self.st.get_name(group)
                elif group_sl == "050":
                    if re.match(r"^\d{5}:county$", group):
                        group_name = self.ct.county_geoid_to_name[group.split(":", 1)[0]]
                    else:
                        county_key = "us:" + group + "/county"
                        group_name = self.kt.key_to_county_name[county_key]
                elif group_sl == "860":
                    group_name = group
                else:
                    group_name = group
                out_str = (
                    iam
                    + (universe + " in " + group_name).ljust(45)[:45]
                    + iam
                    + getattr(dpi, "rh")["population"].rjust(20)
                )
            else:
                out_str = (
                    iam + universe.ljust(45)[:45] + iam + getattr(dpi, "rh")["population"].rjust(20)
                )

            if key != "population":
                out_str += iam + resolved["label"].rjust(20)[:20]

            return out_str

        def ev_print_row(dpi):
            out_str = (
                iam
                + self._display_name(dpi, args.official_labels).ljust(45)[:45]
                + iam
                + getattr(dpi, "fc")["population"].rjust(20)
            )
            if key != "population":
                out_str += iam + getattr(dpi, print_)[key].rjust(20)[:20]
            return out_str

        universe_sl, group_sl, group = self.slt.unpack_context(args.context)
        dpi = evs[0]
        print(divider(dpi))
        print(ev_print_headers(key, universe_sl, group_sl, group))
        print(divider(dpi))
        for ev in evs[: args.n]:
            print(ev_print_row(ev))
        print(divider(dpi))

    def lowest_values(self, args):
        self.extreme_values(args, lowest=True)

    def closest_geographies(self, args):
        try:
            self._normalize_scope_args(args)
        except ValueError as exc:
            self._eprint(str(exc))
            return
        cgs = self.engine.closest_geographies(**vars(args))
        iam = " "

        def divider():
            return "-" * 68

        def cg_print_headers(universe_sl, group_sl, group):
            if universe_sl == "010":
                universe = "Nation"
            elif universe_sl == "040":
                universe = "State"
            elif universe_sl == "050":
                universe = "County"
            elif universe_sl == "160":
                universe = "Place"
            elif universe_sl == "310":
                universe = "Metro/micro area"
            elif universe_sl == "400":
                universe = "Urban area"
            elif universe_sl == "860":
                universe = "ZCTA"
            else:
                universe = "Geography"

            if group:
                if group_sl == "040":
                    group_name = self.st.get_name(group)
                elif group_sl == "050":
                    if re.match(r"^\d{5}:county$", group):
                        group_name = self.ct.county_geoid_to_name[group.split(":", 1)[0]]
                    else:
                        key = "us:" + group + "/county"
                        group_name = self.kt.key_to_county_name[key]
                elif group_sl == "860":
                    group_name = group
                else:
                    group_name = group
                out_str = (
                    iam
                    + (universe + " in " + group_name).ljust(45)[:45]
                    + iam
                    + "Distance (mi)".rjust(20)
                )
            else:
                out_str = iam + universe.ljust(45)[:45] + iam + "Distance (mi)".rjust(20)

            return out_str

        def cg_print_row(dpi, distance):
            return (
                iam
                + self._display_name(dpi, args.official_labels).ljust(45)[:45]
                + iam
                + str(round(distance, 1)).rjust(20)
            )

        universe_sl, group_sl, group = self.slt.unpack_context(args.context)

        if len(cgs) == 0:
            self._eprint("Sorry, no geographies match your criteria.")
            return

        print(divider())
        print(cg_print_headers(universe_sl, group_sl, group))
        print(divider())
        for cg in cgs[: args.n]:
            print(cg_print_row(*cg))
        print(divider())

    def distance(self, args):
        print(self.engine.distance(**vars(args)))

    def remoteness(self, args):
        try:
            self._normalize_scope_args(args)
        except ValueError as exc:
            self._eprint(str(exc))
            return
        if not args.context:
            args.context = "tracts+"
        try:
            results = self.engine.remoteness(**vars(args))
        except ValueError as exc:
            self._eprint(str(exc))
            return

        if len(results) == 0:
            self._eprint("Sorry, no geographies match your criteria.")
            return

        fetch_one = results[0]["candidate"]
        resolved = self.engine.resolve_data_identifier(args.data_identifier, fetch_one)
        display_store = resolved["display_store"]
        key = resolved["key"]

        def metric_value(dp):
            return getattr(dp, display_store)[key]

        distance_label = "Dist (km)" if args.kilometers else "Dist (mi)"
        area_label = "Area (sqkm)" if args.kilometers else "Area (sqmi)"

        displayed_rows = results[: getattr(args, "n", len(results))]
        population_values = [row["candidate"].fc["population"] for row in displayed_rows]
        area_values = (
            [
                self._display_area(row["candidate"], square_kilometers=args.kilometers)
                for row in displayed_rows
            ]
            if args.show_area
            else []
        )
        candidate_values = [
            self._display_name(row["candidate"], args.official_labels) for row in displayed_rows
        ]
        nearest_values = [
            self._display_name(row["nearest_match"], args.official_labels) for row in displayed_rows
        ]
        metric_values = [metric_value(row["candidate"]) for row in displayed_rows]
        match_values = [metric_value(row["nearest_match"]) for row in displayed_rows]
        distance_values = [
            f"{(row['distance_miles'] * 1.609344 if args.kilometers else row['distance_miles']):.1f}"
            for row in displayed_rows
        ]

        candidate_width = max(44, len("Candidate"))
        population_width = max(len("Pop"), *(len(value) for value in population_values))
        area_width = (
            max(len(area_label), *(len(value) for value in area_values)) if args.show_area else 0
        )
        metric_width = max(len("Value"), *(len(value) for value in metric_values))
        nearest_width = max(31, len("Nearest qualifying geography"))
        distance_width = max(len(distance_label), *(len(value) for value in distance_values))
        match_width = max(len("Match Val"), *(len(value) for value in match_values))
        columns = [
            ("Candidate", candidate_width, False),
            ("Pop", population_width, True),
        ]
        if args.show_area:
            columns.append((area_label, area_width, True))
        columns.extend(
            [
                ("Value", metric_width, True),
                ("Nearest qualifying geography", nearest_width, False),
                (distance_label, distance_width, True),
                ("Match Val", match_width, True),
            ]
        )
        width = 1 + sum(column_width + 1 for _, column_width, _ in columns) - 1
        print("-" * width)
        print(
            " "
            + " ".join(
                (
                    self._fit_right(header, column_width, truncate=False)
                    if right_align
                    else self._fit(header, column_width, truncate=False)
                )
                for header, column_width, right_align in columns
            )
        )
        print("-" * width)
        for (
            row,
            candidate_name,
            nearest_name,
            candidate_metric,
            nearest_metric,
            area_value,
            distance_text,
        ) in zip(
            displayed_rows,
            candidate_values,
            nearest_values,
            metric_values,
            match_values,
            area_values if args.show_area else [""] * len(displayed_rows),
            distance_values,
        ):
            candidate = row["candidate"]
            row_values = [
                self._fit(candidate_name, candidate_width),
                self._fit_right(candidate.fc["population"], population_width),
            ]
            if args.show_area:
                row_values.append(self._fit_right(area_value, area_width))
            row_values.extend(
                [
                    self._fit_right(candidate_metric, metric_width),
                    self._fit(nearest_name, nearest_width),
                    self._fit_right(distance_text, distance_width),
                    self._fit_right(nearest_metric, match_width),
                ]
            )
            print(" " + " ".join(row_values))
        print("-" * width)

    def local_average(self, args):
        try:
            self._normalize_scope_args(args)
        except ValueError as exc:
            self._eprint(str(exc))
            return
        if not args.context:
            args.context = "tracts+"
        try:
            results = self.engine.local_average(**vars(args))
        except ValueError as exc:
            self._eprint(str(exc))
            return

        if len(results) == 0:
            self._eprint("Sorry, no geographies match your criteria.")
            return

        fetch_one = results[0]["candidate"]
        resolved = self.engine.resolve_data_identifier(args.data_identifier, fetch_one)
        display_store = resolved["display_store"]
        key = resolved["key"]

        def metric_value(dp):
            return getattr(dp, display_store)[key]

        average_label = "Local Avg"
        span_label = "Span (km)" if args.kilometers else "Span (mi)"

        displayed_rows = results[: getattr(args, "n", len(results))]
        candidate_values = [
            self._display_name(row["candidate"], args.official_labels) for row in displayed_rows
        ]
        population_values = [row["candidate"].fc["population"] for row in displayed_rows]
        metric_values = [metric_value(row["candidate"]) for row in displayed_rows]
        average_values = [
            self.engine._format_profile_component(key, row["local_average"])
            for row in displayed_rows
        ]
        span_values = [
            f"{(row['neighbor_span_miles'] * 1.609344 if args.kilometers else row['neighbor_span_miles']):.1f}"
            for row in displayed_rows
        ]

        candidate_width = max(44, len("Candidate"))
        population_width = max(len("Pop"), *(len(value) for value in population_values))
        metric_width = max(len("Value"), *(len(value) for value in metric_values))
        average_width = max(len(average_label), *(len(value) for value in average_values))
        span_width = max(len(span_label), *(len(value) for value in span_values))
        columns = [
            ("Candidate", candidate_width, False),
            ("Pop", population_width, True),
            ("Value", metric_width, True),
            (average_label, average_width, True),
            (span_label, span_width, True),
        ]
        width = 1 + sum(column_width + 1 for _, column_width, _ in columns) - 1
        print("-" * width)
        print(
            " "
            + " ".join(
                (
                    self._fit_right(header, column_width, truncate=False)
                    if right_align
                    else self._fit(header, column_width, truncate=False)
                )
                for header, column_width, right_align in columns
            )
        )
        print("-" * width)
        for row, candidate_name, average_value, span_value in zip(
            displayed_rows,
            candidate_values,
            average_values,
            span_values,
        ):
            print(
                " "
                + " ".join(
                    [
                        self._fit(candidate_name, candidate_width),
                        self._fit_right(row["candidate"].fc["population"], population_width),
                        self._fit_right(metric_value(row["candidate"]), metric_width),
                        self._fit_right(average_value, average_width),
                        self._fit_right(span_value, span_width),
                    ]
                )
            )
        print("-" * width)

    def rows(self, args):
        try:
            self._normalize_scope_args(args)
        except ValueError as exc:
            self._eprint(str(exc))
            return
        self.engine.rows(**vars(args))

    def get_csv_dp(self, args):
        dp = self._get_single_profile(args.display_label)
        if dp is None:
            return
        self._display_profile(dp, args.official_labels).tocsv(view=args.profile_view)


def main():
    GeoCompareCLI()


if __name__ == "__main__":
    main()
