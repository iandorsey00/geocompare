import argparse
import json
import logging
import sys

from geocompare import __version__
from geocompare.services.query_service import QueryService
from geocompare.tools.query_syntax import build_context


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

        query_parser = subparsers.add_parser("query", help="query and compare geographies")
        query_subparsers = query_parser.add_subparsers(
            help="enter geocompare query <command> -h for details",
            dest="query_command",
            required=True,
        )

        search_parser = query_subparsers.add_parser("search", help="search place names")
        search_parser.add_argument("query", help="search query")
        search_parser.add_argument("-n", type=int, default=15, help="number of results to display")
        search_parser.add_argument(
            "--format",
            choices=["table", "json", "csv"],
            default="table",
            help="output format",
        )
        search_parser.add_argument(
            "--wide", action="store_true", help="wider output without truncation"
        )
        search_parser.set_defaults(func=self.display_label_search)

        profile_parser = query_subparsers.add_parser("profile", help="show one demographic profile")
        profile_parser.add_argument("display_label", help="the exact place name")
        profile_parser.add_argument(
            "--profile-view",
            choices=["compact", "full"],
            default="full",
            help="profile display density",
        )
        profile_parser.set_defaults(func=self.get_dp)

        similar_parser = query_subparsers.add_parser("similar", help="show nearest geovectors")
        similar_parser.add_argument("display_label", help="the exact place name")
        self._add_context_args(similar_parser)
        similar_parser.add_argument("-n", type=int, default=15, help="number of rows to display")
        similar_parser.set_defaults(func=self.compare_geovectors)

        similar_app_parser = query_subparsers.add_parser(
            "similar-app", help="show nearest geovectors (appearance mode)"
        )
        similar_app_parser.add_argument("display_label", help="the exact place name")
        self._add_context_args(similar_app_parser)
        similar_app_parser.add_argument(
            "-n", type=int, default=15, help="number of rows to display"
        )
        similar_app_parser.set_defaults(func=self.compare_geovectors_app)

        top_parser = query_subparsers.add_parser(
            "top", help="show highest values by data identifier"
        )
        self._add_rank_args(top_parser)
        top_parser.set_defaults(func=self.extreme_values)

        bottom_parser = query_subparsers.add_parser(
            "bottom", help="show lowest values by data identifier"
        )
        self._add_rank_args(bottom_parser)
        bottom_parser.set_defaults(func=self.lowest_values)

        nearest_parser = query_subparsers.add_parser(
            "nearest", help="show closest geographies by distance"
        )
        nearest_parser.add_argument("display_label", help="the exact place name")
        self._add_filter_arg(nearest_parser)
        self._add_context_args(nearest_parser)
        nearest_parser.add_argument("-n", type=int, default=15, help="number of rows to display")
        nearest_parser.set_defaults(func=self.closest_geographies)

        dist_parser = query_subparsers.add_parser("distance", help="distance between two places")
        dist_parser.add_argument("display_label_1", help="first place")
        dist_parser.add_argument("display_label_2", help="second place")
        dist_parser.add_argument(
            "-k", "--kilometers", action="store_true", help="display result in kilometers"
        )
        dist_parser.set_defaults(func=self.distance)

        resolve_parser = subparsers.add_parser(
            "resolve", help="resolve a place string to canonical IDs"
        )
        resolve_parser.add_argument("query", help="input place string to resolve")
        resolve_parser.add_argument("--state", help="optional state filter, e.g. ca")
        resolve_parser.add_argument("--sumlevel", help="optional summary level filter, e.g. 160")
        resolve_parser.add_argument("--population", type=int, help="optional population hint")
        resolve_parser.add_argument("-n", type=int, default=5, help="number of matches to return")
        resolve_parser.add_argument(
            "--format",
            choices=["table", "json", "csv"],
            default="table",
            help="output format",
        )
        resolve_parser.add_argument(
            "--wide", action="store_true", help="wider output without truncation"
        )
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
            "-n", type=int, default=0, help="number of rows to display (0 = all)"
        )
        export_rows_parser.set_defaults(func=self.rows)

        export_profile_parser = export_subparsers.add_parser(
            "profile", help="export one demographic profile to CSV"
        )
        export_profile_parser.add_argument("display_label", help="the exact place name")
        export_profile_parser.add_argument(
            "--profile-view",
            choices=["compact", "full"],
            default="full",
            help="profile export density",
        )
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
        parser.add_argument("-n", type=int, default=15, help="number of rows to display")

    def _add_filter_arg(self, parser):
        parser.add_argument(
            "-w",
            "--where",
            dest="geofilter",
            help="filter criteria in modern form (for example: population>=100000)",
        )

    def _add_context_args(self, parser):
        parser.add_argument(
            "-s",
            "--scope",
            dest="context",
            help="scope string (for example: places+ca, 050+06075:county, 94103)",
        )
        parser.add_argument(
            "--universe",
            help="universe summary level (for example: places/counties/states or 160/050/040)",
        )
        scope_group = parser.add_mutually_exclusive_group()
        scope_group.add_argument("--in-state", help="group scope by state abbreviation")
        scope_group.add_argument("--in-county", help="group scope by county key (06075:county)")
        scope_group.add_argument("--in-zcta", help="group scope by zcta (for example: 94103)")

    def _normalize_scope_args(self, args):
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

    def _eprint(self, msg):
        print(msg, file=sys.stderr)

    def _fit(self, value, width, truncate=True):
        text = str(value)
        if truncate and len(text) > width:
            return text[:width]
        return text.ljust(width)

    def display_label_search(self, args):
        search_results = self.engine.display_label_search(**vars(args))
        if args.format == "json":
            payload = [
                {"name": r.name, "population": r.fc["population"]} for r in search_results[: args.n]
            ]
            print(json.dumps(payload, indent=2))
            return
        if args.format == "csv":
            print("Name,Population")
            for r in search_results[: args.n]:
                print(f'"{r.name}","{r.fc["population"]}"')
            return

        name_width = 70 if args.wide else 45
        truncate = not args.wide

        def print_search_divider():
            return "-" * (name_width + 23)

        def print_search_result(dpi):
            iam = " "
            out_str = (
                iam
                + self._fit(getattr(dpi, "name"), name_width, truncate=truncate)
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
        dp_list = self.engine.get_dp(**vars(args))
        if len(dp_list) == 0:
            self._eprint("Sorry, there is no geography with that name.")
            return
        print(dp_list[0].to_table(view=args.profile_view))

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
            print(
                f" {cid.ljust(id_width)}"
                f" {match['sumlevel'].ljust(15)}"
                f" {match['state'].ljust(7)}"
                f" {pop_display.rjust(12)}"
                f" {match['name']}"
            )
        print("-" * (id_width + 58))

    def compare_geovectors(self, args, mode="std"):
        self._normalize_scope_args(args)
        closest_gvs = self.engine.compare_geovectors(**vars(args), mode=mode)

        if len(closest_gvs) == 0:
            self._eprint("Sorry, no GeoVectors match your criteria.")
            return

        comparison_gv = closest_gvs[0]
        width = 105 if mode == "std" else 85

        print("The most demographically similar geographies are:")
        print()
        print("-" * width)
        if mode == "std":
            print(
                " Geography".ljust(41),
                "County".ljust(20),
                "PDN",
                "PCI",
                "WHT",
                "BLK",
                "ASN",
                "HPL",
                "BDH",
                "GDH",
                " Distance",
            )
        else:
            print(" Geography".ljust(41), "County".ljust(20), "PDN", "PCI", "MYS", " Distance")
        print("-" * width)

        for closest_pv in closest_gvs:
            print(
                "",
                closest_pv.display_row(mode),
                round(comparison_gv.distance(closest_pv, mode=mode), 2),
            )

        print("-" * width)

    def compare_geovectors_app(self, args):
        self.compare_geovectors(args, mode="app")

    def extreme_values(self, args, lowest=False):
        self._normalize_scope_args(args)
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
            if universe_sl == "040":
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
                + getattr(dpi, "name").ljust(45)[:45]
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
        self._normalize_scope_args(args)
        cgs = self.engine.closest_geographies(**vars(args))
        iam = " "

        def divider():
            return "-" * 68

        def cg_print_headers(universe_sl, group_sl, group):
            if universe_sl == "040":
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
                iam + getattr(dpi, "name").ljust(45)[:45] + iam + str(round(distance, 1)).rjust(20)
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

    def rows(self, args):
        self._normalize_scope_args(args)
        self.engine.rows(**vars(args))

    def get_csv_dp(self, args):
        self.engine.get_csv_dp(**vars(args))


def main():
    GeoCompareCLI()


if __name__ == "__main__":
    main()
