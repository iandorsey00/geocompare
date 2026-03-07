import argparse

try:
    from geodata.services.query_service import QueryService
except ImportError:  # pragma: no cover - script execution fallback
    from services.query_service import QueryService


class GeodataCLI:
    def __init__(self):
        self.engine = QueryService()

        self.ct = self.engine.ct
        self.st = self.engine.st
        self.kt = self.engine.kt
        self.slt = self.engine.slt

        parser = argparse.ArgumentParser(
            description='Explore and compare geography data from your local data products.',
            epilog=(
                'Examples: geodata build ~/data | geodata query search "san francisco" '
                '| geodata query distance "San Francisco city, California" '
                '"San Jose city, California"'
            ),
            prog='geodata',
        )
        subparsers = parser.add_subparsers(
            help='enter geodata <command> -h for details',
            dest='command',
            required=True,
        )

        build_parser = subparsers.add_parser(
            'build', aliases=['createdb', 'ingest', 'c'], help='build data products from source files'
        )
        build_parser.add_argument('path', help='path to data files')
        build_parser.set_defaults(func=self.create_data_products)

        query_parser = subparsers.add_parser(
            'query', aliases=['view', 'show', 'q', 'v'], help='query and compare geographies'
        )
        query_subparsers = query_parser.add_subparsers(
            help='enter geodata query <command> -h for details',
            dest='query_command',
            required=True,
        )

        search_parser = query_subparsers.add_parser(
            'search', aliases=['find', 'lookup', 's'], help='search place names'
        )
        search_parser.add_argument('query', help='search query')
        search_parser.add_argument('-n', type=int, default=15, help='number of results to display')
        search_parser.set_defaults(func=self.display_label_search)

        profile_parser = query_subparsers.add_parser(
            'profile', aliases=['dp'], help='show one demographic profile'
        )
        profile_parser.add_argument('display_label', help='the exact place name')
        profile_parser.set_defaults(func=self.get_dp)

        similar_parser = query_subparsers.add_parser(
            'similar', aliases=['gv'], help='show nearest geovectors'
        )
        similar_parser.add_argument('display_label', help='the exact place name')
        similar_parser.add_argument('-c', '--context', help='geographies to compare with')
        similar_parser.add_argument('-n', type=int, default=15, help='number of rows to display')
        similar_parser.set_defaults(func=self.compare_geovectors)

        similar_app_parser = query_subparsers.add_parser(
            'similar-app', aliases=['gva'], help='show nearest geovectors (appearance mode)'
        )
        similar_app_parser.add_argument('display_label', help='the exact place name')
        similar_app_parser.add_argument('-c', '--context', help='geographies to compare with')
        similar_app_parser.add_argument('-n', type=int, default=15, help='number of rows to display')
        similar_app_parser.set_defaults(func=self.compare_geovectors_app)

        top_parser = query_subparsers.add_parser(
            'top', aliases=['hv', 'highest'], help='show highest values by component'
        )
        self._add_rank_args(top_parser)
        top_parser.set_defaults(func=self.extreme_values)

        bottom_parser = query_subparsers.add_parser(
            'bottom', aliases=['lv', 'lowest'], help='show lowest values by component'
        )
        self._add_rank_args(bottom_parser)
        bottom_parser.set_defaults(func=self.lowest_values)

        nearest_parser = query_subparsers.add_parser(
            'nearest', aliases=['cg', 'closest', 'near'], help='show closest geographies by distance'
        )
        nearest_parser.add_argument('display_label', help='the exact place name')
        nearest_parser.add_argument('-f', '--geofilter', help='filter by criteria')
        nearest_parser.add_argument('-c', '--context', help='group of geographies to display')
        nearest_parser.add_argument('-n', type=int, default=15, help='number of rows to display')
        nearest_parser.set_defaults(func=self.closest_geographies)

        dist_parser = query_subparsers.add_parser(
            'distance', aliases=['d', 'dist'], help='distance between two places'
        )
        dist_parser.add_argument('display_label_1', help='first place')
        dist_parser.add_argument('display_label_2', help='second place')
        dist_parser.add_argument('-k', '--kilometers', action='store_true', help='display result in kilometers')
        dist_parser.set_defaults(func=self.distance)

        resolve_parser = subparsers.add_parser(
            'resolve', aliases=['key', 'id'], help='resolve a place string to canonical IDs'
        )
        resolve_parser.add_argument('query', help='input place string to resolve')
        resolve_parser.add_argument('--state', help='optional state filter, e.g. ca')
        resolve_parser.add_argument('--sumlevel', help='optional summary level filter, e.g. 160')
        resolve_parser.add_argument('--population', type=int, help='optional population hint')
        resolve_parser.add_argument('-n', type=int, default=5, help='number of matches to return')
        resolve_parser.set_defaults(func=self.resolve_geography)

        export_parser = subparsers.add_parser(
            'export', aliases=['tocsv', 'csv', 'e', 't'], help='export data as CSV'
        )
        export_subparsers = export_parser.add_subparsers(
            help='enter geodata export <command> -h for details',
            dest='export_command',
            required=True,
        )

        export_rows_parser = export_subparsers.add_parser('rows', help='export multiple rows to CSV')
        export_rows_parser.add_argument('comps', help='components or compounds to output')
        export_rows_parser.add_argument('-f', '--geofilter', help='filter by criteria')
        export_rows_parser.add_argument('-c', '--context', help='group of geographies')
        export_rows_parser.add_argument('-n', type=int, default=0, help='number of rows to display (0 = all)')
        export_rows_parser.set_defaults(func=self.rows)

        export_profile_parser = export_subparsers.add_parser(
            'profile', aliases=['dp'], help='export one demographic profile to CSV'
        )
        export_profile_parser.add_argument('display_label', help='the exact place name')
        export_profile_parser.set_defaults(func=self.get_csv_dp)

        # Legacy top-level aliases for older workflows
        legacy_search_parser = subparsers.add_parser('search', help='legacy alias for query search')
        legacy_search_parser.add_argument('query', help='search query')
        legacy_search_parser.add_argument('-n', type=int, default=15, help='number of results to display')
        legacy_search_parser.set_defaults(func=self.display_label_search)

        args = parser.parse_args()
        args.func(args)

    def _add_rank_args(self, parser):
        parser.add_argument('comp', help='the component you want to rank')
        parser.add_argument('-d', '--data_type', choices=['c', 'cc'], help='c: component; cc: compound')
        parser.add_argument('-f', '--geofilter', help='filter by criteria')
        parser.add_argument('-c', '--context', help='group of geographies to display')
        parser.add_argument('-n', type=int, default=15, help='number of rows to display')

    def create_data_products(self, args):
        self.engine.create_data_products(args.path)

    def display_label_search(self, args):
        search_results = self.engine.display_label_search(**vars(args))

        def print_search_divider():
            return '-' * 68

        def print_search_result(dpi):
            iam = ' '
            out_str = iam + getattr(dpi, 'name').ljust(45)[:45] + iam + getattr(dpi, 'fc')['population'].rjust(20)
            return out_str

        print(print_search_divider())
        iam = ' '
        print(iam + 'Search results'.ljust(45)[:45] + iam + 'Total population'.rjust(20))
        print(print_search_divider())
        for dpi_instance in search_results[: args.n]:
            print(print_search_result(dpi_instance))
        print(print_search_divider())

    def get_dp(self, args):
        dp_list = self.engine.get_dp(**vars(args))
        if len(dp_list) == 0:
            print('Sorry, there is no geography with that name.')
            return
        print(dp_list[0])

    def resolve_geography(self, args):
        matches = self.engine.resolve_geography(**vars(args))
        if len(matches) == 0:
            print('No matches found.')
            return

        print('-' * 96)
        print(' Canonical ID'.ljust(38), 'Summary Level'.ljust(15), 'State'.ljust(7), 'Population'.rjust(12), ' Name')
        print('-' * 96)
        for match in matches:
            pop = match.get('population')
            pop_display = '' if pop is None else f'{int(pop):,}'
            print(
                f" {match['canonical_id'][:36].ljust(38)}"
                f" {match['sumlevel'].ljust(15)}"
                f" {match['state'].ljust(7)}"
                f" {pop_display.rjust(12)}"
                f" {match['name']}"
            )
        print('-' * 96)

    def compare_geovectors(self, args, mode='std'):
        closest_gvs = self.engine.compare_geovectors(**vars(args), mode=mode)

        if len(closest_gvs) == 0:
            print('Sorry, no GeoVectors match your criteria.')
            return

        comparison_gv = closest_gvs[0]
        width = 105 if mode == 'std' else 85

        print('The most demographically similar geographies are:')
        print()
        print('-' * width)
        if mode == 'std':
            print(' Geography'.ljust(41), 'County'.ljust(20), 'PDN', 'PCI', 'WHT', 'BLK', 'ASN', 'HPL', 'BDH', 'GDH', ' Distance')
        else:
            print(' Geography'.ljust(41), 'County'.ljust(20), 'PDN', 'PCI', 'MYS', ' Distance')
        print('-' * width)

        for closest_pv in closest_gvs:
            print('', closest_pv.display_row(mode), round(comparison_gv.distance(closest_pv, mode=mode), 2))

        print('-' * width)

    def compare_geovectors_app(self, args):
        self.compare_geovectors(args, mode='app')

    def extreme_values(self, args, lowest=False):
        evs = self.engine.extreme_values(**vars(args), lowest=lowest)
        if len(evs) == 0:
            print('Sorry, no geographies match your criteria.')
            return

        fetch_one = evs[0]
        _, print_ = self.engine.get_data_types(args.comp, args.data_type, fetch_one)
        iam = ' '

        def divider(dpi):
            return '-' * (68 if args.comp == 'population' else 89)

        def ev_print_headers(comp, universe_sl, group_sl, group):
            if universe_sl == '040':
                universe = 'State'
            elif universe_sl == '050':
                universe = 'County'
            elif universe_sl == '160':
                universe = 'Place'
            elif universe_sl == '310':
                universe = 'Metro/micro area'
            elif universe_sl == '400':
                universe = 'Urban area'
            elif universe_sl == '860':
                universe = 'ZCTA'
            else:
                universe = 'Geography'

            if group:
                if group_sl == '040':
                    group_name = self.st.get_name(group)
                elif group_sl == '050':
                    key = 'us:' + group + '/county'
                    group_name = self.kt.key_to_county_name[key]
                elif group_sl == '860':
                    group_name = group
                else:
                    group_name = group
                out_str = iam + (universe + ' in ' + group_name).ljust(45)[:45] + iam + getattr(dpi, 'rh')['population'].rjust(20)
            else:
                out_str = iam + universe.ljust(45)[:45] + iam + getattr(dpi, 'rh')['population'].rjust(20)

            if comp != 'population':
                out_str += iam + getattr(dpi, 'rh')[comp].rjust(20)[:20]

            return out_str

        def ev_print_row(dpi):
            out_str = iam + getattr(dpi, 'name').ljust(45)[:45] + iam + getattr(dpi, 'fc')['population'].rjust(20)
            if args.comp != 'population':
                out_str += iam + getattr(dpi, print_)[args.comp].rjust(20)[:20]
            return out_str

        universe_sl, group_sl, group = self.slt.unpack_context(args.context)
        dpi = self.engine.get_data_products()['demographicprofiles'][0]
        print(divider(dpi))
        print(ev_print_headers(args.comp, universe_sl, group_sl, group))
        print(divider(dpi))
        for ev in evs[: args.n]:
            print(ev_print_row(ev))
        print(divider(dpi))

    def lowest_values(self, args):
        self.extreme_values(args, lowest=True)

    def closest_geographies(self, args):
        cgs = self.engine.closest_geographies(**vars(args))
        iam = ' '

        def divider():
            return '-' * 68

        def cg_print_headers(universe_sl, group_sl, group):
            if universe_sl == '040':
                universe = 'State'
            elif universe_sl == '050':
                universe = 'County'
            elif universe_sl == '160':
                universe = 'Place'
            elif universe_sl == '310':
                universe = 'Metro/micro area'
            elif universe_sl == '400':
                universe = 'Urban area'
            elif universe_sl == '860':
                universe = 'ZCTA'
            else:
                universe = 'Geography'

            if group:
                if group_sl == '040':
                    group_name = self.st.get_name(group)
                elif group_sl == '050':
                    key = 'us:' + group + '/county'
                    group_name = self.kt.key_to_county_name[key]
                elif group_sl == '860':
                    group_name = group
                else:
                    group_name = group
                out_str = iam + (universe + ' in ' + group_name).ljust(45)[:45] + iam + 'Distance (mi)'.rjust(20)
            else:
                out_str = iam + universe.ljust(45)[:45] + iam + 'Distance (mi)'.rjust(20)

            return out_str

        def cg_print_row(dpi, distance):
            return iam + getattr(dpi, 'name').ljust(45)[:45] + iam + str(round(distance, 1)).rjust(20)

        universe_sl, group_sl, group = self.slt.unpack_context(args.context)

        if len(cgs) == 0:
            print('Sorry, no geographies match your criteria.')
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
        self.engine.rows(**vars(args))

    def get_csv_dp(self, args):
        self.engine.get_csv_dp(**vars(args))


def main():
    GeodataCLI()


if __name__ == '__main__':
    main()
