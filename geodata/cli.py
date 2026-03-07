import argparse
from engine import Engine

class GeodataCLI:
    def _add_common_filters(self, parser, include_context=True):
        parser.add_argument('-f', '--geofilter', help='filter criteria (e.g., population:gt:100000)')
        if include_context:
            parser.add_argument('-c', '--context', help='context scope (e.g., 160, 160in040:ca)')

    def _add_n_arg(self, parser, default=15, label='number of rows to display'):
        parser.add_argument('-n', type=int, default=default, help=label)

    def __init__(self):
        self.engine = Engine()

        self.ct = self.engine.ct
        self.st = self.engine.st
        self.kt = self.engine.kt
        self.slt = self.engine.slt

        #######################################################################
        # Argument parsing with argparse

        # Create the top-level argument parser
        parser = argparse.ArgumentParser(
            description='Explore and compare geography data from your local data products.',
            epilog='Examples: geodata build ~/data  |  geodata search "san francisco"  |  geodata view distance "San Francisco city, California" "San Jose city, California"',
            prog='geodata')
        # Create top-level subparsers
        subparsers = parser.add_subparsers(
            help='enter geodata <command> -h for details',
            dest='command',
            required=True)

        # Top-level subparser
        # Create the parsor for the "createdb" command
        createdb_parser = subparsers.add_parser(
            'createdb',
            aliases=['build', 'ingest', 'c'],
            help='build data products from source files',
            description='Build data products from source files and write both SQLite and legacy pickle outputs.'
        )
        createdb_parser.add_argument('path', help='path to data files')
        createdb_parser.set_defaults(func=self.create_data_products)

        # Create the parser for the "view" command
        view_parsers = subparsers.add_parser(
            'view',
            aliases=['show', 'v'],
            help='view data and comparisons',
        )
        # Create subparsers for the "view" command
        view_subparsers = view_parsers.add_subparsers(
            help='enter geodata view <command> -h for details',
            dest='view_command',
            required=True)

        # Create the parser for the "search" command
        search_parser = subparsers.add_parser(
            'search',
            aliases=['find', 'lookup', 's'],
            help='search place names',
            description='Search for a display label (place name)'
        )
        search_parser.add_argument('query', help='search query')
        self._add_n_arg(search_parser, default=15, label='number of results to display')
        search_parser.set_defaults(func=self.display_label_search)

        # Resolve command (identity/key resolution)
        resolve_parser = subparsers.add_parser(
            'resolve',
            aliases=['key', 'id'],
            help='resolve a place string to canonical IDs',
            description='Resolve an input place string to likely canonical geography identifiers.',
        )
        resolve_parser.add_argument('query', help='input place string to resolve')
        resolve_parser.add_argument('--state', help='optional 2-letter lowercase state filter (e.g., ca)')
        resolve_parser.add_argument('--sumlevel', help='optional summary level filter (e.g., 160)')
        resolve_parser.add_argument('--population', type=int, help='optional population hint')
        self._add_n_arg(resolve_parser, default=5, label='number of matches to return')
        resolve_parser.set_defaults(func=self.resolve_geography)

        # Create the parser for the "tocsv" command
        tocsv_parser = subparsers.add_parser(
            'tocsv',
            aliases=['csv', 'export', 't'],
            help='export data as CSV',
            description='Output data in CSV format'
        )
        tocsv_subparsers = tocsv_parser.add_subparsers(
            help='enter geodata tocsv <command> -h for details',
            dest='tocsv_command',
            required=True)

        # View subparser
        # Create parsors for the view command
        # DemographicProfiles #################################################
        dp_parsor = view_subparsers.add_parser('dp', aliases=['profile'],
            help='show one demographic profile',
            description='View a DemographicProfile.')
        dp_parsor.add_argument('display_label', help='the exact place name')
        dp_parsor.set_defaults(func=self.get_dp)

        # GeoVectors [standard mode] ##########################################
        gv_parsor = view_subparsers.add_parser('gv', aliases=['similar'],
            help='show nearest geovectors',
            description='View GeoVectors nearest to a GeoVector.')
        gv_parsor.add_argument('display_label', help='the exact place name')
        gv_parsor.add_argument('-c', '--context', help='geographies to compare with')
        self._add_n_arg(gv_parsor, default=15)
        gv_parsor.set_defaults(func=self.compare_geovectors)

        # GeoVectors [appearance mode] ########################################
        gva_parsor = view_subparsers.add_parser('gva', aliases=['similar-app'],
            help='show nearest geovectors (appearance mode)',
            description='View GeoVectors nearest to a GeoVector [appearance mode]')
        gva_parsor.add_argument('display_label', help='the exact place name')
        gva_parsor.add_argument('-c', '--context', help='geographies to compare with')
        self._add_n_arg(gva_parsor, default=15)
        gva_parsor.set_defaults(func=self.compare_geovectors_app)

        # Highest values ######################################################
        hv_parsor = view_subparsers.add_parser('hv', aliases=['top', 'highest'],
            help='show highest values by component',
            description='View geographies that rank highest with regard to comp')
        hv_parsor.add_argument('comp', help='the comp that you want to rank')
        hv_parsor.add_argument('-d', '--data_type', choices=['c', 'cc'], help='c: component; cc: compound')
        self._add_common_filters(hv_parsor, include_context=True)
        self._add_n_arg(hv_parsor, default=15)
        hv_parsor.set_defaults(func=self.extreme_values)

        # Lowest values #######################################################
        lv_parsor = view_subparsers.add_parser('lv', aliases=['bottom', 'lowest'],
            help='show lowest values by component',
            description='View geographies that rank lowest with regard to comp')
        lv_parsor.add_argument('comp', help='the comp that you want to rank')
        lv_parsor.add_argument('-d', '--data_type', choices=['c', 'cc'], help='c: component; cc: compound')
        self._add_common_filters(lv_parsor, include_context=True)
        self._add_n_arg(lv_parsor, default=15)
        lv_parsor.set_defaults(func=self.lowest_values)

        # Closest geographies #################################################
        cg_parsor = view_subparsers.add_parser('cg', aliases=['near', 'closest'],
            help='show closest geographies by distance',
            description='View geographies that are closest to the one specified by display_label')
        cg_parsor.add_argument('display_label', help='the exact place name')
        self._add_common_filters(cg_parsor, include_context=True)
        self._add_n_arg(cg_parsor, default=15)
        cg_parsor.set_defaults(func=self.closest_geographies)

        # Distance ############################################################
        d_parsor = view_subparsers.add_parser('d', aliases=['distance', 'dist'],
            help='distance between two places',
            description='Get the distance between two places')
        d_parsor.add_argument('display_label_1', help='Get the distance between this display label and...')
        d_parsor.add_argument('display_label_2', help='...this one.')
        d_parsor.add_argument('-k', '--kilometers', action='store_true', help='Display result in kilometers.')
        d_parsor.set_defaults(func=self.distance)

        # tocsv subparsers
        # Create parsors for the tocsv command
        # Rows ################################################################
        rows_parsor = tocsv_subparsers.add_parser('rows',
            help='export multiple rows to CSV',
            description='Output data rows in CSV format')
        rows_parsor.add_argument('comps', help='components or compounds to output')
        self._add_common_filters(rows_parsor, include_context=True)
        self._add_n_arg(rows_parsor, default=0, label='number of rows to display (0 = all)')
        rows_parsor.set_defaults(func=self.rows)

        # DemographicProfile ##################################################
        csv_dp_parsor = tocsv_subparsers.add_parser('dp', aliases=['profile'],
            help='export one demographic profile to CSV',
            description='Output a DemographicProfile in CSV format')
        csv_dp_parsor.add_argument('display_label', help='the exact place name')
        csv_dp_parsor.set_defaults(func=self.get_csv_dp)

        # Parse arguments
        args = parser.parse_args()
        args.func(args)

    def create_data_products(self, args):
        self.engine.create_data_products(args.path)

    def display_label_search(self, args):
        search_results = self.engine.display_label_search(**vars(args))

        def print_search_divider():
            return '-' * 68

        def print_search_result(dpi):
            '''Print a row for search results.'''
            iam = ' '

            out_str = iam + getattr(dpi, 'name').ljust(45)[:45] + iam \
                        + getattr(dpi, 'fc')['population'].rjust(20)
            return out_str

        print(print_search_divider())

        iam = ' '
        
        print(iam + 'Search results'.ljust(45)[:45] + iam + \
            'Total population'.rjust(20))

        print(print_search_divider())

        for dpi_instance in search_results[:args.n]:
            print(print_search_result(dpi_instance))

        print(print_search_divider())

    def get_dp(self, args):
        dp_list = self.engine.get_dp(**vars(args))
        if len(dp_list) == 0:
            print("Sorry, there is no geography with that name.")
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
            print("Sorry, no GeoVectors match your criteria.")
        else:
            comparison_gv = closest_gvs[0]
            if mode == 'std':
                width = 105
            elif mode == 'app':
                width = 85

            print("The most demographically similar geographies are:")
            print()
            print('-' * width)
            if mode == 'std':
                print(' Geography'.ljust(41), 'County'.ljust(20), 'PDN', 'PCI', 'WHT', 'BLK', 'ASN', 'HPL', 'BDH', 'GDH', ' Distance')
            elif mode == 'app':
                print(' Geography'.ljust(41), 'County'.ljust(20), 'PDN', 'PCI', 'MYS', ' Distance')
            print('-' * width)

            # Print these GeoVectors
            for closest_pv in closest_gvs:
                print('', closest_pv.display_row(mode),
                    round(comparison_gv.distance(closest_pv, mode=mode), 2))

            print('-' * width)

    def compare_geovectors_app(self, args):
        self.compare_geovectors(args, mode='app')

    def extreme_values(self, args, lowest=False):
        evs = self.engine.extreme_values(**vars(args), lowest=lowest)
        fetch_one = evs[0]

        sort_by, print_ = self.engine.get_data_types(args.comp, args.data_type, fetch_one)

        # helper methods for printing [hl]v rows ##############################

        # The inter-area margin to divide display sections
        iam = ' '

        def divider(dpi):
            '''Print a divider for DemographicProfiles'''
            if args.comp == 'population':
                return '-' * 68
            else:
                return '-' * 89

        def ev_print_headers(comp, universe_sl, group_sl, group):
            '''Helper method to DRY up sl_print_headers'''

            # Set the name of the universe
            if universe_sl:
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
                group_name = ''

                if group_sl == '040':
                    group_name = self.st.get_name(group)
                elif group_sl == '050':
                    key = 'us:' + group + '/county'
                    group_name = self.kt.key_to_county_name[key]
                elif group_sl == '860':
                    group_name = group
                
                # Output '<UNIVERSE GEOGRAPHY> in <GROUP NAME>'
                out_str = iam + (universe + ' in ' \
                    + group_name).ljust(45)[:45] + iam \
                    + getattr(dpi, 'rh')['population'].rjust(20)
            else:
                out_str = iam + universe.ljust(45)[:45] + iam \
                    + getattr(dpi, 'rh')['population'].rjust(20)

            # Print another column if the comp isn't population
            if args.comp != 'population':
                out_str += iam + getattr(dpi, 'rh')[args.comp].rjust(20)[:20]

            return out_str

        # dpi = demographicprofile_instance
        def ev_print_row(dpi):
            '''Print a data row for DemographicProfiles'''
            out_str = iam + getattr(dpi, 'name').ljust(45)[:45] + iam \
                    + getattr(dpi, 'fc')['population'].rjust(20)
            if args.comp != 'population':
                out_str += iam + getattr(dpi, print_)[args.comp].rjust(20)[:20]
            return out_str

        # Printing ############################################################

        universe_sl, group_sl, group = self.slt.unpack_context(args.context)

        if len(evs) == 0:
            print("Sorry, no geographies match your criteria.")
        else:
            # Print the header and places with their information.
            dpi = self.engine.get_data_products()['demographicprofiles'][0]
            print(divider(dpi))
            print(ev_print_headers(args.comp, universe_sl, group_sl, group))
            print(divider(dpi))
            for ev in evs[:args.n]:
                print(ev_print_row(ev))
            print(divider(dpi))
    
    def lowest_values(self, args):
        self.extreme_values(args, lowest=True)

    def closest_geographies(self, args):
        cgs = self.engine.closest_geographies(**vars(args))

        # Helper methods for printing cg rows #################################

        # The inter-area margin to divide display sections
        iam = ' '

        def divider():
            '''Print a divider for DemographicProfiles'''
            return '-' * 68

        def cg_print_headers(universe_sl, group_sl, group):
            '''Helper method to DRY up sl_print_headers'''

            # Set the name of the universe
            if universe_sl:
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
                group_name = ''

                if group_sl == '040':
                    group_name = self.st.get_name(group)
                elif group_sl == '050':
                    key = 'us:' + group + '/county'
                    group_name = self.kt.key_to_county_name[key]
                elif group_sl == '860':
                    group_name = group
                
                # Output '<UNIVERSE GEOGRAPHY> in <GROUP NAME>'
                out_str = iam + (universe + ' in ' \
                    + group_name).ljust(45)[:45] + iam \
                    + 'Distance (mi)'.rjust(20)
            else:
                out_str = iam + universe.ljust(45)[:45] + iam \
                    + 'Distance (mi)'.rjust(20)

            return out_str

        # dpi = demographicprofile_instance
        def cg_print_row(dpi, distance):
            '''Print a data row for DemographicProfiles'''
            out_str = iam + getattr(dpi, 'name').ljust(45)[:45] + iam \
                    + str(round(distance, 1)).rjust(20)
            return out_str

        # Printing ############################################################

        universe_sl, group_sl, group = self.slt.unpack_context(args.context)

        if len(cgs) == 0:
            print("Sorry, no geographies match your criteria.")
        else:
            # Print the header and places with their information.
            print(divider())
            print(cg_print_headers(universe_sl, group_sl, group))
            print(divider())
            for cg in cgs[:args.n]:
                print(cg_print_row(*cg))
            print(divider())

    def distance(self, args):
        print(self.engine.distance(**vars(args)))

    def rows(self, args):
        self.engine.rows(**vars(args))

    def get_csv_dp(self, args):
        self.engine.get_csv_dp(**vars(args))

gcli = GeodataCLI()
