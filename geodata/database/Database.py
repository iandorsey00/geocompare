import pandas as pd
import numpy as np

try:
    from geodata.datainterface.DemographicProfile import DemographicProfile
    from geodata.datainterface.GeoVector import GeoVector
    from geodata.tools.geodata_typecast import gdt, gdti, gdtf
    from geodata.tools.StateTools import StateTools
except ImportError:  # pragma: no cover - script execution fallback
    from datainterface.DemographicProfile import DemographicProfile
    from datainterface.GeoVector import GeoVector
    from tools.geodata_typecast import gdt, gdti, gdtf
    from tools.StateTools import StateTools
# from initialize_sqlalchemy import Base, engine, session

from itertools import islice
import sqlite3
import csv

from collections import defaultdict
from pathlib import Path
import sys

class Database:
    '''Creates data products for use by geodata.'''
    ###########################################################################
    # Helper methods for __init__

    def get_tm_columns(self, path):
        '''Obtain columns for table_metadata'''
        columns = list(pd.read_csv(path / 'ACS_5yr_Seq_Table_Number_Lookup.txt',
            nrows=1, dtype='str').columns)

        # Convert column headers to snake_case
        columns = list(map(lambda x: x.lower(), columns))
        columns = list(map(lambda x: x.replace(' ', '_'), columns))

        return columns

    def get_gh_columns(self, gh_year, path):
        '''Obtain columns for the geoheaders table.'''
        return list(pd.read_csv(path / f'{gh_year}_Gaz_place_national.txt',
            sep='\t', nrows=1, dtype='str').columns)

    def get_state_gazetteer_path(self, gh_year, path):
        '''Resolve the state gazetteer file path with backward compatibility.'''
        candidates = [
            path / f'{gh_year}_Gaz_state_national.txt',
            path / '2019_Gaz_state_national.txt',
        ]

        for candidate in candidates:
            if candidate.exists():
                return candidate

        candidate_list = ', '.join(str(p.name) for p in candidates)
        raise FileNotFoundError(
            f'Unable to find a state gazetteer file. Expected one of: {candidate_list}'
        )

    def dbapi_qm_substr(self, columns_len):
        '''Get the DBAPI question mark substring'''
        return ', '.join(['?'] * columns_len)

    def dbapi_update_qm_substr(self, columns_len):
        '''Get the DBAPI question mark substring for UPDATE stmts'''
        return ', '.join(['? = ?'] * columns_len)

    # ido = id_offset: Set it to one if there is an id that columns should
    # ignore. Otherwise, if there is no seperate id column, set it 0.
    def create_table(self, table_name, columns, column_defs, rows, ido=1):
        '''Create a table for use by geodata.'''
        # DBAPI question mark substring
        columns_len = len(column_defs) - ido
        question_mark_substr = self.dbapi_qm_substr(columns_len)

        # CREATE TABLE statement
        self.c.execute('''CREATE TABLE %s
                          (%s)''' % (table_name, ', '.join(column_defs)))

        # Insert rows into table
        self.c.executemany('INSERT INTO %s(%s) VALUES (%s)' % (
            table_name, ', '.join(columns), question_mark_substr), rows)

    def debug_output_table(self, table_name):
        '''Print debug information for a table'''
        print('%s table:' % table_name, '\n')
        for row in self.c.execute('SELECT * FROM %s LIMIT 5' % table_name):
            print(row)
        print()

    def debug_output_list(self, list_name):
        '''Print debug information for a list'''
        print('%s:' % list_name, '\n')
        for row in getattr(self, list_name)[:5]:
            print(row)
        print()

    def take(self, n, iterable):
        '''Return first n items of the iterable as a list'''
        return list(islice(iterable, n))

    def debug_output_dict(self, dict_name):
        '''Print debug information for a dictionary'''
        print('%s:' % dict_name, '\n')
        for key, value in self.take(5, getattr(self, dict_name).items()):
            print(key + ':', value)
        print()

    def get_geo_csv_rows(self):
        '''Get all rows geographic CSV files for every state'''
        # Get rows from CSV
        rows = []
        # Get rows from CSV files for each state.
        for state in self.st.get_abbrevs(lowercase=True):
            this_path = self.data_dir / f'g{self.year}5{state}.csv'
            
            with open(this_path, 'rt', encoding='iso-8859-1') as f:
                rows += list(csv.reader(f))

        # Also, get rows for the national file (for ZCTA support).
        this_path = self.data_dir / f'g{self.year}5us.csv'
        with open(this_path, 'rt', encoding='iso-8859-1') as f:
            rows += list(csv.reader(f))
        
        return rows
    
    ###########################################################################
    # __init__

    def __init__(self, path):
        '''Create the database'''
        # Initialize ##########################################################

        self.data_dir = Path(path).expanduser().resolve()
        self.year = '2020'
        self.gh_year = '2023'

        self.st = StateTools()

        # Connect to SQLite3
        self.conn = sqlite3.connect(':memory:')
        self.c = self.conn.cursor()

        # table_metadata ######################################################
        this_table_name = 'table_metadata'

        # Process column definitions
        columns = self.get_tm_columns(self.data_dir)
        column_defs = list(map(lambda x: x + ' TEXT', columns))
        column_defs.insert(0, 'id INTEGER PRIMARY KEY')

        # Get rows from CSV
        this_path = self.data_dir / 'ACS_5yr_Seq_Table_Number_Lookup.txt'
        rows = []

        with open(this_path, 'rt') as f:
            rows = list(csv.reader(f))

        # Create table
        self.create_table(this_table_name, columns, column_defs, rows)

        # Debug output
        self.debug_output_table(this_table_name)

        # geographies #########################################################
        this_table_name = 'geographies'

        # Process column definitions
        columns = [
            'STUSAB',
            'SUMLEVEL',
            'LOGRECNO',
            'STATE',
            'GEOID',
            'NAME',
            ]
        self.geographies_columns = columns
        column_defs = list(map(lambda x: x + ' TEXT', columns))
        column_defs.insert(0, 'id INTEGER PRIMARY KEY')

        # Get rows from CSV
        rows = self.get_geo_csv_rows()
                
        # Filter for summary levels
        # 040 = State
        # 050 = State-County
        # 160 = State-Place
        # 310 = Metro/Micro Area
        # 400 = Urban Area
        # 860 = ZCTA
        rows = list(filter(lambda x: 
                           (x[2] == '160' \
                        or x[2] == '050' \
                        or x[2] == '040' \
                        or x[2] == '860' \
                        or x[2] == '310' \
                        or x[2] == '400')
                        and ''.join(x[48][3:5]) == '00' ,
                           rows))
        rows = list(
                    map(lambda x:
                        [x[1].lower(),             # STUSAB [lowercase]
                        x[2],                      # SUMLEVEL
                        x[4],                      # LOGRECNO
                        self.st.get_state(x[49]),  # STATE
                        x[48],                     # GEOID
                        x[49]],                    # NAME
                        rows
                    )
                )

        # DBAPI question mark substring
        columns_len = len(column_defs)
        question_mark_substr = ', '.join(['?'] * columns_len)

        # Create table
        self.create_table(this_table_name, columns, column_defs, rows)

        # Debug output
        self.debug_output_table(this_table_name)

        # geoheaders ##########################################################
        this_table_name = 'geoheaders'

        # The primary reason we are interested in the 2019 National Gazetteer
        # is that we need to get the land area so that we can calculate
        # population and housing unit densities.

        columns = self.get_gh_columns(self.gh_year, self.data_dir)
        columns[-1] = columns[-1].strip()
        self.geoheaders_columns = columns
        column_defs = list(map(lambda x: x + ' TEXT', columns))
        column_defs.insert(0, 'id INTEGER PRIMARY KEY')

        # Get rows for places (160) from CSV
        this_path = self.data_dir / f'{self.gh_year}_Gaz_place_national.txt'
        rows = []

        with open(this_path, 'rt') as f:
            rows = list(csv.reader(f, delimiter='\t'))

        # Get rows for counties (050) from CSV
        this_path = self.data_dir / f'{self.gh_year}_Gaz_counties_national.txt'

        with open(this_path, 'rt') as f:
            c_rows = list(csv.reader(f, delimiter='\t'))

        # County geoheaders lack two columns that places have, so insert
        # them as empty strings.
        for idx, c_row in enumerate(c_rows):
            c_row.insert(4, '')
            c_row.insert(5, '')
        
        # Get rows for states (040) from CSV
        this_path = self.get_state_gazetteer_path(self.gh_year, self.data_dir)

        with open(this_path, 'rt') as f:
            s_rows = list(csv.reader(f, delimiter='\t'))

        # Get rows for Metro/micro areas (310) from CSV
        this_path = self.data_dir / f'{self.gh_year}_Gaz_cbsa_national.txt'

        with open(this_path, 'rt') as f:
            cbsa_rows = list(csv.reader(f, delimiter='\t'))

        # Get rows for urban areas (400) from CSV
        this_path = self.data_dir / f'{self.gh_year}_Gaz_ua_national.txt'

        with open(this_path, 'rt') as f:
            ua_rows = list(csv.reader(f, delimiter='\t'))

        # UA insert blank columns so that the number of columns match.
        for ua_row in ua_rows:
            ua_row.insert(0, 'US') # The state abbrev for all UAs in geos tbl
            ua_row.insert(2, '')
            ua_row.insert(5, '')

        # Get rows for ZCTAs (860) from CSV
        this_path = self.data_dir / f'{self.gh_year}_Gaz_zcta_national.txt'

        with open(this_path, 'rt') as f:
            z_rows = list(csv.reader(f, delimiter='\t'))

        # ZCTA insert blank columns so that the number of columns match.
        for z_row in z_rows:
            z_row.insert(0, 'US') # The state abbrev for all ZCTAs in geos tbl
            z_row.insert(2, '')
            z_row.insert(3, '')
            z_row.insert(4, '')
            z_row.insert(5, '')

        # Metro/micro area insert blank columns so that the number of columns
        # match.
        for cbsa_row in cbsa_rows:
            del(cbsa_row[0])
            cbsa_row.insert(0, 'US') # The state abb. for all metro/micro areas
            cbsa_row.insert(2, '')
            cbsa_row.insert(5, '')

        def complete_geoids(sumlev_code, rows):
            for row in rows:
                row[1] = sumlev_code + '00US' + row[1]

        # Complete GEOIDs

        complete_geoids('160', rows)
        complete_geoids('040', s_rows)
        complete_geoids('050', c_rows)
        complete_geoids('310', cbsa_rows)
        complete_geoids('400', ua_rows)
        complete_geoids('860', z_rows)

        # Merge rows together
        rows = rows + c_rows + s_rows + z_rows + ua_rows + cbsa_rows

        for row in rows:
            row[-1] = row[-1].strip()

        print(columns)
        print(column_defs)

        # Create table
        self.create_table(this_table_name, columns, column_defs, rows)

        # Debug output
        self.debug_output_table(this_table_name)

        # Specify what data we need ###########################################

        # Specify table_ids and line numbers that have the data we need.
        # See data/ACS_5yr_Seq_Table_Number_Lookup.txt
        if self.year == '2018':
            self.line_numbers_dict = {
                'B01003': ['1'],   # TOTAL POPULATION
                'B19301': ['1'],   # PER CAPITA INCOME IN THE PAST 12 MONTHS (IN
                                # 2018 INFLATION-ADJUSTED DOLLARS)
                'B02001': ['2',    # RACE - White alone
                        '3',    # RACE - Black or African American alone
                        '5'],   # RACE - Asian alone
                'B03002': ['3',    # HISPANIC OR LATINO ORIGIN BY RACE - Not
                                # Hispanic or Latino - White alone
                        '12'],  # ↑ - Hispanic or Latino
                'B04004': ['51'],  # PEOPLE REPORTING SINGLE ANCESTRY - Italian
                # EDUCATIONAL ATTAINMENT FOR THE POPULATION 25 YEARS AND OVER
                'B15003': ['1',    # Total:
                        '22',   # Bachelor's degree
                        '23',   # Master's degree
                        '24',   # Professional school degree
                        '25'],  # Doctorate degree
                # MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2018 INFLATION-ADJUSTED DOLLARS)
                'B19013': ['1'],   # ↑
                'B25035': ['1'],   # Median year structure built
                'B25018': ['1'],   # Median number of rooms
                'B25058': ['1'],   # Median contract rent (of renter-occupied
                                # housing units)
                'B25077': ['1'],   # Median value (of owner-occupied housing units)
            }
        elif self.year == '2020':
            self.line_numbers_dict = {
                'B01003': ['1'],   # TOTAL POPULATION
                'B19301': ['1'],   # PER CAPITA INCOME IN THE PAST 12 MONTHS (IN
                                # 2018 INFLATION-ADJUSTED DOLLARS)
                'B02001': ['2',    # RACE - White alone
                        '3',    # RACE - Black or African American alone
                        '5'],   # RACE - Asian alone
                'B03002': ['3',    # HISPANIC OR LATINO ORIGIN BY RACE - Not
                                # Hispanic or Latino - White alone
                        '12'],  # ↑ - Hispanic or Latino
                'B04004': ['51'],  # PEOPLE REPORTING SINGLE ANCESTRY - Italian
                # EDUCATIONAL ATTAINMENT FOR THE POPULATION 25 YEARS AND OVER
                'B15003': ['1',    # Total:
                        '22',   # Bachelor's degree
                        '23',   # Master's degree
                        '24',   # Professional school degree
                        '25'],  # Doctorate degree
                # MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2018 INFLATION-ADJUSTED DOLLARS)
                'B19013': ['1'],   # ↑
                'B25035': ['1'],   # Median year structure built
                'B25018': ['1'],   # Median number of rooms
                'B25058': ['1'],   # Median contract rent (of renter-occupied
                                # housing units)
                'B25077': ['1'],   # Median value (of owner-occupied housing units)
            }

        # Get needed table metadata.
        self.table_metadata = []

        for table_id, line_numbers in self.line_numbers_dict.items():
            self.table_metadata += self.c.execute('''SELECT * FROM table_metadata
                WHERE table_id = ? AND (line_number IN (%s) OR line_number = '')''' % (
                self.dbapi_qm_substr(len(line_numbers)) ),
                [table_id] + line_numbers)

        self.debug_output_list('table_metadata')

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

        self.debug_output_dict('sequence_numbers')

        # Obtain needed files                                             #####
        self.files = dict()

        for table_id, sequence_numbers in self.sequence_numbers.items():
            if table_id not in self.files.keys():
                self.files[table_id] = []

            for sequence_number in sequence_numbers:
                for state in self.st.get_abbrevs(lowercase=True, inc_us=True):
                    this_path = self.data_dir / f'e{self.year}5{state}{sequence_number}000.txt'
                    self.files[table_id].append(this_path)
        
        self.debug_output_dict('files')

        # Obtain needed positions                                         #####
        self.positions = dict()
        last_start_position = ''
        last_line_number = ''

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
                self.positions[table_id].append(last_start_position\
                     + last_line_number - 1)
        
        self.debug_output_dict('positions')

        # Obtain needed data_identifiers                                  #####
        self.data_identifiers = dict()
        self.data_identifiers_list = ['STATE', 'LOGRECNO']

        for table_id, line_numbers in self.line_numbers_dict.items():
            # If there is no such key, start with 'LOGRECNO'
            if table_id not in self.data_identifiers.keys():
                self.data_identifiers[table_id] = \
                    ['STATE', 'LOGRECNO']

            # Add the data_identifiers.
            # Format: <table_id>_<line_number>
            for line_number in line_numbers:
                this_data_identifier = table_id + '_' + line_number
                self.data_identifiers[table_id].append(this_data_identifier)
                self.data_identifiers_list.append(this_data_identifier)

        self.debug_output_dict('data_identifiers')
        self.debug_output_list('data_identifiers_list')

        # data ################################################################
        this_table_name = 'data'

        print('Processing data table. This might take a while.')
        print()

        columns = self.data_identifiers_list
        self.data_columns = columns
        column_defs = list(map(lambda x: x + ' TEXT', columns))
        column_defs.append('PRIMARY KEY(STATE, LOGRECNO)')

        # CREATE TABLE statement
        self.c.execute('''CREATE TABLE %s
                          (%s)''' % (this_table_name, ', '.join(column_defs)))

        # Map indices (idx) to elements from list
        def idx_map(idxs, list):
            ld = dict(enumerate(list))
            return [ld[i] for i in idxs]

        # Assist with changing the order of the elements around for the
        # INSERT statement below.
        def flip_els(rows):
            return list(
                map(
                    lambda x: x[2:] + x[:2], rows
                    )
                )

        # Record whether or not we're on the first statement of the function
        # below.
        first_table_id = True

        # Iterate through table_ids
        for table_id, line_numbers in self.line_numbers_dict.items():
            columns = self.data_identifiers[table_id]
            rows = []

            # Iterate through files
            for file in self.files[table_id]:
                # Read from each CSV file
                with open(file, 'rt') as f:
                    csv_rows = csv.reader(f)

                    for csv_row in csv_rows:
                        # Get elements at self.positions[table_id] for each row
                        rows.append(idx_map(self.positions[table_id], csv_row))

            if first_table_id:
                question_mark_substr = self.dbapi_qm_substr(len(columns))
                # Insert rows into table
                self.c.executemany('INSERT INTO %s(%s) VALUES (%s)' % (
                    this_table_name, ', '.join(columns),
                    question_mark_substr), rows)

                first_table_id = False
            else:
                set_clause = list(
                    map(
                        lambda x: x + ' = ?',
                        self.data_identifiers[table_id][2:]
                        )
                    )
                self.c.executemany('''UPDATE %s SET %s
                    WHERE STATE = ? AND LOGRECNO = ?''' % (
                    this_table_name, ', '.join(set_clause)), flip_els(rows))

            # Print the count for debug purposes. Should be around ~200,000
            for debug in self.c.execute('SELECT COUNT(*) FROM data'):
                display_data_identifier = table_id
                print('Processing for', display_data_identifier,
                    'complete (' + str(debug[0]), 'rows).')

        print()
        # Debug output
        self.debug_output_table(this_table_name)

        # geodata #############################################################
        this_table_name = 'geodata'

        # Combine data from places, geoheaders, and data into a single table.
        
        # Combine columns
        columns = self.geographies_columns + self.geoheaders_columns \
                  + self.data_columns
        
        # Unambiguous columns
        ub_geographies_columns = list(map(lambda x: 'geographies.' + x, self.geographies_columns))
        ub_geoheaders_columns = list(map(lambda x: 'geoheaders.' + x, self.geoheaders_columns))
        ub_data_columns = list(map(lambda x: 'data.' + x, self.data_columns))
        ub_columns = ub_geographies_columns + ub_geoheaders_columns + ub_data_columns

        # Make columns names unambigious
        def deambigify(column):
            if column in self.geographies_columns:
                return 'geographies.' + column
            elif column in self.geoheaders_columns:
                return 'geoheaders.' + column
            elif column in self.data_columns:
                return 'data.' + column

        # Remove duplicates
        columns = list(dict.fromkeys(columns))
        self.columns = columns
        ub_columns = list(map(deambigify, columns))

        # Column definitions
        column_defs = list(map(lambda x: x + ' TEXT', columns))
        column_defs.insert(0, 'id INTEGER PRIMARY KEY')

        # DBAPI question mark substring
        columns_len = len(column_defs)
        question_mark_substr = self.dbapi_qm_substr(columns_len)

        # CREATE TABLE statement
        self.c.execute('''CREATE TABLE %s
                          (%s)''' % (this_table_name, ', '.join(column_defs)))

        # Insert rows into merged table
        self.c.execute('''INSERT INTO %s(%s)
        SELECT %s FROM geographies
        JOIN geoheaders ON geographies.GEOID = geoheaders.GEOID
        JOIN data ON geographies.LOGRECNO = data.LOGRECNO AND geographies.STUSAB = data.STATE''' % (
            this_table_name, ', '.join(columns), ', '.join(ub_columns)))

        # Debug output
        self.debug_output_list('columns')
        self.debug_output_table(this_table_name)

        # Database: Apply changes #############################################

        # Commit changes
        self.conn.commit()

        # Row factory
        self.conn.row_factory = sqlite3.Row
        self.c = self.conn.cursor()

        # DemographicProfiles #################################################

        # Create a placeholder for DemographicProfiles
        self.demographicprofiles = []

        for row in self.c.execute('SELECT * from geodata'):
            try:
                self.demographicprofiles.append(DemographicProfile(row))
            except AttributeError as e:
                print('AttributeError:', e)
                print(tuple(row))

        # Debug output
        self.debug_output_list('demographicprofiles')

        # Medians and standard deviations #####################################

        # Prepare a DataFrame into which we can insert rows.
        rows = []
        for row in self.c.execute('SELECT * from geodata'):
            try: 
                rows.append([
                            gdt(row['ALAND_SQMI']),
                            gdt(row['B01003_1']),
                            gdt(row['B19301_1']),
                            gdt(row['B02001_2']),
                            gdt(row['B02001_3']),
                            gdt(row['B02001_5']),
                            gdt(row['B03002_3']),
                            gdt(row['B03002_12']),
                            gdt(row['B04004_51']),
                            gdt(row['B15003_1']),
                            gdt(row['B15003_22']),
                            gdt(row['B15003_23']),
                            gdt(row['B15003_24']),
                            gdt(row['B15003_25']),
                            gdt(row['B19013_1']),
                            gdt(row['B25018_1']),
                            gdt(row['B25035_1']),
                            gdt(row['B25058_1']),
                            gdt(row['B25077_1']),
                            ])
            except AttributeError:
                print('AttributeError:', instance)

        # print(dict(enumerate(self.columns)))
        # print([self.columns[11]] + self.columns[15:])
        df = pd.DataFrame(rows, columns=[self.columns[12]] + self.columns[16:])

        # Adjustments for better calculations of medians and
        # standard deviations, and better results for highest and lowest values

        # median_year_structure_built value of 0 were causing problems because
        # all values for available data are between 1939 and the present year.
        # Replace all 0 values with numpy.nan
        df = df.replace({'B25035_1': {0: np.nan}})

        # Print some debug information.
        print('DataFrames:', '\n')
        print(df.head())
        print()

        print('Medians:', '\n')
        medians = df.median()
        print(dict(medians))
        print()

        print('Standard deviations:', '\n')
        standard_deviations = df.std()
        print(dict(standard_deviations))
        print()

        # GeoVectors ##########################################################

        self.geovectors = []

        for row in self.c.execute('SELECT * from geodata'):
            try:
                # Construct a GeoVector and append it to self.geovectors.
                self.geovectors.append(
                    GeoVector(
                        row,
                        dict(medians),
                        dict(standard_deviations)
                    )
                )
            # If a TypeError is thrown because some data is unavailable, just
            # don't make that GeoVector and print a debugging message.
            except (TypeError, ValueError, AttributeError):
                print('Note: Inadequate data for GeoVector creation:',
                      row['NAME'])

        print()

        # Debug output
        self.debug_output_list('geovectors')

    def get_products(self):
        '''Return a dictionary of products.'''
        # Use list(set(...)) to remove duplicates
        return {
            'demographicprofiles':  self.demographicprofiles,
            'geovectors':           self.geovectors,
            }
