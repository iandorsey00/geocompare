'''
Intended to describe a specific geography or compare small numbers of
geographies.
'''

# pylint: disable=import-error
try:
    from geodata.tools.geodata_typecast import gdt, gdti, gdtf
    from geodata.tools.CountyTools import CountyTools
except ImportError:  # pragma: no cover - script execution fallback
    from tools.geodata_typecast import gdt, gdti, gdtf
    from tools.CountyTools import CountyTools
import textwrap
import sys
import csv

class DemographicProfile:
    '''Used to display data for a geography.'''
    _ct = CountyTools()

    def __init__(self, db_row):

        self.name = db_row['NAME']
        self.state = db_row['STUSAB']
        self.geoid = db_row['GEOID']
        self.sumlevel = db_row['SUMLEVEL']
        # self.key = db_row['KEY']

        # CountyTools instance and county data
        ct = self._ct
        # County GEOIDs
        if self.sumlevel == '160': # Place
            self.counties = ct.place_to_counties[self.geoid[7:]]
            # County names (without the state)
            self.counties_display = list(map(lambda x: ct.county_geoid_to_name[x],
                                    ct.place_to_counties[self.geoid[7:]]))
            self.counties_display = list(map(lambda x: x.split(', ')[0],
                                    self.counties_display))
        else:
            self.counties = []
            self.counties_display = []

        #######################################################################
        # Row labels - Formatted names for each type of data

        self.rl = dict()

        # Population category
        self.rl['population'] = 'Total population'
        self.rl['population_density'] = 'Population density'

        # Geography category
        self.rl['land_area'] = 'Land area'
        self.rl['latitude'] = 'Latitude'
        self.rl['longitude'] = 'Longitude'

        # Race category
        self.rl['white_alone'] = 'White alone'
        self.rl['white_alone_not_hispanic_or_latino'] = 'Not Hispanic or Latino'
        self.rl['black_alone'] = 'Black or African American alone'
        self.rl['asian_alone'] = 'Asian alone'
        self.rl['other_race']  = 'Other race'
        # Technically not a race, but included in the race category
        self.rl['hispanic_or_latino'] = 'Hispanic or Latino'
        self.rl['italian_alone'] = 'Italian alone'

        # Education category
        self.rl['population_25_years_and_older'] = 'Total population 25 years and older'
        self.rl['bachelors_degree_or_higher'] = "Bachelor's degree or higher"
        self.rl['graduate_degree_or_higher'] =  'Graduate degree or higher'

        # Income category
        self.rl['per_capita_income'] = 'Per capita income'
        self.rl['median_household_income'] = 'Median household income'

        # Housing category
        self.rl['median_year_structure_built'] = 'Median year unit built'
        self.rl['median_rooms'] = 'Median rooms'
        self.rl['median_value'] = 'Median value'
        self.rl['median_rent'] = 'Median rent'

        #######################################################################
        # Indents - With how many spaces should row labels be indented?

        self.ind = dict()

        # Population category
        self.ind['population'] = 0
        self.ind['population_density'] = 0

        # Geography category
        self.ind['land_area'] = 0
        self.ind['latitude'] = 0
        self.ind['longitude'] = 0

        # Race category
        self.ind['white_alone'] = 4
        self.ind['white_alone_not_hispanic_or_latino'] = 6
        self.ind['black_alone'] = 4
        self.ind['asian_alone'] = 4
        self.ind['other_race']  = 4
        # Technically not a race, but included in the race category
        self.ind['hispanic_or_latino'] = 4
        self.ind['italian_alone'] = 0

        # Education category
        self.ind['population_25_years_and_older'] = 0
        self.ind['bachelors_degree_or_higher'] = 2
        self.ind['graduate_degree_or_higher'] = 2

        # Income category
        self.ind['per_capita_income'] = 0
        self.ind['median_household_income'] = 0

        # Housing category
        self.ind['median_year_structure_built'] = 0
        self.ind['median_rooms'] = 0
        self.ind['median_value'] = 0
        self.ind['median_rent'] = 0

        #######################################################################
        # Row headers - Mostly for CLI display of DemographicProfiles

        self.rh = dict()

        for comp in self.rl.keys():
            self.rh[comp] = ' ' * self.ind[comp] + self.rl[comp]

        #######################################################################
        # Raw components - Data that comes directly from the Census data files
        self.rc = dict()
        self.d = dict()

        # Geography category
        self.rc['land_area'] = gdtf(db_row['ALAND_SQMI'])
        self.rc['latitude'] = gdtf(db_row['INTPTLAT'])
        self.rc['longitude'] = gdtf(db_row['INTPTLONG'])

        # Population category
        self.rc['population'] = gdt(db_row['B01003_1'])

        # Race category
        self.rc['white_alone'] = gdt(db_row['B02001_2'])
        self.rc['black_alone'] = gdt(db_row['B02001_3'])
        self.rc['asian_alone'] = gdt(db_row['B02001_5'])
        self.rc['other_race'] = gdt(db_row['B01003_1']) \
            - gdt(db_row['B02001_2']) - gdt(db_row['B02001_3']) \
            - gdt(db_row['B02001_5'])
        # Technically not a race, but included in the race category
        self.rc['hispanic_or_latino'] = gdt(db_row['B03002_12'])
        self.rc['white_alone_not_hispanic_or_latino'] = gdt(db_row['B03002_3'])

        # Italian
        self.rc['italian_alone'] = gdt(db_row['B04004_51'])

        # Education category
        self.rc['population_25_years_and_older'] = gdt(db_row['B15003_1'])
        self.rc['bachelors_degree_or_higher'] = gdt(db_row['B15003_22']) \
            + gdt(db_row['B15003_23']) + gdt(db_row['B15003_24']) \
            + gdt(db_row['B15003_25'])
        self.rc['graduate_degree_or_higher'] = gdt(db_row['B15003_23']) \
           + gdt(db_row['B15003_24']) + gdt(db_row['B15003_25'])

        # Income category
        self.rc['per_capita_income'] = gdt(db_row['B19301_1'])
        self.rc['median_household_income'] = gdt(db_row['B19013_1'])

        # Housing category
        self.rc['median_year_structure_built'] = gdt(db_row['B25035_1'])
        self.rc['median_rooms'] = gdt(db_row['B25018_1'])
        self.rc['median_value'] = gdt(db_row['B25077_1'])
        self.rc['median_rent'] = gdt(db_row['B25058_1'])

        #######################################################################
        # Formatted components: Thousands seperaters, dollar signs, etc.
        self.fc = dict()

        for key in self.rc.keys():
            if key not in ['per_capita_income', 'median_year_structure_built',
                'median_value', 'median_rent', 'land_area',
                'median_household_income']:
                self.fc[key] = f'{self.rc[key]:,}'
            elif key not in ['median_year_structure_built', 'land_area']:
                if key == 'median_household_income' and self.rc[key] == 250001:
                    self.fc[key] = '$250,000+'
                else:
                    self.fc[key] = '$' + f'{self.rc[key]:,}'
            elif key == 'land_area':
                self.fc[key] = f'{self.rc[key]:,.1f}' + ' sqmi'
            else:
                self.fc[key] = str(self.rc[key])

        #######################################################################
        # Compounds: The result of mathematic operations of raw components.
        # Often, they are the result of the data they represent divided by
        # their universes.

        # Most of the if/else statements below avoid division by zero errors.

        self.c = dict()

        # Geography category
        # No compounds for this category.

        if self.rc['land_area'] != 0:
            # Population category
            self.c['population_density'] = self.rc['population'] / self.rc['land_area']
        else:
            self.c['population_density'] = 0.0

        if self.rc['population'] != 0:
            # Race category - Percentages of the total population
            self.c['white_alone'] = self.rc['white_alone'] / self.rc['population'] * 100.0
            self.c['black_alone'] = self.rc['black_alone'] / self.rc['population'] * 100.0
            self.c['asian_alone'] = self.rc['asian_alone'] / self.rc['population'] * 100.0
            self.c['other_race'] = self.rc['other_race'] / self.rc['population'] * 100.0
            # Technically not a race, but included in the race category
            self.c['hispanic_or_latino'] = self.rc['hispanic_or_latino'] / self.rc['population'] * 100.0
            self.c['white_alone_not_hispanic_or_latino'] = self.rc['white_alone_not_hispanic_or_latino'] / self.rc['population'] * 100.0
            self.c['italian_alone'] = self.rc['italian_alone'] / self.rc['population'] * 100.0
        else:
            # Race category - Percentages of the total population
            self.c['white_alone'] = 0.0
            self.c['black_alone'] = 0.0
            self.c['asian_alone'] = 0.0
            self.c['other_race'] = 0.0          # Technically not a race, but included in the race category
            self.c['hispanic_or_latino'] = 0.0
            self.c['white_alone_not_hispanic_or_latino'] = 0.0
            self.c['italian_alone'] = 0.0

        if self.rc['population_25_years_and_older'] != 0 and self.rc['population'] != 0:
            # Education category - Percentages of the population 25 years and older
            self.c['population_25_years_and_older'] = self.rc['population_25_years_and_older'] / self.rc['population'] * 100.0
            self.c['bachelors_degree_or_higher'] = self.rc['bachelors_degree_or_higher'] / self.rc['population_25_years_and_older'] * 100.0
            self.c['graduate_degree_or_higher'] = self.rc['graduate_degree_or_higher'] / self.rc['population_25_years_and_older'] * 100.0
        else:
            self.c['population_25_years_and_older'] = 0.0
            self.c['bachelors_degree_or_higher'] = 0.0
            self.c['graduate_degree_or_higher'] = 0.0


        # Income category
        # No compounds for this category

        # Housing category
        # No compounds for this category

        #######################################################################
        # Formatted compounds: The result of mathematic operations of raw
        # components
        
        self.fcd = dict()

        for key in self.c.keys():
            if key == 'population_density':
                self.fcd[key] = f'{self.c[key]:,.1f}' + '/sqmi'
            else:
                self.fcd[key] = f'{self.c[key]:,.1f}' + '%'

        #######################################################################
        # Inter-area margin (for display purposes)
        self.iam = ' '

    def __repr__(self):
        '''Display a representation of the DemographicProfile class'''
        return "DemographicProfile(name='%s', counties=%s)" % (self.name,
                self.counties)

    def dp_full_row_str(self, content):
        '''Return a line with just one string'''
        return self.iam + textwrap.fill(content, 67, subsequent_indent=' ') + '\n'

    def divider(self):
        '''Return a divider'''
        return '-' * 69 + '\n'

    def blank_line(self):
        '''Return a blank line'''
        return '\n'
    
    def dp_row_str(self, record_col, component_col, compound_col):
        '''Return a row with a header, compound, and component'''
        return self.iam + record_col.ljust(35) + self.iam \
            + component_col.rjust(15) + self.iam + compound_col.rjust(15) \
            + self.iam + '\n'

    def dp_row_std(self, key):
        '''Return a row with the most common characteristics'''
        return self.dp_row_str(self.rh[key], self.fc[key], self.fcd[key])

    def dp_row_nc(self, key):
        '''Return a row without a compound'''
        return self.dp_row_str(self.rh[key], '', self.fc[key])

    def tocsv(self):
        '''Display as a CSV'''
        csvwriter = csv.writer(sys.stdout, quoting=csv.QUOTE_MINIMAL)

        def csv_dp_full_row_str(content):
            '''Return a CSV row with just one string'''
            csvwriter.writerow([content])

        def csv_divider():
            '''Return a CSV divider'''
            csvwriter.writerow([])

        def csv_dp_row_str(record_col, component_col, compound_col):
            '''Return a CSV row with a header, compound, and component'''
            csvwriter.writerow([record_col, component_col, compound_col])

        def csv_dp_row_std(key):
            '''Return a CSV row with the most common characteristics'''
            csvwriter.writerow([self.rh[key], self.fc[key], self.fcd[key]])

        def csv_dp_row_nc(key):
            '''Return a row without a compound'''
            csvwriter.writerow([self.rh[key], '', self.fc[key]])

        csv_dp_full_row_str(self.name)

        # Print counties if this DemographicProfile is for a place (160)
        if self.sumlevel == '160':
            csv_dp_full_row_str(', '.join(self.counties_display))

        csv_divider()
        csv_dp_full_row_str('GEOGRAPHY')
        csv_dp_row_str(self.rh['land_area'], '', self.fc['land_area'])
        csv_dp_full_row_str('POPULATION')
        csv_dp_row_nc('population')
        csv_dp_row_str(self.rh['population_density'], '', self.fcd['population_density'])
        csv_dp_full_row_str('  Race')
        csv_dp_row_std('white_alone')
        csv_dp_row_std('white_alone_not_hispanic_or_latino')
        csv_dp_row_std('black_alone')
        csv_dp_row_std('asian_alone')
        csv_dp_row_std('other_race')
        csv_dp_full_row_str('  Hispanic or Latino (of any race)')
        csv_dp_row_std('hispanic_or_latino')
        csv_dp_full_row_str('EDUCATION')
        csv_dp_row_std('population_25_years_and_older')
        csv_dp_row_std('bachelors_degree_or_higher')
        csv_dp_row_std('graduate_degree_or_higher')
        csv_dp_full_row_str('INCOME')
        csv_dp_row_nc('per_capita_income')
        csv_dp_row_nc('median_household_income')
        csv_dp_full_row_str('HOUSING')
        csv_dp_row_nc('median_year_structure_built')
        csv_dp_row_nc('median_rooms')
        csv_dp_row_nc('median_value')
        csv_dp_row_nc('median_rent')
        csv_divider()

    def __str__(self):
        '''Return table'''
        # + self.dp_full_row_str(self.key) \
        out_str  = self.divider()
        out_str += self.dp_full_row_str(self.name)

        # Print counties if this DemographicProfile is for a place (160)
        if self.sumlevel == '160':
            out_str += self.dp_full_row_str(', '.join(self.counties_display))

        out_str += self.divider()
        out_str += self.dp_full_row_str('GEOGRAPHY')
        out_str += self.dp_row_str(self.rh['land_area'], '', self.fc['land_area'])
        out_str += self.dp_full_row_str('POPULATION')
        out_str += self.dp_row_nc('population')
        out_str += self.dp_row_str(self.rh['population_density'], '', self.fcd['population_density'])
        out_str += self.dp_full_row_str('  Race')
        out_str += self.dp_row_std('white_alone')
        out_str += self.dp_row_std('white_alone_not_hispanic_or_latino')
        out_str += self.dp_row_std('black_alone')
        out_str += self.dp_row_std('asian_alone')
        out_str += self.dp_row_std('other_race')
        out_str += self.dp_full_row_str('  Hispanic or Latino (of any race)')
        out_str += self.dp_row_std('hispanic_or_latino')
        out_str += self.dp_full_row_str('EDUCATION')
        out_str += self.dp_row_std('population_25_years_and_older')
        out_str += self.dp_row_std('bachelors_degree_or_higher')
        out_str += self.dp_row_std('graduate_degree_or_higher')
        out_str += self.dp_full_row_str('INCOME')
        out_str += self.dp_row_nc('per_capita_income')
        out_str += self.dp_row_nc('median_household_income')
        out_str += self.dp_full_row_str('HOUSING')
        out_str += self.dp_row_nc('median_year_structure_built')
        out_str += self.dp_row_nc('median_rooms')
        out_str += self.dp_row_nc('median_value')
        out_str += self.dp_row_nc('median_rent')
        out_str += self.divider()

        return out_str

    def __eq__(self, other):
        return self.sumlevel == other.sumlevel and self.name == other.name

    def __hash__(self):
        return hash((self.sumlevel, self.name))
