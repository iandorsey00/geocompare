'''
Intended to describe a specific geography or compare small numbers of
geographies.
'''

# pylint: disable=import-error
try:
    from geocompare.tools.geodata_typecast import gdt, gdti, gdtf
    from geocompare.tools.CountyTools import CountyTools
except ImportError:  # pragma: no cover - script execution fallback
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
        self.rl['under_18'] = 'Population under 18'
        self.rl['age_65_plus'] = 'Population 65 and over'

        # Geography category
        self.rl['land_area'] = 'Land area'
        self.rl['latitude'] = 'Latitude'
        self.rl['longitude'] = 'Longitude'
        self.rl['median_age'] = 'Median age'

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
        self.rl['population_below_poverty_level'] = 'Population below poverty level'
        self.rl['labor_force'] = 'Civilian labor force'
        self.rl['unemployed_population'] = 'Unemployed population'
        self.rl['households'] = 'Total households'

        # Housing category
        self.rl['average_household_size'] = 'Average household size'
        self.rl['occupied_housing_units'] = 'Occupied housing units'
        self.rl['homeowner_occupied_housing_units'] = 'Owner-occupied housing units'
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
        self.ind['under_18'] = 0
        self.ind['age_65_plus'] = 0

        # Geography category
        self.ind['land_area'] = 0
        self.ind['latitude'] = 0
        self.ind['longitude'] = 0
        self.ind['median_age'] = 0

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
        self.ind['population_below_poverty_level'] = 0
        self.ind['labor_force'] = 0
        self.ind['unemployed_population'] = 2
        self.ind['households'] = 0

        # Housing category
        self.ind['average_household_size'] = 0
        self.ind['occupied_housing_units'] = 0
        self.ind['homeowner_occupied_housing_units'] = 2
        self.ind['median_year_structure_built'] = 0
        self.ind['median_rooms'] = 0
        self.ind['median_value'] = 0
        self.ind['median_rent'] = 0

        #######################################################################
        # Row headers - Mostly for CLI display of DemographicProfiles

        self.rh = dict()

        for comp in self.rl.keys():
            self.rh[comp] = ' ' * self.ind[comp] + self.rl[comp]

        # Display sections define how rows should be rendered in __str__/tocsv.
        # row mode: nc=no compound, std=component+compound, co=compound only.
        self.display_sections = [
            ('GEOGRAPHY', [('nc', 'land_area')]),
            ('POPULATION', [
                ('nc', 'population'),
                ('co', 'population_density'),
                ('std', 'under_18'),
                ('std', 'age_65_plus'),
            ]),
            ('AGE', [('nc', 'median_age')]),
            ('  Race', [
                ('std', 'white_alone'),
                ('std', 'white_alone_not_hispanic_or_latino'),
                ('std', 'black_alone'),
                ('std', 'asian_alone'),
                ('std', 'other_race'),
            ]),
            ('  Hispanic or Latino (of any race)', [('std', 'hispanic_or_latino')]),
            ('EDUCATION', [
                ('std', 'population_25_years_and_older'),
                ('std', 'bachelors_degree_or_higher'),
                ('std', 'graduate_degree_or_higher'),
            ]),
            ('INCOME', [('nc', 'per_capita_income'), ('nc', 'median_household_income')]),
            ('ECONOMY', [
                ('std', 'population_below_poverty_level'),
                ('std', 'labor_force'),
                ('std', 'unemployed_population'),
                ('nc', 'households'),
            ]),
            ('HOUSING', [
                ('nc', 'average_household_size'),
                ('nc', 'occupied_housing_units'),
                ('std', 'homeowner_occupied_housing_units'),
                ('nc', 'median_year_structure_built'),
                ('nc', 'median_rooms'),
                ('nc', 'median_value'),
                ('nc', 'median_rent'),
            ]),
        ]
        self.compact_display_sections = [
            ('GEOGRAPHY', [('nc', 'land_area')]),
            ('POPULATION', [
                ('nc', 'population'),
                ('co', 'population_density'),
                ('std', 'under_18'),
                ('std', 'age_65_plus'),
            ]),
            ('INCOME', [('nc', 'per_capita_income'), ('nc', 'median_household_income')]),
            ('ECONOMY', [
                ('std', 'population_below_poverty_level'),
                ('std', 'unemployed_population'),
            ]),
            ('HOUSING', [
                ('std', 'homeowner_occupied_housing_units'),
                ('nc', 'median_value'),
                ('nc', 'median_rent'),
            ]),
        ]

        #######################################################################
        # Raw components - Data that comes directly from the Census data files
        self.rc = dict()
        self.d = dict()

        # Geography category
        self.rc['land_area'] = gdtf(db_row['ALAND_SQMI'])
        self.rc['latitude'] = gdtf(db_row['INTPTLAT'])
        self.rc['longitude'] = gdtf(db_row['INTPTLONG'])
        self.rc['median_age'] = gdtf(db_row['B01002_1'])

        # Population category
        self.rc['population'] = gdt(db_row['B01003_1'])
        self.rc['under_18'] = \
            gdt(db_row['B01001_3']) + gdt(db_row['B01001_4']) \
            + gdt(db_row['B01001_5']) + gdt(db_row['B01001_6']) \
            + gdt(db_row['B01001_27']) + gdt(db_row['B01001_28']) \
            + gdt(db_row['B01001_29']) + gdt(db_row['B01001_30'])
        self.rc['age_65_plus'] = \
            gdt(db_row['B01001_20']) + gdt(db_row['B01001_21']) \
            + gdt(db_row['B01001_22']) + gdt(db_row['B01001_23']) \
            + gdt(db_row['B01001_24']) + gdt(db_row['B01001_25']) \
            + gdt(db_row['B01001_44']) + gdt(db_row['B01001_45']) \
            + gdt(db_row['B01001_46']) + gdt(db_row['B01001_47']) \
            + gdt(db_row['B01001_48']) + gdt(db_row['B01001_49'])

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
        self.rc['poverty_universe'] = gdt(db_row['B17001_1'])
        self.rc['population_below_poverty_level'] = gdt(db_row['B17001_2'])
        self.rc['labor_force'] = gdt(db_row['B23025_3'])
        self.rc['unemployed_population'] = gdt(db_row['B23025_5'])
        self.rc['households'] = gdt(db_row['B11001_1'])

        # Housing category
        self.rc['average_household_size'] = gdtf(db_row['B25010_1'])
        self.rc['occupied_housing_units'] = gdt(db_row['B25003_1'])
        self.rc['homeowner_occupied_housing_units'] = gdt(db_row['B25003_2'])
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
                'median_household_income', 'median_age',
                'average_household_size']:
                self.fc[key] = f'{self.rc[key]:,}'
            elif key not in ['median_year_structure_built', 'land_area']:
                if key == 'median_household_income' and self.rc[key] == 250001:
                    self.fc[key] = '$250,000+'
                elif key in ['median_age', 'average_household_size']:
                    self.fc[key] = f'{self.rc[key]:,.1f}'
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
            self.c['under_18'] = self.rc['under_18'] / self.rc['population'] * 100.0
            self.c['age_65_plus'] = self.rc['age_65_plus'] / self.rc['population'] * 100.0
        else:
            # Race category - Percentages of the total population
            self.c['white_alone'] = 0.0
            self.c['black_alone'] = 0.0
            self.c['asian_alone'] = 0.0
            self.c['other_race'] = 0.0          # Technically not a race, but included in the race category
            self.c['hispanic_or_latino'] = 0.0
            self.c['white_alone_not_hispanic_or_latino'] = 0.0
            self.c['italian_alone'] = 0.0
            self.c['under_18'] = 0.0
            self.c['age_65_plus'] = 0.0

        if self.rc['population_25_years_and_older'] != 0 and self.rc['population'] != 0:
            # Education category - Percentages of the population 25 years and older
            self.c['population_25_years_and_older'] = self.rc['population_25_years_and_older'] / self.rc['population'] * 100.0
            self.c['bachelors_degree_or_higher'] = self.rc['bachelors_degree_or_higher'] / self.rc['population_25_years_and_older'] * 100.0
            self.c['graduate_degree_or_higher'] = self.rc['graduate_degree_or_higher'] / self.rc['population_25_years_and_older'] * 100.0
        else:
            self.c['population_25_years_and_older'] = 0.0
            self.c['bachelors_degree_or_higher'] = 0.0
            self.c['graduate_degree_or_higher'] = 0.0

        if self.rc['poverty_universe'] != 0:
            self.c['population_below_poverty_level'] = \
                self.rc['population_below_poverty_level'] / self.rc['poverty_universe'] * 100.0
        else:
            self.c['population_below_poverty_level'] = 0.0

        if self.rc['labor_force'] != 0:
            self.c['unemployed_population'] = \
                self.rc['unemployed_population'] / self.rc['labor_force'] * 100.0
        else:
            self.c['unemployed_population'] = 0.0

        if self.rc['occupied_housing_units'] != 0:
            self.c['homeowner_occupied_housing_units'] = \
                self.rc['homeowner_occupied_housing_units'] / self.rc['occupied_housing_units'] * 100.0
        else:
            self.c['homeowner_occupied_housing_units'] = 0.0


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

    def _format_component_value(self, key, value):
        if key == 'land_area':
            return f'{value:,.1f} sqmi'
        if key in ['per_capita_income', 'median_household_income', 'median_value', 'median_rent']:
            if key == 'median_household_income' and value == 250001:
                return '$250,000+'
            return '$' + f'{value:,}'
        if key == 'median_year_structure_built':
            return str(value)
        return f'{value:,}'

    def _format_compound_value(self, key, value, suffix):
        if key == 'population_density':
            return f'{value:,.1f}/sqmi'
        if suffix:
            return f'{value:,.1f}{suffix}'
        return f'{value:,.1f}'

    def add_custom_metric(
        self,
        section_title,
        key,
        label,
        value,
        indent=0,
        value_display=None,
        compound_value=None,
        compound_display=None,
        compound_suffix='%',
    ):
        self.rl[key] = label
        self.ind[key] = indent
        self.rh[key] = ' ' * indent + label
        self.rc[key] = value
        self.fc[key] = (
            value_display if value_display is not None else self._format_component_value(key, value)
        )

        row_mode = 'nc'
        if compound_value is not None:
            self.c[key] = compound_value
            self.fcd[key] = (
                compound_display
                if compound_display is not None
                else self._format_compound_value(key, compound_value, compound_suffix)
            )
            row_mode = 'std'

        for idx, (existing_title, rows) in enumerate(self.display_sections):
            if existing_title == section_title:
                rows.append((row_mode, key))
                self.display_sections[idx] = (existing_title, rows)
                return
        self.display_sections.append((section_title, [(row_mode, key)]))

    def _sections_for_view(self, view='full'):
        if view == 'compact':
            sections = getattr(self, 'compact_display_sections', [])
        else:
            sections = getattr(self, 'display_sections', [])
        if sections:
            return sections
        # Backward compatibility for legacy pickled objects.
        return [('POPULATION', [('nc', 'population')])]

    def _can_render_row(self, row_mode, key):
        # Backward compatibility: legacy pickled profiles may miss newer keys.
        if key not in self.rh:
            return False
        if row_mode == 'std':
            return key in self.fc and key in self.fcd
        if row_mode == 'co':
            return key in self.fcd
        return key in self.fc

    def __repr__(self):
        '''Display a representation of the DemographicProfile class'''
        return (
            "DemographicProfile("
            f"name={self.name!r}, "
            f"geoid={self.geoid!r}, "
            f"sumlevel={self.sumlevel!r}, "
            f"state={self.state!r}"
            ")"
        )

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

    def tocsv(self, view='full'):
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
        for section_title, rows in self._sections_for_view(view):
            renderable_rows = [row for row in rows if self._can_render_row(*row)]
            if not renderable_rows:
                continue
            csv_dp_full_row_str(section_title)
            for row_mode, key in renderable_rows:
                if row_mode == 'std':
                    csv_dp_row_std(key)
                elif row_mode == 'co':
                    csv_dp_row_str(self.rh[key], '', self.fcd[key])
                else:
                    csv_dp_row_nc(key)
        csv_divider()

    def to_table(self, view='full'):
        '''Return table view'''
        # + self.dp_full_row_str(self.key) \
        out_str  = self.divider()
        out_str += self.dp_full_row_str(self.name)

        # Print counties if this DemographicProfile is for a place (160)
        if self.sumlevel == '160':
            out_str += self.dp_full_row_str(', '.join(self.counties_display))

        out_str += self.divider()
        for section_title, rows in self._sections_for_view(view):
            renderable_rows = [row for row in rows if self._can_render_row(*row)]
            if not renderable_rows:
                continue
            out_str += self.dp_full_row_str(section_title)
            for row_mode, key in renderable_rows:
                if row_mode == 'std':
                    out_str += self.dp_row_std(key)
                elif row_mode == 'co':
                    out_str += self.dp_row_str(self.rh[key], '', self.fcd[key])
                else:
                    out_str += self.dp_row_nc(key)
        out_str += self.divider()

        return out_str

    def __str__(self):
        '''Return full table'''
        return self.to_table(view='full')

    def __eq__(self, other):
        return self.sumlevel == other.sumlevel and self.name == other.name

    def __hash__(self):
        return hash((self.sumlevel, self.name))
