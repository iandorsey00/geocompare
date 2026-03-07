'''
Vectors used to compare one geography's demographic characteristics with
others. The closer the Euclidean distance between the vectors, the more
similar the geographies. Scores are determined based on a normalization process
for demographic data.
'''

try:
    from geodata.tools.geodata_safedivision import gdsd
    from geodata.tools.geodata_typecast import gdt, gdti, gdtf
    from geodata.tools.CountyTools import CountyTools
except ImportError:  # pragma: no cover - script execution fallback
    from tools.geodata_safedivision import gdsd
    from tools.geodata_typecast import gdt, gdti, gdtf
    from tools.CountyTools import CountyTools

import sys

class GeoVector:
    '''A vector used to compare places with others.'''
    _ct = CountyTools()

    def __init__(
        self,
        db_row,
        medians,
        standard_deviations
        ):

        self.sumlevel = db_row['SUMLEVEL']
        self.state = db_row['STUSAB']
        self.geoid = db_row['GEOID']
        self.name = db_row['NAME']

        # CountyTools instance and county data
        ct = self._ct
        self.counties = []
        self.counties_display = []

        if self.sumlevel == '160':
            # County GEOIDs
            self.counties = ct.place_to_counties[self.geoid[7:]]
            # County names (without the state)
            self.counties_display = list(map(lambda x: ct.county_geoid_to_name[x],
                                        ct.place_to_counties[self.geoid[7:]]))
            self.counties_display = list(map(lambda x: x.split(', ')[0],
                                        self.counties_display))

        # Data - All data handled by GeoVectors
        self.d = dict()

        self.d['population'] = db_row['B01003_1']
        self.d['land_area_sqmi'] = db_row['ALAND_SQMI']
        self.d['per_capita_income'] = db_row['B19301_1']
        self.d['white_alone'] = db_row['B02001_2']
        self.d['black_alone'] = db_row['B02001_3']
        self.d['asian_alone'] = db_row['B02001_5']
        self.d['hispanic_or_latino'] = db_row['B03002_12']
        self.d['population_25_years_or_older'] = db_row['B15003_1']
        self.d['bachelors_degree'] = db_row['B15003_22']
        self.d['masters_degree'] = db_row['B15003_23']
        self.d['professional_school_degree'] = db_row['B15003_24']
        self.d['doctorate_degree'] = db_row['B15003_25']
        self.d['median_year_structure_built'] = db_row['B25035_1']

        # Stop creation if there is insufficient data.
        for key in self.d.keys():
            if self.d[key] == '':
                raise AttributeError('Not enough data.')

        # Raw subcomponents - Raw values of each subcomponent
        self.rs = dict()
        
        def gdsdd(dividend, divisor, numeric_dividend=False):
            '''Wrapper for gdsd for GeoVector data.'''
            if not numeric_dividend:
                return gdsd(self.d[dividend], self.d[divisor])
            else:
                return gdsd(dividend, self.d[divisor])

        self.rs['population_density'] = gdsdd('population', 'land_area_sqmi')
        self.rs['per_capita_income'] = gdti(self.d['per_capita_income'])
        self.rs['white_alone'] = gdsdd('white_alone', 'population') * 100.0
        self.rs['black_alone'] = gdsdd('black_alone', 'population') * 100.0
        self.rs['asian_alone'] = gdsdd('asian_alone', 'population') * 100.0
        self.rs['hispanic_or_latino'] = gdsdd('hispanic_or_latino', 'population') * 100.0

        self.rs['bachelors_degree_or_higher'] = gdsdd(
              gdti(self.d['bachelors_degree']) \
            + gdti(self.d['masters_degree']) \
            + gdti(self.d['professional_school_degree']) \
            + gdti(self.d['doctorate_degree']), 
              'population_25_years_or_older', numeric_dividend=True)  * 100.0
        
        self.rs['graduate_degree_or_higher'] = gdsdd(
              gdti(self.d['masters_degree']) \
            + gdti(self.d['professional_school_degree']) \
            + gdti(self.d['doctorate_degree']), 
              'population_25_years_or_older', numeric_dividend=True) * 100.0

        self.rs['median_year_structure_built'] = gdti(self.d['median_year_structure_built']) - 1939

        # Get medians for each subcomponent
        self.med = dict()

        def gdsdm(dividend, divisor, numeric_dividend=False):
            '''Wrapper function for operations with medians.'''
            if not numeric_dividend:
                return gdsd(float(medians[dividend]), float(medians[divisor]))
            else:
                return gdsd(dividend, float(medians[divisor]))

        self.med['population_density'] = gdsdm('B01003_1', 'ALAND_SQMI')
        self.med['per_capita_income'] = gdtf(medians['B19301_1'])
        self.med['white_alone'] = gdsdm('B02001_2', 'B01003_1') * 100.0
        self.med['black_alone'] = gdsdm('B02001_3', 'B01003_1') * 100.0
        self.med['asian_alone'] = gdsdm('B03002_12', 'B01003_1') * 100.0
        self.med['hispanic_or_latino'] = gdsdm('B03002_12', 'B01003_1') * 100.0

        self.med['bachelors_degree_or_higher'] = gdsdm(
              gdti(medians['B15003_22']) \
            + gdti(medians['B15003_23']) \
            + gdti(medians['B15003_24']) \
            + gdti(medians['B15003_25']),
              'B15003_1', numeric_dividend=True) * 100.0
        self.med['graduate_degree_or_higher'] = gdsdm(
              gdti(medians['B15003_23']) \
            + gdti(medians['B15003_24']) \
            + gdti(medians['B15003_25']),
              'B15003_1', numeric_dividend=True) * 100.0

        self.med['median_year_structure_built'] = gdtf(medians['B25035_1']) - 1939

        # Get standard deviations for each subcomponent
        self.sd = dict()

        def gdsds(dividend, divisor, numeric_dividend=False):
            '''Wrapper function for operations with standard deviations.'''
            if not numeric_dividend:
                return gdsd(float(standard_deviations[dividend]), float(standard_deviations[divisor]))
            else:
                return gdsd(dividend, float(standard_deviations[divisor]))


        self.sd['population_density'] = gdsds('B01003_1', 'ALAND_SQMI')
        self.sd['per_capita_income'] = gdtf(standard_deviations['B19301_1'])
        self.sd['white_alone'] = gdsds('B02001_2', 'B01003_1') * 100.0
        self.sd['black_alone'] = gdsds('B02001_3', 'B01003_1') * 100.0
        self.sd['asian_alone'] = gdsds('B03002_12', 'B01003_1') * 100.0
        self.sd['hispanic_or_latino'] = gdsds('B03002_12', 'B01003_1') * 100.0

        self.sd['bachelors_degree_or_higher'] = gdsds(
              gdti(standard_deviations['B15003_22']) \
            + gdti(standard_deviations['B15003_23']) \
            + gdti(standard_deviations['B15003_24']) \
            + gdti(standard_deviations['B15003_25']),
              'B15003_1', numeric_dividend=True) * 100.0
        self.sd['graduate_degree_or_higher'] = gdsds(
              gdti(standard_deviations['B15003_23']) \
            + gdti(standard_deviations['B15003_24']) \
            + gdti(standard_deviations['B15003_25']),
              'B15003_1', numeric_dividend=True) * 100.0

        self.sd['median_year_structure_built'] = gdtf(standard_deviations['B25035_1'])

        #######################################################################
        # Calculate subcomponent scores
        #
        # Scores for each subcomponent are calculated as follows.
        #
        # If the value for the subcomponent is below the median, then
        # score is the ratio of the value and the median times 50. (In simple
        # terms, we are assigning a proportional value between 0 and 50.)
        #
        # If the value for the subcomponent is at the median, the score is 50.
        #
        # If the value for the subcomponent is above the median but below
        # three standard deviations from the median, the score is the ratio of
        # the value and (the median plus two standard deviations) times 100.
        # (In simple terms, we are assigning a proportional value between 50
        # and 100.)
        #
        # If the value for the subcomponent is at the median plus two standard
        # deviations or more, the score will be 100.

        # Create a dictionary to store subcomponents
        self.s = dict()

        # sc = s = subcomponent
        # rs = raw subcomponent
        # med = median
        # sd = standard deviation
        for sc in self.rs.keys():
            if self.rs[sc] < self.med[sc]:
                self.s[sc] = \
                int(
                    round( (self.rs[sc] / self.med[sc]) * 50 )
                )
            elif self.rs[sc] == self.med[sc]:
                self.s[sc] = 50
            elif self.rs[sc] > self.med[sc] and self.rs[sc] < self.med[sc] + (self.sd[sc] * 3):
                self.s[sc] = \
                int(
                    round( 
                            50 + (
                            (self.rs[sc] - self.med[sc]) / (self.sd[sc] * 3)
                            ) * 50
                    )
                )
            else: # self.rs[sc] > self.med[sc] + (self.sd[sc] * 3)
                self.s[sc] = 100

        #######################################################################
        # Weighted subcomponents
        #
        # Though the __repr__ for GeoVectors will only display unweighted
        # subcomponents, the weight subcomponents will be the ones actually
        # involved in the distance calculation between GeoVectors.
        #
        # Groups of weighted subcomponents make up each component shown below.

        self.ws = dict()

        # Standard mode
        self.ws['std'] = dict()
        # Appearance mode
        self.ws['app'] = dict()

        #
        # The population density component:
        #   Both standard and appearance modes:
        #     Population density (100%)
        #

        self.ws['std']['population_density'] = self.s['population_density']
        self.ws['app']['population_density'] = self.s['population_density']

        #
        # The income component:
        #   Both standard and appearance modes:
        #     Per capita income (100%)
        #

        self.ws['std']['per_capita_income'] = self.s['per_capita_income']
        self.ws['app']['per_capita_income'] = self.s['per_capita_income']

        #
        # The race component:
        #   Standard mode only:
        #     White alone (25%)
        #     Black alone (25%)
        #     Asian alone (25%)
        #     Hispanic or Latino (25%)*
        #
        # * Note: Hispanic or Latino is not a race as far as the Census is
        # concerned. To the Census, a person is either Hispanic or Latino or
        # they are not, and in addition to that they are of some race. But for
        # GeoVector calculation purposes, that category makes up 25% of the
        # race component.
        #

        self.ws['std']['white_alone'] = self.s['white_alone'] / 4
        self.ws['std']['black_alone'] = self.s['black_alone'] / 4
        self.ws['std']['asian_alone'] = self.s['asian_alone'] / 4
        self.ws['std']['hispanic_or_latino'] = self.s['hispanic_or_latino'] / 4

        #
        # The education component:
        #   Standard mode only:
        #     Bachelor's degree or higher (50%)
        #     Graduate degree or higher (50%)
        #

        self.ws['std']['bachelors_degree_or_higher'] = self.s['bachelors_degree_or_higher'] / 2
        self.ws['std']['graduate_degree_or_higher'] = self.s['graduate_degree_or_higher'] / 2

        #
        # The median year structure built component:
        #   Appearance mode only:
        #     Median year structure built (100%)
        #

        self.ws['app']['median_year_structure_built'] = self.s['median_year_structure_built']

    def distance(self, other, mode='std'):
        '''Calculate the euclidean distance from other GeoVectors.'''
        distance = 0

        # Square the difference of each subcomponent, then add them together.
        for sc in self.ws[mode].keys():
            distance += (self.ws[mode][sc] - other.ws[mode][sc]) ** 2

        import math

        # Return the square root of that sum.
        return math.sqrt(distance)

    def display_row(self, mode):
        '''Display the GeoVector as a data row.'''
        # The inter-area margin
        iam = ' '

        # Print the display_label for the geo
        out_str = self.name.ljust(40)[:40] + iam
        # If the geo is a place, print the county/ies
        if self.sumlevel == '160':
            out_str += ', '.join(self.counties_display).ljust(20)[:20] + iam
        else:
            out_str += ' '.ljust(20)[:20] + iam

        # Print the subcomponent scores
        for comp in self.ws[mode].keys():
            out_str += str(self.s[comp]).rjust(3) + iam
        
        return out_str

    def __repr__(self):
        '''Display vector information and subcomponent scores.'''
        population = gdti(self.d['population'])
        if self.sumlevel == '160':
            out_str = '''GeoVector(%s; %s.
Population: %s. Std: (%s,%s,%s,%s,%s,%s,%s,%s). App: (%s,%s,%s).)''' % (
            self.name,
            ', '.join(self.counties_display),
            f'{population:,}',
            self.s['population_density'],
            self.s['per_capita_income'],
            self.s['white_alone'],
            self.s['black_alone'],
            self.s['asian_alone'],
            self.s['hispanic_or_latino'],
            self.s['bachelors_degree_or_higher'],
            self.s['graduate_degree_or_higher'],
            self.s['population_density'],
            self.s['per_capita_income'],
            self.s['median_year_structure_built'],
            )
        else:
            out_str = '''GeoVector(%s
Population: %s. Std: (%s,%s,%s,%s,%s,%s,%s,%s). App: (%s,%s,%s).)''' % (
            self.name,
            f'{population:,}',
            self.s['population_density'],
            self.s['per_capita_income'],
            self.s['white_alone'],
            self.s['black_alone'],
            self.s['asian_alone'],
            self.s['hispanic_or_latino'],
            self.s['bachelors_degree_or_higher'],
            self.s['graduate_degree_or_higher'],
            self.s['population_density'],
            self.s['per_capita_income'],
            self.s['median_year_structure_built'],
            )
        
        return out_str

    def __eq__(self, other):
        return self.sumlevel == other.sumlevel and self.name == other.name

    def __hash__(self):
        return hash((self.sumlevel, self.name))
