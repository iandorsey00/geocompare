class StateTools:
    '''Tools to convert state names to abbreviations, etc.'''
    def get_state(self, geo_name):
        '''Get the state name given a Census display label.'''
        # Split the name string by ', '
        if ';' in geo_name:
            split_geo_name = geo_name.split('; ')
        else:
            split_geo_name = geo_name.split(', ')
        return split_geo_name[-1]

    def get_abbrevs(self, lowercase=False, inc_us=False):
        '''Get two-letter state abbreviations.'''
        abbrevs = self.abbrevs

        if inc_us:
            abbrevs.append('US')

        if not lowercase:
            return abbrevs
        else:
            return list(map(lambda x: x.lower(), abbrevs))

    def get_abbrev(self, name, lowercase=False):
        '''Transform a state name into its abbreviation.'''
        if not lowercase:
            return self.name_to_abbrev[name]
        else:
            return self.name_to_abbrev[name].lower()

    def get_name(self, abbrev):
        '''Transform a state abbreviation into its name.'''
        return self.state_abbrev_to_name[abbrev.upper()]

    def __init__(self):
        # geoid_to_name #######################################################
        # name_to_geoid #######################################################
        self.geoid_to_name = {
            '01': 'Alabama',
            '02': 'Alaska',
            '04': 'Arizona',
            '05': 'Arkansas',
            '06': 'California',
            '08': 'Colorado',
            '09': 'Connecticut',
            '10': 'Delaware',
            '11': 'District of Columbia',
            '12': 'Florida',
            '13': 'Georgia',
            '15': 'Hawaii',
            '16': 'Idaho',
            '17': 'Illinois',
            '18': 'Indiana',
            '19': 'Iowa',
            '20': 'Kansas',
            '21': 'Kentucky',
            '22': 'Louisiana',
            '23': 'Maine',
            '24': 'Maryland',
            '25': 'Massachusetts',
            '26': 'Michigan',
            '27': 'Minnesota',
            '28': 'Mississippi',
            '29': 'Missouri',
            '30': 'Montana',
            '31': 'Nebraska',
            '32': 'Nevada',
            '33': 'New Hampshire',
            '34': 'New Jersey',
            '35': 'New Mexico',
            '36': 'New York',
            '37': 'North Carolina',
            '38': 'North Dakota',
            '39': 'Ohio',
            '40': 'Oklahoma',
            '41': 'Oregon',
            '42': 'Pennsylvania',
            '44': 'Rhode Island',
            '45': 'South Carolina',
            '46': 'South Dakota',
            '47': 'Tennessee',
            '48': 'Texas',
            '49': 'Utah',
            '50': 'Vermont',
            '51': 'Virginia',
            '53': 'Washington',
            '54': 'West Virginia',
            '55': 'Wisconsin',
            '56': 'Wyoming',
            '60': 'American Samoa',
            '66': 'Guam',
            '69': 'Commonwealth of the Northern Mariana Islands',
            '72': 'Puerto Rico',
            '78': 'United States Virgin Islands',
        }

        # Inverse of self.geoid_to_name
        self.name_to_geoid = {v: k for k, v in self.geoid_to_name.items()}

        # States and abbreviations ############################################
        self.abbrevs = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DC', 'DE',
        'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY',
        'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT',
        'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

        self.name_to_abbrev = {
            'Alaska': 'AK',
            'Alabama': 'AL',
            'Arkansas': 'AR',
            'American Samoa': 'AS',
            'Arizona': 'AZ',
            'California': 'CA',
            'Colorado': 'CO',
            'Connecticut': 'CT',
            'District of Columbia': 'DC',
            'Delaware': 'DE',
            'Florida': 'FL',
            'Georgia': 'GA',
            'Guam': 'GU',
            'Hawaii': 'HI',
            'Iowa': 'IA',
            'Idaho': 'ID',
            'Illinois': 'IL',
            'Indiana': 'IN',
            'Kansas': 'KS',
            'Kentucky': 'KY',
            'Louisiana': 'LA',
            'Massachusetts': 'MA',
            'Maryland': 'MD',
            'Maine': 'ME',
            'Michigan': 'MI',
            'Minnesota': 'MN',
            'Missouri': 'MO',
            'Northern Mariana Islands': 'MP',
            'Mississippi': 'MS',
            'Montana': 'MT',
            'National': 'NA',
            'North Carolina': 'NC',
            'North Dakota': 'ND',
            'Nebraska': 'NE',
            'New Hampshire': 'NH',
            'New Jersey': 'NJ',
            'New Mexico': 'NM',
            'Nevada': 'NV',
            'New York': 'NY',
            'Ohio': 'OH',
            'Oklahoma': 'OK',
            'Oregon': 'OR',
            'Pennsylvania': 'PA',
            'Puerto Rico': 'PR',
            'Rhode Island': 'RI',
            'South Carolina': 'SC',
            'South Dakota': 'SD',
            'Tennessee': 'TN',
            'Texas': 'TX',
            'Utah': 'UT',
            'Virginia': 'VA',
            'Virgin Islands': 'VI',
            'Vermont': 'VT',
            'Washington': 'WA',
            'Wisconsin': 'WI',
            'West Virginia': 'WV',
            'Wyoming': 'WY'
        }

        self.state_abbrev_to_name = dict()

        for name, abbrev in self.name_to_abbrev.items():
            self.state_abbrev_to_name[abbrev] = name
