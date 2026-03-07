from geocompare.tools.CountyTools import CountyTools
from geocompare.tools.StateTools import StateTools


class KeyTools:
    '''
    CURRENTLY:
    At this stage in development, we're mostly interested in hashing county
    names for context arguments.
    '''
    def summary_level(self, key):
        '''Get the key's summary level code.'''
        components = key.split(':')
        n_components = len(components)

        if n_components == 3:
            return '050' # County
        else:
            return '040' # State

    def __init__(self):
        st = StateTools()
        ct = CountyTools()

        #######################################################################
        # Counties

        # key_to_county_name ##################################################
        # county_name_to_key ##################################################
        self.key_to_county_name = dict()
        self.county_name_to_key = dict()

        for county_name in ct.county_names:
            split_county_name = county_name.split(', ')

            # name portion
            name = split_county_name[0]
            name = name[:-7]
            name = name.replace(' ', '')
            name = name.lower()

            # state portion
            state = split_county_name[-1]
            state = st.get_abbrev(state, lowercase=True)

            # Build key
            key = 'us:' + state + ':' + name + '/county'

            # Insert key/value pair
            self.key_to_county_name[key] = county_name
            self.county_name_to_key[county_name] = key
