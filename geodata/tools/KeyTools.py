try:
    from geodata.tools.StateTools import StateTools
    from geodata.tools.CountyTools import CountyTools
except ImportError:  # pragma: no cover - script execution fallback
    from tools.StateTools import StateTools
    from tools.CountyTools import CountyTools

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

###############################################################################
###############################################################################
# Content from older versions of geodata beyond this point
'''
IN PREVIOUS VERSIONS OF GEODATA:
Transforms place name strings to consistent hashes.

The challenge for joining data for geographies from different datasets is
that standarized identifiers are not necessarily provided. For example, the
the city of Adelanto in California might be identified as "Adelanto" in one
source, "Adelanto, CA" in other, "Adelanto city, California" in still
another, and "Adelanto, California" in yet another. This leaves us with
nothing to join the data together.

This file attempts to offer a solution to this problem through a function
gives all place names a common identifier.

For example, "Adelanto, California" will be hashed as "us:ca:adelanto".
'''

# # Define hardcoded hashes for exceptionally tricky geographic names.
# hardcoded_values = {
#     # Official name of Ventura is San Buenaventura. The official name is not
#     # used much outside of formal settings.
#     "San Buenaventura (Ventura) city, California" : "us:ca:ventura",
#     # The same for Paso Robles.
#     "El Paso de Robles (Paso Robles) city, California" : "us:ca:pasorobles",

#     # Green Acres CDP, California and Greenacres CDP, California differ only by
#     # a space. They are in Riverside and Kern counties, respectively.
#     "Green Acres CDP, California" : "us:ca:greenacres/cdp/riverside",
#     "Greenacres CDP, California" : "us:ca:greenacres/cdp/kern",

#     # There are two places in the U.S. with a comma in the name.
#     # We'll have to hardcode them right now.
#     "Islamorada, Village of Islands village; Florida" : 'us:fl:islamorada',
# }

# # For the time being, we will pass in arguments for the country and the
# # state/province.
# def key_hash(unhashed_string, country="us"):
#     # First of all, if the value is hardcoded, skip everything and return the
#     # hardcoded value.
#     if unhashed_string in hardcoded_values:
#         return hardcoded_values[unhashed_string]

#     # The beginning of the hashed string
#     hashed_string = country + ':'

#     # Declare a boolean which specifies whether the geography is a CDP.
#     cdp = False

#     # First of all, if the geography names contain a comma, split it on the
#     # comma.
#     comma_split = unhashed_string.split(", ")
#     # Get the first item
#     comma_split_first_item = comma_split[0]
#     # Get the last item, the state.
#     state = comma_split[-1]
#     # Transform the long state name into its lowercase abbreviation.
#     try:
#         state = states[state]
#     except KeyError:
#         print(unhashed_string)
#     # Add it to the output string
#     hashed_string += state + ':'

#     # Determine whether a geography is a CDP.
#     if "CDP" in comma_split_first_item:
#         cdp = True

#     # Remove unwanted words
#     cdp_removed = comma_split_first_item.replace('CDP', '')
#     city_removed = cdp_removed.replace('city', '')
#     town_removed = city_removed.replace('town', '')

#     # Remove whitespace
#     whitespace_removed = city_removed.replace(' ', '')

#     # Remove hyphens or dashes
#     dashes_removed = whitespace_removed.replace('-', '')

#     # To lower case
#     lowercase = whitespace_removed.lower()

#     # If the place is a CDP, attach the /cdp option.
#     if cdp:
#         hashed_string += lowercase + "/cdp"
#     # Otherwise, we're done.
#     else:
#         hashed_string += lowercase

#     return hashed_string
