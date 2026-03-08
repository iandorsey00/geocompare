from typing import Dict, List

from geocompare.tools.data.county_geoid_to_name import county_geoid_to_name
from geocompare.tools.data.county_name_to_geoid import county_name_to_geoid
from geocompare.tools.data.county_names import county_names
from geocompare.tools.data.county_to_places import county_to_places
from geocompare.tools.data.place_to_counties import place_to_counties


class CountyLookup:
    """Lookup county/place relationships using generated static datasets."""

    county_geoid_to_name: Dict[str, str]
    county_name_to_geoid: Dict[str, str]
    county_names: List[str]
    county_to_places: Dict[str, List[str]]
    place_to_counties: Dict[str, List[str]]

    def __init__(self):
        self.county_geoid_to_name = county_geoid_to_name
        self.county_name_to_geoid = county_name_to_geoid
        self.county_names = county_names
        self.county_to_places = county_to_places
        self.place_to_counties = place_to_counties
