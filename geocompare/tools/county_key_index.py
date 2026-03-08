from typing import Dict

from geocompare.tools.county_lookup import CountyLookup
from geocompare.tools.state_lookup import StateLookup


class CountyKeyIndex:
    """Build and decode county grouping keys used in context arguments."""

    key_to_county_name: Dict[str, str]
    county_name_to_key: Dict[str, str]

    def summary_level(self, key: str) -> str:
        components = key.split(":")
        return "050" if len(components) == 3 else "040"

    def __init__(self):
        st = StateLookup()
        ct = CountyLookup()
        self.key_to_county_name = {}
        self.county_name_to_key = {}
        for county_name in ct.county_names:
            parts = county_name.split(", ")
            county_stem = parts[0][:-7].replace(" ", "").lower()
            state = st.get_abbrev(parts[-1], lowercase=True)
            key = "us:" + state + ":" + county_stem + "/county"
            self.key_to_county_name[key] = county_name
            self.county_name_to_key[county_name] = key
