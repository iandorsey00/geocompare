from typing import Dict, List


class StateLookup:
    """Tools to convert state names to abbreviations and vice versa."""

    geoid_to_name: Dict[str, str]
    name_to_geoid: Dict[str, str]
    abbrevs: List[str]
    name_to_abbrev: Dict[str, str]
    state_abbrev_to_name: Dict[str, str]

    def get_state(self, geo_name: str) -> str:
        """Get the state name given a Census display label."""
        if "; " in geo_name:
            parts = geo_name.split("; ")
        else:
            parts = geo_name.split(", ")
        return parts[-1]

    def extract_state_from_label(self, geo_name: str) -> str:
        """Explicit alias for get_state()."""
        return self.get_state(geo_name)

    def get_abbrevs(self, lowercase: bool = False, inc_us: bool = False) -> List[str]:
        """Get two-letter state abbreviations."""
        abbrevs = list(self.abbrevs)
        if inc_us and "US" not in abbrevs:
            abbrevs.append("US")
        if lowercase:
            return [abbr.lower() for abbr in abbrevs]
        return abbrevs

    def list_state_abbreviations(self, lowercase: bool = False, inc_us: bool = False) -> List[str]:
        """Explicit alias for get_abbrevs()."""
        return self.get_abbrevs(lowercase=lowercase, inc_us=inc_us)

    def get_abbrev(self, name: str, lowercase: bool = False) -> str:
        """Transform a state name into its abbreviation."""
        abbrev = self.name_to_abbrev[name]
        if lowercase:
            return abbrev.lower()
        return abbrev

    def get_name(self, abbrev: str) -> str:
        """Transform a state abbreviation into its name."""
        return self.state_abbrev_to_name[abbrev.upper()]

    def __init__(self):
        self.geoid_to_name = {
            "01": "Alabama",
            "02": "Alaska",
            "04": "Arizona",
            "05": "Arkansas",
            "06": "California",
            "08": "Colorado",
            "09": "Connecticut",
            "10": "Delaware",
            "11": "District of Columbia",
            "12": "Florida",
            "13": "Georgia",
            "15": "Hawaii",
            "16": "Idaho",
            "17": "Illinois",
            "18": "Indiana",
            "19": "Iowa",
            "20": "Kansas",
            "21": "Kentucky",
            "22": "Louisiana",
            "23": "Maine",
            "24": "Maryland",
            "25": "Massachusetts",
            "26": "Michigan",
            "27": "Minnesota",
            "28": "Mississippi",
            "29": "Missouri",
            "30": "Montana",
            "31": "Nebraska",
            "32": "Nevada",
            "33": "New Hampshire",
            "34": "New Jersey",
            "35": "New Mexico",
            "36": "New York",
            "37": "North Carolina",
            "38": "North Dakota",
            "39": "Ohio",
            "40": "Oklahoma",
            "41": "Oregon",
            "42": "Pennsylvania",
            "44": "Rhode Island",
            "45": "South Carolina",
            "46": "South Dakota",
            "47": "Tennessee",
            "48": "Texas",
            "49": "Utah",
            "50": "Vermont",
            "51": "Virginia",
            "53": "Washington",
            "54": "West Virginia",
            "55": "Wisconsin",
            "56": "Wyoming",
            "60": "American Samoa",
            "66": "Guam",
            "69": "Commonwealth of the Northern Mariana Islands",
            "72": "Puerto Rico",
            "78": "United States Virgin Islands",
        }
        self.name_to_geoid = {name: geoid for geoid, name in self.geoid_to_name.items()}
        self.abbrevs = [
            "AL","AK","AZ","AR","CA","CO","CT","DC","DE","FL","GA","HI","ID","IL","IN","IA",
            "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM",
            "NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA",
            "WV","WI","WY",
        ]
        self.name_to_abbrev = {
            "Alaska": "AK","Alabama": "AL","Arkansas": "AR","American Samoa": "AS","Arizona": "AZ",
            "California": "CA","Colorado": "CO","Connecticut": "CT","District of Columbia": "DC",
            "Delaware": "DE","Florida": "FL","Georgia": "GA","Guam": "GU","Hawaii": "HI",
            "Iowa": "IA","Idaho": "ID","Illinois": "IL","Indiana": "IN","Kansas": "KS",
            "Kentucky": "KY","Louisiana": "LA","Massachusetts": "MA","Maryland": "MD","Maine": "ME",
            "Michigan": "MI","Minnesota": "MN","Missouri": "MO","Northern Mariana Islands": "MP",
            "Mississippi": "MS","Montana": "MT","National": "NA","North Carolina": "NC",
            "North Dakota": "ND","Nebraska": "NE","New Hampshire": "NH","New Jersey": "NJ",
            "New Mexico": "NM","Nevada": "NV","New York": "NY","Ohio": "OH","Oklahoma": "OK",
            "Oregon": "OR","Pennsylvania": "PA","Puerto Rico": "PR","Rhode Island": "RI",
            "South Carolina": "SC","South Dakota": "SD","Tennessee": "TN","Texas": "TX",
            "Utah": "UT","Virginia": "VA","Virgin Islands": "VI","Vermont": "VT","Washington": "WA",
            "Wisconsin": "WI","West Virginia": "WV","Wyoming": "WY",
        }
        self.state_abbrev_to_name = {abbrev: name for name, abbrev in self.name_to_abbrev.items()}
