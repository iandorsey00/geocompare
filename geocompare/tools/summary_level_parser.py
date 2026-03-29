from typing import Dict, Optional, Tuple


class SummaryLevelParser:
    """Parse summary-level context strings."""

    keyword_to_code: Dict[str, str]
    code_to_keyword: Dict[str, str]

    def __init__(self):
        self.keyword_to_code = {
            "nations": "010",
            "nation": "010",
            "countries": "010",
            "n": "010",
            "states": "040",
            "state": "040",
            "s": "040",
            "counties": "050",
            "county": "050",
            "c": "050",
            "tracts": "140",
            "tract": "140",
            "censustracts": "140",
            "censustract": "140",
            "t": "140",
            "places": "160",
            "place": "160",
            "p": "160",
            "cbsas": "310",
            "cbsa": "310",
            "cb": "310",
            "urbanareas": "400",
            "urbanarea": "400",
            "u": "400",
            "zctas": "860",
            "zcta": "860",
            "z": "860",
        }
        self.code_to_keyword = {
            "010": "nations",
            "050": "counties",
            "040": "states",
            "140": "tracts",
            "160": "places",
            "310": "cbsas",
            "400": "urbanareas",
            "860": "zctas",
        }

    def is_summary_level_keyword(self, input_str: str) -> bool:
        return input_str.lower() in self.keyword_to_code

    def is_summary_level_code(self, input_str: str) -> bool:
        return input_str in self.code_to_keyword

    def parse_context(self, context: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        universe_sl = None
        group_sl = None
        group = None
        if context:
            if "+" in context:
                universe_sl, group = context.split("+", 1)
                universe_sl = universe_sl.lower()
                if self.is_summary_level_keyword(universe_sl):
                    universe_sl = self.keyword_to_code[universe_sl]
                elif not self.is_summary_level_code(universe_sl):
                    raise ValueError("The context summary level is not valid.")
            else:
                group = context
        if group:
            if group.isdigit():
                group_sl = "860"
            elif ":" in context:
                group_sl = "050"
            else:
                group_sl = "040"
        return (universe_sl, group_sl, group)

    def unpack_context(self, context: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        return self.parse_context(context)

    def normalize_summary_level(self, input_str: str) -> str:
        normalized = "".join(ch for ch in str(input_str or "").lower() if ch.isalnum())
        if self.is_summary_level_keyword(normalized):
            return self.keyword_to_code[normalized]
        if self.is_summary_level_code(str(input_str or "")):
            return str(input_str)
        raise ValueError("The context summary level is not valid.")

    def parse_universes(self, raw_value: str):
        raw = str(raw_value or "").strip()
        if not raw:
            return None
        if raw.lower() == "all":
            return list(self.code_to_keyword.keys())

        selections = []
        for token in [part.strip() for part in raw.split(",") if part.strip()]:
            selections.append(self.normalize_summary_level(token))

        deduped = []
        for code in selections:
            if code not in deduped:
                deduped.append(code)
        return deduped
