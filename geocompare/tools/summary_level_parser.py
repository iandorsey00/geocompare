from typing import Dict, Optional, Tuple


class SummaryLevelParser:
    """Parse summary-level context strings."""

    keyword_to_code: Dict[str, str]
    code_to_keyword: Dict[str, str]

    def __init__(self):
        self.keyword_to_code = {
            "states": "040",
            "s": "040",
            "counties": "050",
            "c": "050",
            "places": "160",
            "p": "160",
            "cbsas": "310",
            "cb": "310",
            "urbanareas": "400",
            "u": "400",
            "zctas": "860",
            "z": "860",
        }
        self.code_to_keyword = {
            "050": "counties",
            "040": "states",
            "160": "places",
            "310": "cbsas",
            "400": "urbanareas",
            "860": "zctas",
        }

    def is_summary_level_keyword(self, input_str: str) -> bool:
        return input_str in self.keyword_to_code

    def is_summary_level_code(self, input_str: str) -> bool:
        return input_str in self.code_to_keyword

    def parse_context(self, context: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        universe_sl = None
        group_sl = None
        group = None
        if context:
            if "+" in context:
                universe_sl, group = context.split("+", 1)
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
