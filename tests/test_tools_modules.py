from geocompare.tools.county_key_index import CountyKeyIndex
from geocompare.tools.county_lookup import CountyLookup
from geocompare.tools.CountyTools import CountyTools
from geocompare.tools.KeyTools import KeyTools
from geocompare.tools.state_lookup import StateLookup
from geocompare.tools.StateTools import StateTools
from geocompare.tools.summary_level_parser import SummaryLevelParser
from geocompare.tools.SummaryLevelTools import SummaryLevelTools


def test_state_tools_get_abbrevs_is_not_mutating():
    st = StateLookup()
    base = st.get_abbrevs()
    with_us = st.get_abbrevs(inc_us=True)
    after = st.get_abbrevs()

    assert "US" not in base
    assert with_us[-1] == "US"
    assert "US" not in after


def test_state_tools_get_state_and_names():
    st = StateLookup()
    assert st.get_state("San Francisco city, California") == "California"
    assert st.get_state("Islamorada, Village of Islands village; Florida") == "Florida"
    assert st.get_abbrev("California") == "CA"
    assert st.get_abbrev("California", lowercase=True) == "ca"
    assert st.get_name("ca") == "California"


def test_key_tools_summary_level_and_maps():
    kt = CountyKeyIndex()
    assert kt.summary_level("us:ca:sanfrancisco/county") == "050"
    assert kt.summary_level("160") == "040"

    county_name = "San Francisco County, California"
    key = "us:ca:sanfrancisco/county"
    assert kt.county_name_to_key[county_name] == key
    assert kt.key_to_county_name[key] == county_name


def test_summary_level_tools_unpack_context():
    slt = SummaryLevelParser()
    assert slt.unpack_context("places+ca") == ("160", "040", "ca")
    assert slt.unpack_context("160+ca") == ("160", "040", "ca")
    assert slt.unpack_context("160+06075:county") == ("160", "050", "06075:county")
    assert slt.unpack_context("94103") == (None, "860", "94103")


def test_summary_level_tools_reject_invalid_universe():
    slt = SummaryLevelParser()
    try:
        slt.unpack_context("invalid+ca")
    except ValueError:
        return
    assert False, "Expected ValueError for invalid summary level context"


def test_county_tools_data_loaded():
    ct = CountyLookup()
    assert ct.county_name_to_geoid["San Francisco County, California"] == "06075"
    assert ct.county_geoid_to_name["06075"] == "San Francisco County, California"


def test_legacy_tool_aliases_still_work():
    assert StateTools().get_abbrev("California") == "CA"
    assert CountyTools().county_name_to_geoid["San Francisco County, California"] == "06075"
    assert KeyTools().summary_level("us:ca:sanfrancisco/county") == "050"
    assert SummaryLevelTools().unpack_context("places+ca") == ("160", "040", "ca")
