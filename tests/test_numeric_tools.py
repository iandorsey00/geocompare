import numpy as np

from geodata.tools.geodata_safedivision import gdsd
from geodata.tools.geodata_typecast import gdt, gdtf, gdti
from geodata.tools.numeric import parse_float, parse_int, parse_number, safe_divide


def test_parse_number_tolerant_cleanup():
    assert parse_number("$1,234") == 1234
    assert parse_number("-12.3%", as_type="float") == -12.3
    assert np.isnan(parse_number("abc"))


def test_legacy_typecast_wrappers_match_expected_behavior():
    assert gdt("42") == 42
    assert gdti("12.9") == 12
    assert gdtf("12.9%") == 12.9


def test_safe_divide_and_legacy_wrapper():
    assert safe_divide("10", "2") == 5
    assert safe_divide("10", "0", divide_by_zero=-1) == -1
    assert gdsd("6", "3") == 2
    assert np.isnan(gdsd("x", "3"))


def test_parse_int_float_defaults():
    assert parse_int("", default=0) == 0
    assert parse_float("", default=0.0) == 0.0
