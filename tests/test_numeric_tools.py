import numpy as np

from geocompare.tools.numeric import parse_float, parse_int, parse_number, safe_divide


def test_parse_number_tolerant_cleanup():
    assert parse_number("$1,234") == 1234
    assert parse_number("-12.3%", as_type="float") == -12.3
    assert np.isnan(parse_number("abc"))
    assert np.isnan(parse_number("-666666666"))


def test_safe_divide():
    assert safe_divide("10", "2") == 5
    assert safe_divide("10", "0", divide_by_zero=-1) == -1
    assert safe_divide("6", "3") == 2
    assert np.isnan(safe_divide("x", "3"))


def test_parse_int_float_defaults():
    assert parse_int("", default=0) == 0
    assert parse_float("", default=0.0) == 0.0
