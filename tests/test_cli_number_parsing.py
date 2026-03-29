import argparse

import pytest

from geocompare.interfaces.cli import _parse_cli_float, _parse_cli_int


def test_parse_cli_int_accepts_commas():
    assert _parse_cli_int("1,000,000") == 1000000
    assert _parse_cli_int("15") == 15


def test_parse_cli_float_accepts_commas():
    assert _parse_cli_float("1,500.5") == 1500.5
    assert _parse_cli_float("42") == 42.0


def test_parse_cli_number_rejects_invalid_values():
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_cli_int("abc")

    with pytest.raises(argparse.ArgumentTypeError):
        _parse_cli_float("abc")
