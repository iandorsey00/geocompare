import json
from types import SimpleNamespace

from geocompare.interfaces.cli import GeoCompareCLI


def _cli_with_sources():
    cli = GeoCompareCLI.__new__(GeoCompareCLI)
    cli.engine = SimpleNamespace(
        sources=lambda: [
            {
                "key": "acs_5yr",
                "name": "American Community Survey 5-year estimates",
                "used_for": "Core demographic metrics",
                "provider": "U.S. Census Bureau",
                "notes": "Primary profile source.",
            }
        ]
    )
    return cli


def test_display_sources_table(capsys):
    cli = _cli_with_sources()
    cli.display_sources(SimpleNamespace(format="table"))

    out = capsys.readouterr().out
    assert "American Community Survey 5-year estimates" in out
    assert "Core demographic metrics" in out
    assert "Primary profile source." in out


def test_display_sources_json(capsys):
    cli = _cli_with_sources()
    cli.display_sources(SimpleNamespace(format="json"))

    out = json.loads(capsys.readouterr().out)
    assert out[0]["key"] == "acs_5yr"
