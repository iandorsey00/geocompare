from types import SimpleNamespace

from geocompare.interfaces.cli import GeoCompareCLI


def _profile(name, latitude=37.7749, longitude=-122.4194):
    return SimpleNamespace(
        name=name,
        rc={"latitude": latitude, "longitude": longitude},
    )


def test_map_links_prints_expected_labels(capsys):
    cli = GeoCompareCLI.__new__(GeoCompareCLI)
    cli.engine = SimpleNamespace(get_dp=lambda display_label: [_profile(display_label)])
    cli._eprint = lambda msg: None

    cli.map_links(SimpleNamespace(display_label="San Francisco city, California"))

    out = capsys.readouterr().out
    assert "Open in Google Maps URL:" in out
    assert "Random Google Street View URL:" in out


def test_map_links_reports_missing_geography(capsys):
    cli = GeoCompareCLI.__new__(GeoCompareCLI)
    cli.engine = SimpleNamespace(get_dp=lambda display_label: (_ for _ in ()).throw(ValueError()))
    cli._eprint = print

    cli.map_links(SimpleNamespace(display_label="Missing"))

    err = capsys.readouterr().out
    assert "Sorry, there is no geography with that name." in err
