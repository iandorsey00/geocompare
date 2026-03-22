from types import SimpleNamespace

from geocompare.interfaces.cli import GeoCompareCLI


def _profile(name, population, income, land_area):
    return SimpleNamespace(
        name=name,
        canonical_name=f"Census Tract {name}, Test County, California",
        sumlevel="140",
        rc={"population": population, "median_household_income": income, "land_area": land_area},
        fc={
            "population": f"{population:,}",
            "median_household_income": f"${income:,}",
            "land_area": f"{land_area:,.1f} sqmi",
        },
    )


def _build_cli(results):
    cli = GeoCompareCLI.__new__(GeoCompareCLI)
    cli.engine = SimpleNamespace(
        remoteness=lambda **kwargs: results,
        resolve_data_identifier=lambda data_identifier, fetch_one: {
            "display_store": "fc",
            "key": data_identifier,
        },
        _format_profile_component=lambda key, value: f"{value:,.1f} sqmi",
    )
    cli._normalize_scope_args = lambda args: None
    return cli


def test_remoteness_uses_compact_headers_by_default(capsys):
    candidate = _profile("9601, near Pahrump, Nye County, NV", 2645, 107903, 44.2)
    nearest = _profile("9707, near Hawthorne, Mineral County, NV", 3101, 66400, 18.0)
    cli = _build_cli([{"candidate": candidate, "nearest_match": nearest, "distance_miles": 123.6}])
    args = SimpleNamespace(
        context="tracts+",
        kilometers=False,
        official_labels=False,
        show_area=False,
        data_identifier="median_household_income",
    )

    cli.remoteness(args)
    output = capsys.readouterr().out

    assert "Pop" in output
    assert "Value" in output
    assert " Match Val" in output
    assert "Dist (mi)" in output
    assert "Area (sqmi)" not in output


def test_remoteness_can_show_area_column(capsys):
    candidate = _profile("9601, near Pahrump, Nye County, NV", 2645, 107903, 44.2)
    nearest = _profile("9707, near Hawthorne, Mineral County, NV", 3101, 66400, 18.0)
    cli = _build_cli([{"candidate": candidate, "nearest_match": nearest, "distance_miles": 123.6}])
    args = SimpleNamespace(
        context="tracts+",
        kilometers=False,
        official_labels=False,
        show_area=True,
        data_identifier="median_household_income",
    )

    cli.remoteness(args)
    output = capsys.readouterr().out

    assert "Area (sqmi)" in output
    assert "44.2" in output
    assert "44.2 sqmi" not in output


def test_remoteness_shows_area_in_square_kilometers_with_kilometers_flag(capsys):
    candidate = _profile("9601, near Pahrump, Nye County, NV", 2645, 107903, 44.2)
    nearest = _profile("9707, near Hawthorne, Mineral County, NV", 3101, 66400, 18.0)
    cli = _build_cli([{"candidate": candidate, "nearest_match": nearest, "distance_miles": 123.6}])
    args = SimpleNamespace(
        context="tracts+",
        kilometers=True,
        official_labels=False,
        show_area=True,
        data_identifier="median_household_income",
    )

    cli.remoteness(args)
    output = capsys.readouterr().out

    assert "Area (sqkm)" in output
    assert "Dist (km)" in output
    assert "114.5" in output
    assert "Area (sqmi)" not in output
