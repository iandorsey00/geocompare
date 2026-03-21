from types import SimpleNamespace

from geocompare.interfaces.cli import GeoCompareCLI


def _profile(name, population, income):
    return SimpleNamespace(
        name=name,
        canonical_name=f"Census Tract {name}, Test County, California",
        sumlevel="140",
        counties=["06001"],
        rc={"population": population, "median_household_income": income},
        fc={
            "population": f"{population:,}",
            "median_household_income": f"${income:,}",
        },
    )


def _build_cli(results):
    cli = GeoCompareCLI.__new__(GeoCompareCLI)
    cli.engine = SimpleNamespace(
        local_average=lambda **kwargs: results,
        resolve_data_identifier=lambda data_identifier, fetch_one: {
            "display_store": "fc",
            "key": data_identifier,
        },
        _format_profile_component=lambda key, value: f"${int(round(value)):,}",
    )
    cli._normalize_scope_args = lambda args: None
    return cli


def test_local_average_uses_expected_headers(capsys):
    candidate = _profile("9601, near Pahrump, Nye County, NV", 2645, 107903)
    cli = _build_cli(
        [{"candidate": candidate, "local_average": 101250, "neighbor_span_miles": 12.4}]
    )
    args = SimpleNamespace(
        context="tracts+",
        kilometers=False,
        official_labels=False,
        data_identifier="median_household_income",
        n=15,
    )

    cli.local_average(args)
    output = capsys.readouterr().out

    assert "Candidate" in output
    assert "Pop" in output
    assert "Value" in output
    assert "Local Avg" in output
    assert "Span (mi)" in output
