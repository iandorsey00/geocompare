from types import SimpleNamespace

from geocompare.interfaces.cli import GeoCompareCLI


class _DummyGeoVector:
    def __init__(self, row_text):
        self.row_text = row_text
        self.sumlevel = "050"
        self.name = "Fairfax County, Virginia"

    def display_row(self, mode):
        return self.row_text

    def distance(self, other, mode="std"):
        return 0.0 if self is other else 1.23


def _build_cli(results):
    cli = GeoCompareCLI.__new__(GeoCompareCLI)
    cli.engine = SimpleNamespace(compare_geovectors=lambda **kwargs: results)
    cli._normalize_scope_args = lambda args: None
    return cli


def _run_similarity(mode, capsys):
    row = (
        "Fairfax County, Virginia".ljust(40)[:40]
        + " "
        + "Fairfax County".ljust(20)[:20]
        + " "
        + "1,147,837".rjust(11)
        + " 100 100  51  55  61  58 "
    )
    if mode == "std":
        row += " 50  52 "
    gv = _DummyGeoVector(row)
    cli = _build_cli([gv])
    args = SimpleNamespace(context="", official_labels=False, display_label=gv.name, n=15)
    cli.compare_geovectors(args, mode=mode)
    return capsys.readouterr().out.splitlines()


def test_standard_geovector_divider_matches_header_width(capsys):
    lines = _run_similarity("std", capsys)
    divider = lines[2]
    header = lines[3]

    assert set(divider) == {"-"}
    assert len(divider) == len(header)


def test_built_form_geovector_divider_matches_header_width(capsys):
    lines = _run_similarity("app", capsys)
    divider = lines[2]
    header = lines[3]

    assert set(divider) == {"-"}
    assert len(divider) == len(header)
