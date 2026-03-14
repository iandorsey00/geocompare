import importlib.util
import sys
from pathlib import Path


def _load_fetch_overlays_module():
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    module_path = repo_root / "scripts" / "fetch_overlays.py"
    spec = importlib.util.spec_from_file_location("fetch_overlays", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


fetch_overlays = _load_fetch_overlays_module()


def test_parse_texas_voter_history_extracts_counties_and_statewide(tmp_path):
    source = Path(tmp_path) / "mar2026.shtml"
    source.write_text(
        """
        <html><body><pre>
        County  Precincts  Registered Voters  Suspense  Non-Suspense
        ANDERSON  20  31,500  1,200  30,300
        HARRIS  1000  2,800,000  120,000  2,680,000
        STATEWIDE TOTAL  9000  18,623,931  800,000  17,823,931
        </pre></body></html>
        """,
        encoding="utf-8",
    )

    rows = fetch_overlays._parse_texas_voter_history(str(source))
    by_geoid = {row["GEOID"]: row for row in rows}

    assert by_geoid["48001"]["registered_voters"] == 31500.0
    assert by_geoid["48201"]["registered_voters"] == 2800000.0
    assert by_geoid["0400000US48"]["registered_voters"] == 18623931.0


def test_merge_existing_rows_preserves_prior_voter_metrics(tmp_path):
    destination = Path(tmp_path) / "voter_data.csv"
    destination.write_text(
        (
            "GEOID,registered_voters,democratic_voters,republican_voters,other_voters\n"
            "06037,6000000,3200000,1800000,1000000\n"
        ),
        encoding="utf-8",
    )

    merged = fetch_overlays._merge_existing_rows(
        destination,
        [{"GEOID": "48001", "registered_voters": 31500.0}],
        [
            "GEOID",
            "registered_voters",
            "democratic_voters",
            "republican_voters",
            "other_voters",
        ],
    )
    by_geoid = {row["GEOID"]: row for row in merged}

    assert by_geoid["06037"]["democratic_voters"] == "3200000"
    assert by_geoid["06037"]["registered_voters"] == "6000000"
    assert by_geoid["48001"]["registered_voters"] == 31500.0


def test_parse_texas_press_release_extracts_statewide_total(tmp_path):
    source = Path(tmp_path) / "021726.shtml"
    source.write_text(
        """
        <html><body>
        <p>Secretary Jane Doe today announced Texas has 18,623,931 registered voters.</p>
        </body></html>
        """,
        encoding="utf-8",
    )

    rows = fetch_overlays._parse_texas_voter_press_release(str(source))

    assert rows == [
        {
            "GEOID": "0400000US48",
            "registered_voters": 18623931.0,
        }
    ]
