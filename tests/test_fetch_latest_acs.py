import importlib.util
import sys
from pathlib import Path

from geocompare.database.Database import Database


def _load_fetch_latest_acs_module():
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    module_path = repo_root / "scripts" / "fetch_latest_acs.py"
    spec = importlib.util.spec_from_file_location("fetch_latest_acs", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


fetch_latest_acs = _load_fetch_latest_acs_module()


def test_discover_latest_acs_year_uses_layout_probe_fallback(monkeypatch):
    monkeypatch.setattr(
        fetch_latest_acs,
        "fetch_text",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("no listing")),
    )
    monkeypatch.setattr(
        fetch_latest_acs,
        "time",
        type("TimeStub", (), {"gmtime": staticmethod(lambda: type("Gm", (), {"tm_year": 2026})())}),
    )
    monkeypatch.setattr(
        fetch_latest_acs,
        "detect_acs_layout_for_year",
        lambda year: "table" if year == 2024 else None,
    )

    assert fetch_latest_acs.discover_latest_acs_year() == "2024"


def test_progress_line_supports_prefix():
    line = fetch_latest_acs.progress_line("example.dat", 50, 100, prefix="[2/10]")
    assert line.startswith("[2/10] example.dat")


def test_is_table_based_acs_year_uses_directory_listing_when_available(monkeypatch):
    def fake_fetch_text(url, **kwargs):
        if url.endswith("/documentation/"):
            return "Geos20245YR.txt"
        if url.endswith("/data/5YRData/"):
            return "acsdt5y2024-b01003.dat"
        raise RuntimeError("unexpected url")

    monkeypatch.setattr(fetch_latest_acs, "fetch_text", fake_fetch_text)
    monkeypatch.setattr(fetch_latest_acs, "url_exists", lambda *args, **kwargs: False)

    assert fetch_latest_acs.is_table_based_acs_year(2024) is True


def test_main_explicit_year_tries_table_bootstrap_before_layout_detection(monkeypatch, tmp_path):
    out_dir = Path(tmp_path) / "data"
    monkeypatch.setattr(
        fetch_latest_acs.sys,
        "argv",
        [
            "fetch_latest_acs.py",
            "--out-dir",
            str(out_dir),
            "--acs-year",
            "2024",
            "--dry-run",
        ],
    )
    monkeypatch.setattr(fetch_latest_acs, "install_signal_handlers", lambda: None)
    monkeypatch.setattr(fetch_latest_acs, "parse_states", lambda raw: ["us"])
    monkeypatch.setattr(fetch_latest_acs, "discover_latest_gazetteer_year", lambda: "2025")
    monkeypatch.setattr(
        fetch_latest_acs,
        "fetch_table_geography_file",
        lambda *args, **kwargs: "planned",
    )
    monkeypatch.setattr(
        fetch_latest_acs,
        "fetch_table_data_file",
        lambda *args, **kwargs: "planned",
    )
    monkeypatch.setattr(fetch_latest_acs, "fetch_gazetteer_file", lambda *args, **kwargs: "planned")
    monkeypatch.setattr(fetch_latest_acs, "verify_required_files", lambda *args, **kwargs: None)

    exit_code = fetch_latest_acs.main()

    assert exit_code == 0


def test_fetch_table_geography_file_attempts_download_without_url_exists(monkeypatch, tmp_path):
    dest = Path(tmp_path) / "Geos20245YR.txt"
    seen = []

    def fake_download(url, *args, **kwargs):
        seen.append(url)
        if url.endswith("/documentation/Geos20245YR.txt"):
            return "planned"
        raise fetch_latest_acs.DownloadError("nope")

    monkeypatch.setattr(fetch_latest_acs, "download_file", fake_download)
    monkeypatch.setattr(fetch_latest_acs, "url_exists", lambda *args, **kwargs: False)

    result = fetch_latest_acs.fetch_table_geography_file(
        "2024",
        dest,
        overwrite=False,
        resume=True,
        dry_run=True,
        timeout=60,
        max_attempts=2,
    )

    assert result == "planned"
    assert seen


def test_normalize_tract_gazetteer_rows_matches_current_gazetteer_shape():
    db = Database.__new__(Database)
    rows = db.normalize_tract_gazetteer_rows(
        [
            ["USPS", "GEOID", "GEOIDFQ", "ALAND", "AWATER", "ALAND_SQMI", "AWATER_SQMI", "INTPTLAT", "INTPTLONG"],
            ["AL", "01001020100", "1400000US01001020100", "9825303", "28435", "3.794", "0.011", "32.4819731", "-86.4915648"],
        ]
    )

    assert rows[0][:5] == ["USPS", "GEOID", "GEOIDFQ", "ANSICODE", "NAME"]
    assert rows[1][0] == "AL"
    assert rows[1][1] == "1400000US01001020100"
    assert rows[1][2] == "1400000US01001020100"
    assert rows[1][7] == "9825303"
    assert rows[1][11] == "32.4819731"
