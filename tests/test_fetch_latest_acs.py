import importlib.util
import sys
from pathlib import Path


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
