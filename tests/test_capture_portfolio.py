import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

SPEC = importlib.util.spec_from_file_location(
    "capture_portfolio", Path(__file__).resolve().parents[1] / "scripts" / "capture_portfolio.py"
)
capture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(capture)


def transcript():
    return "\n".join(
        [
            "-" * 114,
            " Candidate Pop Value Nearest qualifying geography Dist (mi) Match Val",
            "-" * 114,
        ]
        + [
            f" Example {index} city, California 100000 $50000 Example CDP 10.0 $30000"
            for index in range(8)
        ]
        + ["-" * 114]
    )


def test_capture_runs_fixed_cli_without_shell(monkeypatch):
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(stdout=transcript())

    monkeypatch.setattr(capture.subprocess, "run", run)
    assert len(capture.run_workflow()) == 12
    command, kwargs = calls[0]
    assert command == capture.COMMAND
    assert kwargs["check"] is True
    assert not kwargs.get("shell")
    assert kwargs["env"]["TZ"] == "UTC"
    assert kwargs["timeout"] == 180


@pytest.mark.parametrize("private_text", capture.BLOCKED_TEXT)
def test_capture_rejects_private_patterns(monkeypatch, private_text):
    monkeypatch.setattr(
        capture.subprocess,
        "run",
        lambda *a, **k: SimpleNamespace(stdout=transcript() + private_text),
    )
    with pytest.raises(RuntimeError, match="blocked text"):
        capture.run_workflow()


@pytest.mark.parametrize("output", ["", "Sorry, no geographies match your criteria."])
def test_capture_rejects_non_result_output(monkeypatch, output):
    monkeypatch.setattr(capture.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout=output))
    with pytest.raises(RuntimeError, match="complete eight-result"):
        capture.run_workflow()


@pytest.mark.parametrize("height,social", [(620, False), (720, True)])
def test_render_image_dimensions(height, social):
    pytest.importorskip("PIL")
    try:
        capture.font_path()
    except RuntimeError:
        pytest.skip("No capture font installed")
    rendered = capture.render_image(transcript().splitlines(), social=social)
    assert rendered.size == (1440, height)
    assert rendered.mode == "RGB"


def test_font_override_fails_instead_of_silently_substituting(monkeypatch):
    monkeypatch.setenv("PORTFOLIO_FONT", "/missing/font.ttf")
    with pytest.raises(RuntimeError, match="PORTFOLIO_FONT"):
        capture.font_path()


def test_render_rejects_clipped_output():
    pytest.importorskip("PIL")
    try:
        capture.font_path()
    except RuntimeError:
        pytest.skip("No capture font installed")
    with pytest.raises(RuntimeError, match="clipped"):
        capture.render_image(["x" * 300])
