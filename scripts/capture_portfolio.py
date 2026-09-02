#!/usr/bin/env python3
"""Render a real, public-metric CLI workflow into repeatable portfolio PNGs."""

import os
import shlex
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCREENSHOT_DIR = PROJECT_ROOT / "docs" / "portfolio" / "screenshots"
PRIMARY_PATH = SCREENSHOT_DIR / "01-remoteness-workflow.png"
SOCIAL_PATH = SCREENSHOT_DIR / "social-preview.png"
COMMAND_ARGS = [
    "query",
    "remoteness",
    "per_capita_income",
    "40000",
    "--universe",
    "places",
    "--where",
    "population>=50000",
    "--county-population-min",
    "500000",
    "-n",
    "8",
]
COMMAND = [sys.executable, "-m", "geocompare.interfaces.cli", *COMMAND_ARGS]
BLOCKED_TEXT = (
    "/Users/",
    "/home/",
    "password",
    "access_token",
    "api_key",
    "project_",
    "social_alignment",
)


def run_workflow():
    result = subprocess.run(
        COMMAND,
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
        env={**os.environ, "NO_COLOR": "1", "LC_ALL": "C", "TZ": "UTC"},
        timeout=180,
    )
    output = result.stdout.strip()
    for blocked in BLOCKED_TEXT:
        if blocked.lower() in output.lower():
            raise RuntimeError(f"Refusing to capture output containing blocked text: {blocked}")
    lines = output.splitlines()
    if len(lines) != 12 or "Nearest qualifying geography" not in lines[1]:
        raise RuntimeError("Expected a complete eight-result remoteness table; review CLI output.")
    if any(len(line) > 118 or not line.isascii() for line in lines):
        raise RuntimeError(
            "CLI output exceeds the reviewed capture layout; review before publishing."
        )
    return lines


def font_path():
    override = os.environ.get("PORTFOLIO_FONT")
    candidates = (
        [override]
        if override
        else [
            "/System/Library/Fonts/Menlo.ttc",
            "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
            "C:/Windows/Fonts/consola.ttf",
        ]
    )
    for candidate in candidates:
        if Path(candidate).is_file():
            return candidate
    raise RuntimeError("Set PORTFOLIO_FONT to an installed monospace TTF/TTC font.")


def render_image(output_lines, social=False):
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError as exc:
        raise RuntimeError(
            'Install the optional renderer: python3 -m pip install -e ".[portfolio]"'
        ) from exc

    scale = 2
    width, height = (1440, 720) if social else (1440, 620)
    canvas = Image.new("RGB", (width * scale, height * scale), "#e9ece8" if social else "#151918")
    draw = ImageDraw.Draw(canvas)
    selected_font = font_path()

    def text(line, x, y, size, color):
        font = ImageFont.truetype(selected_font, size=size * scale)
        box = draw.textbbox((x * scale, y * scale), line, font=font, anchor="lt")
        if box[2] > (width - 32) * scale or box[3] > (height - 32) * scale:
            raise RuntimeError("Text would be clipped; adjust the portfolio layout before capture.")
        draw.text((x * scale, y * scale), line, font=font, fill=color, anchor="lt")

    if social:
        text("GeoCompare", 40, 28, 38, "#18201d")
        text("Explore places through demographics and distance.", 40, 86, 20, "#53605b")
        draw.rounded_rectangle((64, 280, 2816, 1376), radius=24, fill="#151918")
        x, command_y, output_y, size, spacing = 64, 178, 266, 18, 29
    else:
        x, command_y, output_y, size, spacing = 56, 48, 158, 19, 31

    # Display the same fixed argument list used by the subprocess, without a host or path.
    split_at = COMMAND_ARGS.index("--where")
    command_lines = [
        "$ geocompare " + shlex.join(COMMAND_ARGS[:split_at]) + " \\",
        "    " + shlex.join(COMMAND_ARGS[split_at:]),
    ]
    for index, line in enumerate(command_lines):
        text(line, x, command_y + index * spacing, size, "#9fcf9f")
    for index, line in enumerate(output_lines):
        text(line, x, output_y + index * spacing, size, "#edf1ef")
    return canvas.resize((width, height), Image.Resampling.LANCZOS)


def main():
    output_lines = run_workflow()
    images = [
        (PRIMARY_PATH, render_image(output_lines)),
        (SOCIAL_PATH, render_image(output_lines, social=True)),
    ]
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    for path, rendered in images:
        rendered.save(path, format="PNG", optimize=True)
        print(f"Captured {path.relative_to(PROJECT_ROOT)} ({rendered.width} x {rendered.height})")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.SubprocessError) as exc:
        print(f"Portfolio capture failed: {exc}", file=sys.stderr)
        sys.exit(1)
