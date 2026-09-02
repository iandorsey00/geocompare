# GeoCompare Portfolio Capture

GeoCompare uses one primary terminal scene to show its signature analytical
workflow without turning the documentation into a screenshot gallery.

## Images

- `screenshots/01-remoteness-workflow.png`
  - Primary README image, `1440 x 620`.
  - Shows a real remoteness query ranking large places by distance from the
    nearest place below a per-capita-income threshold.
- `screenshots/social-preview.png`
  - `1440 x 720` composition for the GitHub repository social preview and LinkedIn.

Both images render unchanged stdout from the real CLI using public ACS-derived
fields. They are terminal transcript renders, not operating-system window
screenshots. No results, rankings, or values are invented for the images.
Only the separate social preview adds a product heading.

The fixed query uses per-capita income, population, county population, and
geographic distance. It does not select custom overlay metrics. Basic guards
reject known private-path and credential patterns; manual privacy review is
still required. Do not capture from a database containing private geography
names or modified base metrics.

## Capture

From the repository root, activate an environment containing GeoCompare and
run:

```bash
source .venv/bin/activate
python3 -m pip install -e ".[portfolio]"
python3 scripts/capture_portfolio.py
```

The capture runs the real CLI against `bin/default.sqlite`, then renders the
same workflow at two fixed sizes with Pillow, an optional portfolio-only
dependency. It draws at 2x resolution and downsamples for crisp text. There is
no browser, account, authentication session, animation, or external resource.
The CLI uses the C locale and UTC. Rendering is fixed to a dark terminal theme.

The default font is Menlo on macOS, DejaVu Sans Mono on Linux, or Consolas on
Windows. Set `PORTFOLIO_FONT` to an installed monospace TTF/TTC to override it.
Use the same font and Pillow version for pixel-identical rendering. The checked-in
images were captured with Menlo and Pillow 12.3.0. Font files are not redistributed.

Capture assumptions:

- a current `bin/default.sqlite` exists
- the data product contains standard ACS metrics
- no network access is required
- output remains deterministic for a given data product
- CLI errors, unexpected table shapes, and text that would be clipped stop capture

## Representative Query

```bash
geocompare query remoteness per_capita_income 40000 --universe places \
  --where "population>=50000" --county-population-min 500000 -n 8
```

This ranks candidate places with at least 50,000 residents in counties with at
least 500,000 residents by their distance from the nearest place below the
income threshold. The population filters restrict candidates, not the nearest
qualifying places. Distances are miles; income is per-capita dollars.

The PNGs are tied to the local data product used at capture time, not a promise
of current-year estimates. A later data build may change the ranking.

## Publish And Review

The README references the primary image. Upload `social-preview.png` in the
repository's GitHub social-preview settings when publishing; adding it to the
README does not configure that preview. This capture script does not publish
images, change GitHub settings, or post to LinkedIn.

Review both PNG files after recapturing. Refresh them after a material change
to the CLI output or the representative workflow, not for every routine
release.

Check the full table, including the rightmost distance and match-value columns,
and check the social image at thumbnail size. Confirm there are no usernames,
hostnames, paths, credentials, or private overlay values. A reduced screenshot
cannot replace the readable query and explanation above.
