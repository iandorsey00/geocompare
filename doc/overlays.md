# Overlays

GeoCompare supports two overlay types:

- built-in overlays: canonical crime and voter files
- custom overlays: private or user-defined metrics

Overlay files live under:

```text
<data_dir>/overlays/
```

Canonical filenames:

- `crime_data.csv`
- `voter_data.csv`
- `project_data.csv`

## Built-in overlays

You can normalize raw sources into canonical overlay files with:

```bash
python3 scripts/fetch_overlays.py \
  --out-dir /path/to/data \
  --crime-source /path/or/url/to/crime.csv \
  --voter-source /path/or/url/to/voter.csv
```

`scripts/fetch_overlays.py` merges into existing canonical overlay files, so
state-by-state voter imports can accumulate over time instead of overwriting
earlier rows.

Built-in voter overlays may be partial. If a source only publishes total
registered voters, GeoCompare can still import `registered_voters` without any
party-breakout columns.

## Custom overlays

For custom overlays:

- keep one canonical CSV with `GEOID` and numeric metric columns
- prefer `project_` prefixes for private or custom identifiers
- use `_pct` suffix for percentages

Example:

```csv
GEOID,project_custom_score,project_custom_confidence_pct
06037,63.2,91.5
06073,58.8,88.1
```

## Overlay manifests

Optional manifest files:

- `overlays/overlay_manifest.json`
- `overlays/manifest.json`

Manifest metadata can control:

- metric labels
- profile section placement
- display ordering
- formatting hints

Example:

```json
{
  "overlay": "custom-overlay",
  "metrics": [
    {
      "key": "project_custom_score",
      "label": "Custom score",
      "section": "PROJECT DATA",
      "type": "score"
    },
    {
      "key": "project_custom_confidence_pct",
      "label": "Custom confidence",
      "section": "PROJECT DATA",
      "type": "pct"
    }
  ]
}
```

`overlay_manifest.json` is optional, but recommended for stable naming, labels,
and section placement.

## Base-only rebuild

To return to a clean base state without private overlays:

1. Fetch ACS and gazetteer data.
2. Optionally refresh canonical crime and voter overlays.
3. Remove or relocate `project_data.csv` and any private overlay manifest.
4. Rebuild GeoCompare.

```bash
python3 scripts/fetch_latest_acs.py --out-dir /path/to/data --archive-existing
python3 scripts/fetch_overlays.py \
  --out-dir /path/to/data \
  --crime-source /path/or/url/to/crime.csv \
  --voter-source /path/or/url/to/voter.csv
python3 -m geocompare.interfaces.cli build /path/to/data
```
