# FBI CDE Access Notes

Last validated: 2026-03-08 (America/Los_Angeles)

## Working Base

- `https://api.usa.gov/crime/fbi/cde/`

## Key Parameter

- Use query parameter `API_KEY=<key>` (uppercase) on working CDE routes.

## Verified Working Route

- State arrests time series:
  - `GET /crime/fbi/cde/arrest/state/{STATE_ABBR}/all`
  - Required query params:
    - `from=MM-YYYY`
    - `to=MM-YYYY`
    - `type=counts`
    - `API_KEY=<key>`

Example shape:

`https://api.usa.gov/crime/fbi/cde/arrest/state/CA/all?from=01-2023&to=12-2025&type=counts&API_KEY=...`

## Response Shape (State Arrests)

- Top-level keys:
  - `rates`
  - `actuals`
  - `tooltips`
  - `populations`
  - `cde_properties`
- `cde_properties.max_data_date.UCR` gives latest available month.

## Error Semantics Observed

- `403 {"message":"Missing Authentication Token"}`:
  - Route shape is not active at the gateway path (or wrong path).
- `400 Year and month range is missing or not valid`:
  - Route exists; required params are missing/invalid.
- `400 ... expected format MM-YYYY`:
  - `from`/`to` must be `MM-YYYY`.
- `400 Invalid 'type'`:
  - `type` query param is required and must be valid (`counts` works).

## Routes Not Working in This Environment

- `.../crime/fbi/cde/estimates/...` and `.../crime/fbi/cde/estimated/...` tested variants returned Missing Authentication Token.
- Legacy UCR base (`/crime/fbi/ucr/...`) returned 404 from decommissioned backend route.
