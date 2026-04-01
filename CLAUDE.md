# UK Fuel Prices → Microreact

Python script that pulls live UK petrol station fuel prices from the GOV.UK Fuel Finder API and pushes them to an interactive Microreact map.

## Files

- `fuel_to_microreact.py` — main script
- `fuel-finder.env` — credentials (do not commit)
- `state/` — local cache (auto-created)
  - `stations.json` — full station dataset (~7,500 stations with prices, metadata, coordinates)
  - `microreact.json` — Microreact project ID and URL
  - `last_updated.txt` — ISO timestamp of last successful fetch
  - `cron.log` — cron output log

## Setup

No dependencies beyond Python stdlib.

Credentials in `fuel-finder.env`:
```
FUEL_FINDER_CLIENT_ID=...
FUEL_FINDER_CLIENT_SECRET=...
MICROREACT_API_TOKEN=...
```

## Usage

```bash
# First run — seed all data and create Microreact project
python3 fuel_to_microreact.py --full-load

# Dry run (fetches data but doesn't push to Microreact)
python3 fuel_to_microreact.py --full-load --dry-run

# Incremental update — fetches only price changes since last run
python3 fuel_to_microreact.py --update
```

## Cron

Runs `--update` every 15 minutes:
```
*/15 * * * * cd $HOME/code/petroleum.watch && python3 fuel_to_microreact.py --update >> state/cron.log 2>&1
```

## API Details

### Fuel Finder (GOV.UK)

- Auth: POST JSON `{client_id, client_secret}` to `/api/v1/oauth/generate_access_token` → Bearer token
- Station metadata (lat/lon, address, brand): `GET /api/v1/pfs?batch-number=N`
- Fuel prices: `GET /api/v1/pfs/fuel-prices?batch-number=N`
- Incremental prices: add `effective-start-timestamp=YYYY-MM-DD` param
- Paginated in batches of ~500. Keep incrementing `batch-number` until empty response.
- Behind CloudFront geo-restriction — **must run from a UK IP**.

### Microreact

- Auth: `Access-Token` header
- Create project: `POST /api/projects/create` with `.microreact` JSON body
- Update project: `POST /api/projects/update?project={id}` with `.microreact` JSON body
- Docs: https://docs.microreact.org/api/editing-projects

## CSV Output Columns

`id`, `name`, `brand_name`, `latitude`, `longitude`, `{fuel_type}_price`, `{fuel_type}_updated`, `postcode`, `city`, `county`, `address_line_1`, `is_motorway_service_station`, `is_supermarket_service_station`, `temporary_closure`, `permanent_closure`, `public_phone_number`

Fuel types: B7_STANDARD, B7_PREMIUM, B10, E10, E5, HVO (varies by station).
