#!/usr/bin/env python3
"""
fuel_to_microreact.py — Sync UK fuel prices from the GOV.UK Fuel Finder API to Microreact.

Two modes:
  --full-load   Fetch all stations + prices, save locally, create/overwrite Microreact project.
  --update      Fetch incremental price changes, merge into local state, re-push to Microreact.

State is kept in ~/.claude/fuel-finder-state/
  stations.json       — master copy of all station data keyed by station ID
  microreact.json     — Microreact project ID and URL
  last_updated.txt    — ISO timestamp of last successful incremental fetch

Setup:
  1. Add credentials to ~/.claude/fuel-finder.env:
       FUEL_FINDER_CLIENT_ID=...
       FUEL_FINDER_CLIENT_SECRET=...
       MICROREACT_API_TOKEN=...
  2. Run: python3 fuel_to_microreact.py --full-load
  3. Then run: python3 fuel_to_microreact.py --update  (on a schedule, e.g. every 15 min)
"""

import argparse
import csv
import io
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration — update if needed
# ---------------------------------------------------------------------------

FUEL_FINDER_TOKEN_URL = "https://www.fuel-finder.service.gov.uk/api/v1/oauth/generate_access_token"
FUEL_FINDER_API_BASE  = "https://www.fuel-finder.service.gov.uk/api/v1"
FUEL_FINDER_SCOPE     = "fuelfinder.read"

MICROREACT_API_BASE   = "https://microreact.org/api"

PROJECT_DIR = Path.home() / "code" / "petroleum.watch"
ENV_FILE   = PROJECT_DIR / "fuel-finder.env"
STATE_DIR  = PROJECT_DIR / "state"
STATIONS_FILE      = STATE_DIR / "stations.json"
MICROREACT_FILE    = STATE_DIR / "microreact.json"
LAST_UPDATED_FILE  = STATE_DIR / "last_updated.txt"
PRICE_HISTORY_FILE = STATE_DIR / "price_history.csv"


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def load_env():
    env = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    for key in ("FUEL_FINDER_CLIENT_ID", "FUEL_FINDER_CLIENT_SECRET", "MICROREACT_API_TOKEN"):
        if os.environ.get(key):
            env[key] = os.environ[key]
    return env


# ---------------------------------------------------------------------------
# Fuel Finder API
# ---------------------------------------------------------------------------

def get_token(client_id, client_secret):
    """Obtain a Bearer token. API expects JSON body (not OAuth2 form-encoded)."""
    data = json.dumps({"client_id": client_id, "client_secret": client_secret}).encode()
    req = urllib.request.Request(
        FUEL_FINDER_TOKEN_URL, data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.load(resp)
        # Response: {"success": true, "data": {"access_token": "...", ...}}
        return result["data"]["access_token"]
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        cache = e.headers.get("X-Cache", "")
        print(f"Token request failed HTTP {e.code} (X-Cache: {cache}): {body[:300]}")
        if "cloudfront" in cache.lower():
            print("CloudFront is blocking this request -- likely a geo/IP restriction.")
            print("Run this script from a UK IP address.")
        sys.exit(1)


def api_get(path, token, params=None, allow_404=False):
    url = f"{FUEL_FINDER_API_BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {token}"}, method="GET"
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        if allow_404 and e.code == 404:
            return []
        print(f"API error {e.code}: GET {url}")
        print(e.read().decode(errors="replace")[:300])
        sys.exit(1)


def extract_records(data):
    """Pull the list of station records from whatever shape the API returns."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("results", "data", "prices", "items", "stations",
                    "PetrolStations", "petrolStations", "content"):
            if key in data and isinstance(data[key], list):
                return data[key]
    return []


def is_last_batch(data, records):
    if isinstance(data, dict):
        if data.get("last") is True:
            return True
        total = data.get("totalBatches") or data.get("total_batches") or data.get("totalPages")
        page  = data.get("number") or data.get("page")
        if total and page is not None and int(page) >= int(total) - 1:
            return True
    # Heuristic: small batch = probably the last one
    return len(records) < 50


def fetch_station_metadata(token):
    """Fetch station metadata (incl. lat/lon) via paginated /pfs endpoint."""
    all_records = []
    batch = 1
    while True:
        print(f"  Metadata batch {batch}...", end=" ", flush=True)
        data = api_get("/pfs", token, {"batch-number": batch}, allow_404=True)
        records = extract_records(data)
        if not records:
            print("empty — done.")
            break
        all_records.extend(records)
        print(f"{len(records)} records (total: {len(all_records)})")
        if is_last_batch(data, records):
            break
        batch += 1
        if batch > 500:
            print("Metadata batch safety limit reached (500).")
            break
    return all_records


def merge_metadata_into_stations(stations_dict, metadata_records):
    """Merge station metadata (location, address, brand, etc.) into stations dict."""
    merged = 0
    for m in metadata_records:
        sid = str(m.get("node_id", ""))
        if not sid:
            continue
        if sid in stations_dict:
            # Merge all metadata fields into the existing station record
            for k, v in m.items():
                if k != "fuel_prices":
                    stations_dict[sid][k] = v
            merged += 1
        else:
            # Station exists in metadata but not in prices — add it anyway
            stations_dict[sid] = m
    return merged


def fetch_incremental_metadata(token, since_date):
    """Fetch station metadata changes since the given date (YYYY-MM-DD)."""
    all_records = []
    batch = 1
    while True:
        print(f"  Incremental metadata batch {batch} (since {since_date})...", end=" ", flush=True)
        data = api_get("/pfs", token, {
            "effective-start-timestamp": since_date,
            "batch-number": batch,
        }, allow_404=True)
        records = extract_records(data)
        if not records:
            print("empty — done.")
            break
        all_records.extend(records)
        print(f"{len(records)} records")
        if is_last_batch(data, records):
            break
        batch += 1
        if batch > 500:
            break
    return all_records


def fetch_full(token):
    """Fetch all stations via paginated /pfs/fuel-prices endpoint."""
    all_records = []
    batch = 1
    while True:
        print(f"  Batch {batch}...", end=" ", flush=True)
        data = api_get("/pfs/fuel-prices", token, {"batch-number": batch})
        records = extract_records(data)
        if not records:
            print("empty — done.")
            break
        all_records.extend(records)
        print(f"{len(records)} records (total: {len(all_records)})")
        if is_last_batch(data, records):
            break
        batch += 1
        if batch > 500:
            print("Batch safety limit reached (500).")
            break
    return all_records


def fetch_incremental(token, since_date):
    """Fetch price changes since the given date (YYYY-MM-DD).
    Uses the same /pfs/fuel-prices endpoint with effective-start-timestamp.
    Paginates through all batches for that date range.
    """
    all_records = []
    batch = 1
    while True:
        print(f"  Incremental batch {batch} (since {since_date})...", end=" ", flush=True)
        data = api_get("/pfs/fuel-prices", token, {
            "effective-start-timestamp": since_date,
            "batch-number": batch,
        }, allow_404=True)
        records = extract_records(data)
        if not records:
            print("empty — done.")
            break
        all_records.extend(records)
        print(f"{len(records)} records")
        if is_last_batch(data, records):
            break
        batch += 1
        if batch > 500:
            break
    return all_records


# ---------------------------------------------------------------------------
# Local state
# ---------------------------------------------------------------------------

def load_stations():
    if STATIONS_FILE.exists():
        return json.loads(STATIONS_FILE.read_text())
    return {}


def save_stations(stations_dict):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATIONS_FILE.write_text(json.dumps(stations_dict, indent=2))


def records_to_dict(records):
    """Convert list of station records to dict keyed by node_id."""
    result = {}
    for r in records:
        sid = str(r.get("node_id", ""))
        if sid:
            result[sid] = r
    return result


def merge_updates(stations, updates):
    """Apply incremental updates: merge fuel_prices arrays by fuel type."""
    updated = dict(stations)
    changed = 0
    for r in updates:
        sid = str(r.get("node_id", ""))
        if not sid:
            continue
        if sid in updated:
            # Merge fuel_prices: update by fuel type key
            existing_prices = {
                fp.get("fuel_type") or fp.get("fuelType", "unknown"): fp
                for fp in updated[sid].get("fuel_prices", [])
            }
            for fp in r.get("fuel_prices", []):
                ft = fp.get("fuel_type") or fp.get("fuelType", "unknown")
                existing_prices[ft] = fp
            updated[sid]["fuel_prices"] = list(existing_prices.values())
            # Update top-level fields (trading_name, phone, etc.)
            for k, v in r.items():
                if k != "fuel_prices":
                    updated[sid][k] = v
        else:
            updated[sid] = r
        changed += 1
    return updated, changed


def snapshot_prices(stations_dict):
    """Append current prices to the history CSV. Only records rows where the
    price differs from the last recorded price for that station+fuel combo."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Load last known prices from the tail of the history file
    last_prices = {}  # (station_id, fuel_type) -> price
    if PRICE_HISTORY_FILE.exists():
        with open(PRICE_HISTORY_FILE, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (row.get("station_id", ""), row.get("fuel_type", ""))
                last_prices[key] = row.get("price", "")

    new_rows = []
    for sid, station in stations_dict.items():
        name = station.get("trading_name", "")
        brand = station.get("brand_name", "")
        for fp in station.get("fuel_prices", []):
            ft = fp.get("fuel_type") or fp.get("fuelType", "")
            price = fp.get("price") or fp.get("pump_price") or fp.get("priceInPence", "")
            if not ft or not price:
                continue
            price_str = str(price)
            if last_prices.get((sid, ft)) != price_str:
                new_rows.append({
                    "timestamp": now,
                    "station_id": sid,
                    "station_name": name,
                    "brand": brand,
                    "fuel_type": ft,
                    "price": price_str,
                })

    if not new_rows:
        print("No price changes to record in history.")
        return

    fieldnames = ["timestamp", "station_id", "station_name", "brand", "fuel_type", "price"]
    write_header = not PRICE_HISTORY_FILE.exists()
    with open(PRICE_HISTORY_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)
    print(f"Recorded {len(new_rows)} price changes to history.")


# ---------------------------------------------------------------------------
# Build Microreact CSV
# ---------------------------------------------------------------------------

def collect_fuel_types(stations_dict):
    """Collect all fuel type keys present across all stations."""
    fuel_types = set()
    for station in stations_dict.values():
        for fp in station.get("fuel_prices", []):
            ft = fp.get("fuel_type") or fp.get("fuelType", "")
            if ft:
                fuel_types.add(ft)
    return sorted(fuel_types)


# Postcode areas that are always west of Greenwich (longitude should be negative).
# Excludes areas that straddle or sit east of the meridian (CT, DA, ME, IG, RM,
# BR, TN, SE, E, EC, BN — these have legitimate positive longitudes).
_WESTERN_POSTCODES = {
    # Northern Ireland
    "BT",
    # Scotland
    "PA", "KA", "KW", "IV", "HS", "PH", "FK", "G", "DG", "ML", "EH", "KY",
    "DD", "AB", "TD",
    # Wales
    "LL", "SA", "CF", "NP", "SY", "LD",
    # South West England
    "PL", "TR", "EX", "TQ", "TA", "BS", "BA", "BH", "DT", "SP", "GL",
    # West Midlands / North West
    "HR", "WR", "B", "CV", "WV", "WS", "DY", "ST", "CW", "CH",
    "M", "SK", "OL", "BL", "WN", "WA", "L", "PR", "FY", "BB",
    # North England
    "LA", "CA", "NE", "DH", "SR", "TS", "DL", "HG",
    "BD", "HX", "HD", "WF", "LS", "HU", "YO",
    "S", "DN", "DE", "NG", "LE", "NN",
    # South / Central (clearly west of Greenwich)
    "SN", "SO", "RG", "OX", "HP", "MK", "LU", "SL", "GU", "PO",
    "SW", "W", "WC", "N", "NW",
    "TW", "KT", "CR", "SM", "HA", "UB", "WD", "AL",
}


def stations_to_csv(stations_dict):
    """Build Microreact CSV with one row per station, price columns per fuel type."""
    records = list(stations_dict.values())
    if not records:
        print("No station data.")
        sys.exit(1)

    # Normalise brand names: uppercase, merge variants, bucket small brands
    from collections import Counter as _Counter
    brand_counts = _Counter()
    for r in records:
        brand_counts[r.get("brand_name", "").strip().upper()] += 1
    top_brands = {b for b, c in brand_counts.items() if c >= 15}
    def normalise_brand(raw):
        upper = raw.strip().upper()
        if not upper:
            return "INDEPENDENT"
        return upper if upper in top_brands else "INDEPENDENT"

    sample = records[0]
    print(f"Station fields: {[k for k in sample.keys() if k != 'fuel_prices']}")
    if sample.get("fuel_prices"):
        print(f"Fuel price fields: {list(sample['fuel_prices'][0].keys())}")

    # Find lat/lon fields -- may be top-level or inside a location sub-object
    def find_nested(r, *keys):
        for k in keys:
            if k in r:
                return k, r[k]
        # Check one level of nesting
        for subkey in ("location", "coordinates", "address", "geo"):
            if subkey in r and isinstance(r[subkey], dict):
                for k in keys:
                    if k in r[subkey]:
                        return f"{subkey}.{k}", r[subkey][k]
        return None, None

    lat_key, _ = find_nested(sample, "latitude", "lat", "Latitude")
    lon_key, _ = find_nested(sample, "longitude", "lon", "lng", "Longitude")

    if not lat_key or not lon_key:
        print(f"WARNING: lat/lon not found. Map won't render. Station keys: {list(sample.keys())}")

    fuel_types = collect_fuel_types(stations_dict)

    # CSV columns: id, name, brand, latitude, longitude, <fuel_type>_price, <fuel_type>_updated, ...
    fieldnames = ["id", "name", "brand_name"]
    if lat_key:
        fieldnames.append("latitude")
    if lon_key:
        fieldnames.append("longitude")
    for ft in fuel_types:
        fieldnames.append(f"{ft}_price")
        fieldnames.append(f"{ft}_updated")
    fieldnames.append("E10_price_band")
    fieldnames.append("brand_avg_E10")
    # Add useful metadata columns
    extra_cols = ["postcode", "city", "county", "address_line_1",
                  "is_motorway_service_station", "is_supermarket_service_station",
                  "temporary_closure", "permanent_closure", "public_phone_number"]
    for col in extra_cols:
        fieldnames.append(col)
    # Add any remaining scalar fields not already included
    skip = {"node_id", "trading_name", "fuel_prices", "brand_name"} | set(extra_cols)
    for k in sample.keys():
        if k not in skip and not isinstance(sample[k], (dict, list)) and k not in fieldnames:
            fieldnames.append(k)

    # First pass: build all rows
    all_rows = []
    for r in records:
        row = {
            "id":   r.get("node_id", ""),
            "name": r.get("trading_name", ""),
            "brand_name": normalise_brand(r.get("brand_name", "")),
        }
        if lat_key:
            if "." in lat_key:
                sub, field = lat_key.split(".", 1)
                row["latitude"] = r.get(sub, {}).get(field, "")
            else:
                row["latitude"] = r.get(lat_key, "")
        if lon_key:
            if "." in lon_key:
                sub, field = lon_key.split(".", 1)
                row["longitude"] = r.get(sub, {}).get(field, "")
            else:
                row["longitude"] = r.get(lon_key, "")

        # Fix coordinates using postcode to detect sign errors
        try:
            lat_val = float(row.get("latitude", 0))
            lon_val = float(row.get("longitude", 0))
            loc = r.get("location", {}) if isinstance(r.get("location"), dict) else {}
            pc = loc.get("postcode", r.get("postcode", "")).strip().upper()
            pc_area = ""
            for c in pc:
                if c.isalpha():
                    pc_area += c
                else:
                    break

            # Fix missing negative sign on latitude
            if lat_val < 0 and -61 <= lat_val <= -49.5:
                lat_val = -lat_val
                row["latitude"] = lat_val

            # Fix swapped lat/lon (e.g. lat=-7.6, lon=54.3)
            if not (49.5 <= lat_val <= 61) and 49.5 <= lon_val <= 61:
                lat_val, lon_val = lon_val, lat_val
                row["latitude"] = lat_val
                row["longitude"] = lon_val

            # Fix positive lon using postcode: these areas are always west of Greenwich
            if lon_val > 0 and pc_area in _WESTERN_POSTCODES:
                lon_val = -lon_val
                row["longitude"] = lon_val

            # Drop rows still outside UK bounds
            if not (49.5 <= lat_val <= 61 and -8.5 <= lon_val <= 1.77):
                continue
        except (ValueError, TypeError):
            continue

        # Extract address fields from location object
        loc = r.get("location", {}) if isinstance(r.get("location"), dict) else {}
        for addr_field in ("postcode", "city", "county", "address_line_1"):
            if addr_field not in row or not row.get(addr_field):
                row[addr_field] = loc.get(addr_field, r.get(addr_field, ""))

        # Extract top-level metadata fields
        for meta_field in ("is_motorway_service_station", "is_supermarket_service_station",
                           "temporary_closure", "permanent_closure", "public_phone_number"):
            val = r.get(meta_field)
            if val is not None:
                row[meta_field] = val

        # Flatten fuel_prices array
        for fp in r.get("fuel_prices", []):
            ft = fp.get("fuel_type") or fp.get("fuelType", "")
            if ft:
                row[f"{ft}_price"] = fp.get("price") or fp.get("pump_price") or fp.get("priceInPence", "")
                row[f"{ft}_updated"] = (fp.get("price_last_updated") or fp.get("price_change_effective_timestamp")
                                        or fp.get("updated_at") or fp.get("updatedAt") or fp.get("timestamp", ""))

        # Remaining scalar fields (top-level + metadata)
        for k in r.keys():
            if k not in skip and not isinstance(r.get(k), (dict, list)):
                if k not in row or row.get(k) is None or row.get(k) == "":
                    row[k] = r.get(k, "")

        all_rows.append(row)

    # Compute average E10 price per brand
    from collections import defaultdict
    brand_prices = defaultdict(list)
    for row in all_rows:
        try:
            p = float(row.get("E10_price", 0))
            brand = row.get("brand_name", "").strip()
            if p > 1 and brand:
                brand_prices[brand].append(p)
        except (ValueError, TypeError):
            pass
    brand_avg = {b: round(sum(ps) / len(ps), 1) for b, ps in brand_prices.items() if len(ps) >= 5}
    # Assign to rows
    for row in all_rows:
        brand = row.get("brand_name", "").strip()
        row["brand_avg_E10"] = brand_avg.get(brand, "")

    # Compute E10 price quintiles for dynamic bands
    e10_prices = []
    for row in all_rows:
        try:
            p = float(row.get("E10_price", 0))
            if p > 1:
                e10_prices.append(p)
        except (ValueError, TypeError):
            pass
    e10_prices.sort()
    if e10_prices:
        p20 = e10_prices[len(e10_prices) // 5]
        p40 = e10_prices[2 * len(e10_prices) // 5]
        p60 = e10_prices[3 * len(e10_prices) // 5]
        p80 = e10_prices[4 * len(e10_prices) // 5]
        print(f"E10 price bands: Cheapest ≤{p20}p, Cheap {p20}–{p40}p, "
              f"Average {p40}–{p60}p, Pricey {p60}–{p80}p, Expensive >{p80}p")
    else:
        p20, p40, p60, p80 = 148, 151, 154, 157

    # Second pass: assign bands and write CSV
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()

    for row in all_rows:
        try:
            e10 = float(row.get("E10_price", 0))
            if e10 <= 1:
                row["E10_price_band"] = ""
            elif e10 <= p20:
                row["E10_price_band"] = "1 Cheapest"
            elif e10 <= p40:
                row["E10_price_band"] = "2 Cheap"
            elif e10 <= p60:
                row["E10_price_band"] = "3 Average"
            elif e10 <= p80:
                row["E10_price_band"] = "4 Pricey"
            else:
                row["E10_price_band"] = "5 Expensive"
        except (ValueError, TypeError):
            row["E10_price_band"] = ""
        writer.writerow(row)

    return output.getvalue()


# ---------------------------------------------------------------------------
# Microreact
# ---------------------------------------------------------------------------

def load_microreact_state():
    if MICROREACT_FILE.exists():
        return json.loads(MICROREACT_FILE.read_text())
    return {}


def save_microreact_state(state):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    MICROREACT_FILE.write_text(json.dumps(state, indent=2))


MICROREACT_HEADERS = {
    "Content-Type": "application/json; charset=utf-8",
    "User-Agent": "petroleum.watch/1.0",
}


def build_microreact_body(csv_data, n_stations):
    """Build a Microreact .microreact JSON body (schema v1)."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    return {
        "schema": "https://microreact.org/schema/v1.json",
        "meta": {
            "name": "petroleum.watch",
            "description": (
                f"Live fuel prices for {n_stations} UK petrol stations "
                f"from the GOV.UK Fuel Finder API. Last updated {now}."
            ),
        },
        "datasets": {
            "dataset-1": {
                "id": "dataset-1",
                "file": "data-file-1",
                "idFieldName": "id",
            },
        },
        "files": {
            "data-file-1": {
                "id": "data-file-1",
                "type": "data",
                "name": "data.csv",
                "format": "text/csv",
                "blob": csv_data,
            },
        },
        "tables": {
            "table-1": {
                "title": "Stations",
                "dataset": "dataset-1",
                "displayMode": "cosy",
                "columns": [
                    {"field": "id"},
                    {"field": "name"},
                    {"field": "brand_name"},
                    {"field": "latitude"},
                    {"field": "longitude"},
                    {"field": "E10_price"},
                    {"field": "E10_price_band"},
                    {"field": "brand_avg_E10"},
                    {"field": "B7_STANDARD_price"},
                    {"field": "B7_STANDARD_updated"},
                    {"field": "E5_price"},
                    {"field": "E5_updated"},
                    {"field": "B7_PREMIUM_price"},
                    {"field": "B7_PREMIUM_updated"},
                    {"field": "B10_price"},
                    {"field": "B10_updated"},
                    {"field": "HVO_price"},
                    {"field": "HVO_updated"},
                    {"field": "E10_updated"},
                    {"field": "postcode"},
                    {"field": "city"},
                    {"field": "county"},
                    {"field": "address_line_1"},
                    {"field": "is_supermarket_service_station"},
                    {"field": "is_motorway_service_station"},
                    {"field": "temporary_closure"},
                    {"field": "permanent_closure"},
                    {"field": "public_phone_number"},
                    {"field": "is_same_trading_and_brand_name"},
                    {"field": "permanent_closure_date"},
                ],
            },
        },
        "maps": {
            "map-1": {
                "title": "Map",
                "dataType": "geographic-coordinates",
                "latitudeField": "latitude",
                "longitudeField": "longitude",
                "showMarkers": True,
                "markersOpacity": 100,
                "nodeSize": 8,
                "minNodeSize": 4,
                "maxNodeSize": 64,
                "controls": False,
                "grouped": True,
                "scaleMarkers": False,
                "type": "mapbox",
                "viewport": {
                    "longitude": -2.5,
                    "latitude": 54.0,
                    "zoom": 5.5,
                    "pitch": 0,
                    "bearing": 0,
                },
            },
        },
        "styles": {
            "coloursField": "E10_price_band",
            "colourPalettes": [],
            "defaultColour": "#cccccc",
            "defaultShape": "circle",
            "colourSettings": {
                "E10_price_band": {
                    "palette": "ColorBrewer YlGn-5",
                },
            },
            "labelsField": "name",
            "legendDirection": "row",
            "shapesField": None,
            "shapePalettes": [],
        },
        "charts": {
            "chart-1": {
                "controls": False,
                "title": "Avg E10 Price by Brand",
                "type": "custom",
                "spec": json.dumps({
                    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                                        "transform": [
                        {"filter": "datum.E10_price > 0"},
                        {"calculate": "toNumber(datum.E10_price)", "as": "E10_price_num"},
                        {"filter": "datum.E10_price_num > 100 && datum.E10_price_num < 200"},
                    ],
                    "mark": "bar",
                    "encoding": {
                        "x": {
                            "field": "brand_name", "type": "nominal",
                            "sort": {"op": "mean", "field": "E10_price_num"},
                            "title": None,
                            "axis": {"labelAngle": -90},
                        },
                        "y": {
                            "field": "E10_price_num", "type": "quantitative",
                            "aggregate": "mean", "scale": {"zero": False},
                            "title": "Avg E10 Price (p)",
                        },
                        "color": {"field": "brand_name", "type": "nominal", "legend": None},
                    },
                }),
            },
            "chart-2": {
                "controls": False,
                "title": "E10 Price by Brand",
                "type": "custom",
                "spec": json.dumps({
                    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                                        "transform": [
                        {"filter": "datum.E10_price > 0"},
                        {"calculate": "toNumber(datum.E10_price)", "as": "E10_price_num"},
                        {"filter": "datum.E10_price_num > 100 && datum.E10_price_num < 200"},
                    ],
                    "mark": {"type": "boxplot", "extent": 1.5},
                    "encoding": {
                        "x": {"field": "brand_name", "type": "nominal"},
                        "color": {"field": "brand_name", "type": "nominal", "legend": None},
                        "y": {
                            "field": "E10_price_num", "type": "quantitative",
                            "scale": {"zero": False},
                            "title": "E10 Price (p)",
                        },
                    },
                }),
            },
            "chart-3": {
                "controls": False,
                "title": "Diesel vs Petrol",
                "type": "custom",
                "spec": json.dumps({
                    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                                        "transform": [
                        {"filter": "datum.E10_price > 0 && datum.B7_STANDARD_price > 0"},
                        {"calculate": "toNumber(datum.E10_price)", "as": "petrol"},
                        {"calculate": "toNumber(datum.B7_STANDARD_price)", "as": "diesel"},
                        {"filter": "datum.petrol > 100 && datum.petrol < 200 && datum.diesel > 100 && datum.diesel < 250"},
                        {"calculate": "indexof(['ESSO','BP','SHELL','TESCO','TEXACO','MORRISONS','ASDA','SAINSBURY\\'S','JET','GULF'], datum.brand_name) >= 0 ? datum.brand_name : 'OTHER'", "as": "brand_top10"},
                    ],
                    "mark": {"type": "circle", "opacity": 0.4, "size": 25},
                    "encoding": {
                        "x": {
                            "field": "petrol", "type": "quantitative",
                            "scale": {"zero": False}, "title": "E10 Petrol (p)",
                        },
                        "y": {
                            "field": "diesel", "type": "quantitative",
                            "scale": {"zero": False}, "title": "B7 Diesel (p)",
                        },
                        "color": {
                            "field": "brand_top10", "type": "nominal",
                            "title": "Brand",
                        },
                        "tooltip": [
                            {"field": "name", "type": "nominal", "title": "Station"},
                            {"field": "brand_name", "type": "nominal", "title": "Brand"},
                            {"field": "petrol", "type": "quantitative", "title": "E10 (p)"},
                            {"field": "diesel", "type": "quantitative", "title": "B7 (p)"},
                        ],
                    },
                }),
            },
            "chart-4": {
                "controls": False,
                "title": "Supermarket vs Others",
                "type": "custom",
                "spec": json.dumps({
                    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                                        "transform": [
                        {"filter": "datum.E10_price > 0"},
                        {"calculate": "toNumber(datum.E10_price)", "as": "E10_price_num"},
                        {"filter": "datum.E10_price_num > 100 && datum.E10_price_num < 200"},
                        {"calculate": "datum.is_supermarket_service_station == 'True' ? 'Supermarket' : datum.is_motorway_service_station == 'True' ? 'Motorway' : 'Other'", "as": "station_type"},
                    ],
                    "mark": {"type": "boxplot", "extent": 1.5},
                    "encoding": {
                        "x": {
                            "field": "station_type", "type": "nominal",
                            "title": None, "sort": ["Supermarket", "Other", "Motorway"],
                        },
                        "y": {
                            "field": "E10_price_num", "type": "quantitative",
                            "scale": {"zero": False}, "title": "E10 Price (p)",
                        },
                        "color": {"field": "station_type", "type": "nominal", "legend": None},
                    },
                }),
            },
            "chart-5": {
                "controls": False,
                "title": "Motorway Premium",
                "type": "custom",
                "spec": json.dumps({
                    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                                        "transform": [
                        {"calculate": "datum.is_motorway_service_station == 'True' ? 'Motorway' : 'Non-Motorway'", "as": "mway"},
                        {"fold": ["E10_price", "B7_STANDARD_price", "E5_price"], "as": ["fuel_type", "price_raw"]},
                        {"calculate": "toNumber(datum.price_raw)", "as": "price"},
                        {"filter": "datum.price > 100 && datum.price < 250"},
                        {"calculate": "datum.fuel_type == 'E10_price' ? 'E10' : datum.fuel_type == 'B7_STANDARD_price' ? 'B7 Diesel' : 'E5 Premium'", "as": "fuel"},
                    ],
                    "mark": "bar",
                    "encoding": {
                        "x": {"field": "fuel", "type": "nominal", "title": None},
                        "y": {
                            "field": "price", "type": "quantitative",
                            "aggregate": "mean", "scale": {"zero": False},
                            "title": "Avg Price (p)",
                        },
                        "color": {"field": "mway", "type": "nominal", "title": None},
                        "xOffset": {"field": "mway"},
                    },
                }),
            },
            "chart-6": {
                "controls": False,
                "title": "Price Staleness",
                "type": "custom",
                "spec": json.dumps({
                    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                                        "transform": [
                        {"filter": "datum.E10_updated != ''"},
                        {"calculate": "floor((now() - toDate(datum.E10_updated)) / 86400000)", "as": "days_since_update"},
                        {"filter": "datum.days_since_update >= 0 && datum.days_since_update < 60"},
                    ],
                    "mark": "bar",
                    "encoding": {
                        "x": {
                            "field": "days_since_update", "type": "quantitative",
                            "bin": {"maxbins": 30}, "title": "Days Since Last Update",
                        },
                        "y": {"aggregate": "count", "title": "Number of Stations"},
                    },
                }),
            },
            "chart-7": {
                "controls": False,
                "title": "Fuel Availability",
                "type": "custom",
                "spec": json.dumps({
                    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                                        "transform": [
                        {"fold": ["E10_price", "B7_STANDARD_price", "E5_price", "B7_PREMIUM_price", "B10_price", "HVO_price"], "as": ["fuel_col", "fprice"]},
                        {"filter": "datum.fprice != '' && toNumber(datum.fprice) > 0"},
                        {"calculate": "datum.fuel_col == 'E10_price' ? 'E10' : datum.fuel_col == 'B7_STANDARD_price' ? 'B7 Diesel' : datum.fuel_col == 'E5_price' ? 'E5 Premium' : datum.fuel_col == 'B7_PREMIUM_price' ? 'B7 Premium' : datum.fuel_col == 'B10_price' ? 'B10' : 'HVO'", "as": "fuel"},
                    ],
                    "mark": "bar",
                    "encoding": {
                        "x": {
                            "field": "fuel", "type": "nominal", "title": None,
                            "sort": ["E10", "B7 Diesel", "E5 Premium", "B7 Premium", "B10", "HVO"],
                        },
                        "y": {"aggregate": "count", "title": "Number of Stations"},
                        "color": {"field": "fuel", "type": "nominal", "legend": None},
                    },
                }),
            },
        },
        "filters": {},
        "matrices": {},
        "networks": {},
        "notes": {},
        "slicers": {},
        "timelines": {},
        "trees": {},
        "views": [],
        "panes": {
            "model": {
                "global": {
                    "splitterSize": 2,
                    "tabEnableClose": False,
                    "tabSetHeaderHeight": 1,
                    "tabSetTabStripHeight": 1,
                    "tabSetMinWidth": 160,
                    "tabSetMinHeight": 160,
                    "borderMinSize": 160,
                    "borderBarSize": 20,
                    "borderEnableDrop": False,
                },
                "borders": [
                    {
                        "type": "border",
                        "size": 240,
                        "location": "right",
                        "children": [
                            {"type": "tab", "id": "--mr-legend-pane", "name": "Legend",
                             "component": "Legend", "enableClose": False, "enableDrag": False},
                            {"type": "tab", "id": "--mr-selection-pane", "name": "Selection",
                             "component": "Selection", "enableClose": False, "enableDrag": False},
                            {"type": "tab", "id": "--mr-history-pane", "name": "History",
                             "component": "History", "enableClose": False, "enableDrag": False},
                            {"type": "tab", "id": "--mr-views-pane", "name": "Views",
                             "component": "Views", "enableClose": False, "enableDrag": False},
                        ],
                    }
                ],
                "layout": {
                    "type": "row",
                    "children": [
                        {
                            "type": "row",
                            "children": [
                                {
                                    "type": "tabset",
                                    "weight": 71,
                                    "children": [
                                        {"type": "tab", "id": "map-1", "name": "Map", "component": "Map"},
                                    ],
                                },
                                {
                                    "type": "row",
                                    "weight": 29,
                                    "children": [
                                        {
                                            "type": "tabset",
                                            "weight": 25,
                                            "children": [
                                                {"type": "tab", "id": "chart-2", "name": "E10 by Brand", "component": "Chart"},
                                                {"type": "tab", "id": "chart-3", "name": "Diesel vs Petrol", "component": "Chart"},
                                                {"type": "tab", "id": "chart-1", "name": "Avg Price", "component": "Chart"},
                                                {"type": "tab", "id": "chart-4", "name": "Station Type", "component": "Chart"},
                                                {"type": "tab", "id": "chart-5", "name": "Motorway Premium", "component": "Chart"},
                                                {"type": "tab", "id": "chart-6", "name": "Staleness", "component": "Chart"},
                                                {"type": "tab", "id": "chart-7", "name": "Fuel Availability", "component": "Chart"},
                                            ],
                                        },
                                        {
                                            "type": "tabset",
                                            "weight": 75,
                                            "children": [
                                                {"type": "tab", "id": "table-1", "name": "Stations", "component": "Table"},
                                            ],
                                        },
                                    ],
                                },
                            ],
                        }
                    ],
                },
            },
        },
    }


def push_to_microreact(csv_data, api_token, n_stations, existing_id=None):
    """Create or update a Microreact project. Returns (url, project_id)."""
    project = build_microreact_body(csv_data, n_stations)
    headers = {**MICROREACT_HEADERS, "Access-Token": api_token}

    # Try to update existing project first, fall back to create
    if existing_id:
        update_url = f"{MICROREACT_API_BASE}/projects/update?project={existing_id}"
        req = urllib.request.Request(
            update_url,
            data=json.dumps(project).encode(),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(req) as resp:
                result = json.load(resp)
            url = result.get("url") or f"https://microreact.org/project/{existing_id}"
            print(f"Updated existing Microreact project: {url}")
            return url, existing_id
        except urllib.error.HTTPError as e:
            if e.code in (404, 405, 400):
                print(f"Update failed ({e.code}), creating new project...")
            else:
                print(f"Microreact update error {e.code}: {e.read().decode()[:200]}")
                sys.exit(1)

    # Create new project
    req = urllib.request.Request(
        f"{MICROREACT_API_BASE}/projects/create",
        data=json.dumps(project).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.load(resp)
    except urllib.error.HTTPError as e:
        print(f"Microreact create error {e.code}: {e.read().decode()[:300]}")
        sys.exit(1)

    url = result.get("url") or result.get("id") or str(result)
    project_id = result.get("id") or result.get("project", {}).get("id")
    print(f"Created Microreact project: {url}")
    return url, project_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Sync UK fuel prices to Microreact")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--full-load", action="store_true",
                       help="Fetch all stations and create/overwrite Microreact project")
    group.add_argument("--update", action="store_true",
                       help="Fetch incremental changes and re-push to Microreact")
    parser.add_argument("--dry-run", action="store_true",
                        help="Do not push to Microreact; print CSV stats only")
    args = parser.parse_args()

    env = load_env()
    client_id     = env.get("FUEL_FINDER_CLIENT_ID")
    client_secret = env.get("FUEL_FINDER_CLIENT_SECRET")
    mr_token      = env.get("MICROREACT_API_TOKEN")

    if not client_id or not client_secret:
        print(f"Missing Fuel Finder credentials in {ENV_FILE}")
        sys.exit(1)
    if not mr_token and not args.dry_run:
        print(f"Missing MICROREACT_API_TOKEN in {ENV_FILE}")
        print("Get it from: microreact.org -> Account -> API -> Create token")
        sys.exit(1)

    print("Getting Fuel Finder token...")
    token = get_token(client_id, client_secret)
    print("Token OK.")

    if args.full_load:
        print("Fetching full station dataset...")
        records = fetch_full(token)
        print(f"Fetched {len(records)} stations.")
        stations = records_to_dict(records)

        print("Fetching station metadata (location, brand, etc.)...")
        metadata = fetch_station_metadata(token)
        print(f"Fetched metadata for {len(metadata)} stations.")
        n_merged = merge_metadata_into_stations(stations, metadata)
        print(f"Merged metadata into {n_merged} stations.")

        save_stations(stations)
        print(f"Saved to {STATIONS_FILE}")

    elif args.update:
        stations = load_stations()
        if not stations:
            print("No local state found. Run --full-load first.")
            sys.exit(1)

        since = ""
        if LAST_UPDATED_FILE.exists():
            since = LAST_UPDATED_FILE.read_text().strip()
        if not since:
            from datetime import timedelta
            since = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"No last_updated found. Defaulting to {since}.")
        else:
            # API wants YYYY-MM-DD format
            since = since[:10]

        print(f"Fetching incremental price updates since {since}...")
        updates = fetch_incremental(token, since)
        stations, n_changed = merge_updates(stations, updates)
        print(f"{n_changed} stations with price updates.")

        print(f"Fetching incremental station metadata since {since}...")
        meta_updates = fetch_incremental_metadata(token, since)
        if meta_updates:
            n_meta = merge_metadata_into_stations(stations, meta_updates)
            print(f"{n_meta} stations with metadata updates.")
        else:
            print("No metadata updates.")

        save_stations(stations)

    # Record price history
    print("Recording price history...")
    snapshot_prices(stations)

    # Build CSV and push
    print("Building CSV...")
    csv_data = stations_to_csv(stations)
    lines = csv_data.count("\n")
    print(f"CSV: {lines} rows, {len(csv_data):,} chars")

    # Record update time
    now_iso = datetime.now(timezone.utc).isoformat()
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    LAST_UPDATED_FILE.write_text(now_iso)

    if args.dry_run:
        preview = csv_data.splitlines()
        print("\n--- CSV preview ---")
        print("\n".join(preview[:5]))
        return

    mr_state = load_microreact_state()
    existing_id = mr_state.get("project_id")

    print("Pushing to Microreact...")
    url, project_id = push_to_microreact(csv_data, mr_token, len(stations), existing_id)

    save_microreact_state({"project_id": project_id, "url": url, "last_pushed": now_iso})
    print(f"\nDone. Microreact: {url}")


if __name__ == "__main__":
    main()
