#!/usr/bin/env python3

import json
import sys
import time
from pathlib import Path

import requests

API_URL = "https://api.tarkov.dev/graphql"
USER_AGENT = "eftapi-cache-updater/1.0"
TIMEOUT_SECONDS = 6.0
MAX_ATTEMPTS_PER_DATASET = 3

ITEMS_QUERY = (
    "query Items { items { id name shortName category { parent { name } } } }"
)
HAZARDS_QUERY = (
    "query Hazards { maps { nameId hazards { name outline { x y z } position { x y z } } } }"
)

REPO_ROOT = Path(__file__).resolve().parents[1]
ITEMS_OUTFILE = REPO_ROOT / "items.json"
HAZARDS_OUTFILE = REPO_ROOT / "hazards.json"


class FetchError(RuntimeError):
    pass


def post_graphql(query: str) -> dict:
    try:
        response = requests.post(
            API_URL,
            json={"query": query},
            headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
            timeout=TIMEOUT_SECONDS,
        )
    except requests.RequestException as err:
        raise FetchError(f"Network timeout/error: {err}") from err

    if response.status_code != 200:
        raise FetchError(f"HTTP {response.status_code}: {response.text[:400]}")

    try:
        parsed = response.json()
    except ValueError as err:
        raise FetchError(f"Invalid JSON response: {err}") from err

    if not isinstance(parsed, dict):
        raise FetchError("Response root is not a JSON object")

    errors = parsed.get("errors")
    if isinstance(errors, list) and len(errors) > 0:
        raise FetchError(f"GraphQL returned errors: {json.dumps(errors)[:400]}")

    if "data" not in parsed or not isinstance(parsed["data"], dict):
        raise FetchError("GraphQL response missing valid 'data' object")

    return parsed


def validate_items_payload(payload: dict) -> None:
    data = payload.get("data")
    if not isinstance(data, dict):
        raise FetchError("Items payload has invalid 'data' section")

    items = data.get("items")
    if not isinstance(items, list) or len(items) == 0:
        raise FetchError("Items payload is missing a non-empty data.items array")

    first = items[0]
    if not isinstance(first, dict) or "id" not in first or "shortName" not in first:
        raise FetchError("Items payload is missing expected item fields")


def validate_hazards_payload(payload: dict) -> None:
    data = payload.get("data")
    if not isinstance(data, dict):
        raise FetchError("Hazards payload has invalid 'data' section")

    maps_data = data.get("maps")
    if not isinstance(maps_data, list) or len(maps_data) == 0:
        raise FetchError("Hazards payload is missing a non-empty data.maps array")

    first = maps_data[0]
    if not isinstance(first, dict) or "nameId" not in first or "hazards" not in first:
        raise FetchError("Hazards payload is missing expected map fields")


def fetch_dataset(name: str, query: str, validator) -> dict:
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS_PER_DATASET + 1):
        print(f"[{name}] attempt {attempt}/{MAX_ATTEMPTS_PER_DATASET}", flush=True)
        try:
            payload = post_graphql(query)
            validator(payload)
            print(f"[{name}] response validated", flush=True)
            return payload
        except FetchError as err:
            last_error = err
            print(f"[{name}] failed: {err}", file=sys.stderr, flush=True)
            if attempt < MAX_ATTEMPTS_PER_DATASET:
                time.sleep(attempt)

    raise FetchError(
        f"{name} failed after {MAX_ATTEMPTS_PER_DATASET} attempts: {last_error}"
    )


def write_json(path: Path, payload: dict) -> None:
    serialized = json.dumps(payload, indent=2)
    path.write_text(serialized + "\n", encoding="utf-8")


def main() -> int:
    try:
        items_payload = fetch_dataset("items", ITEMS_QUERY, validate_items_payload)
        hazards_payload = fetch_dataset("hazards", HAZARDS_QUERY, validate_hazards_payload)
    except FetchError as err:
        print(f"Fetch run failed: {err}", file=sys.stderr)
        return 1

    write_json(ITEMS_OUTFILE, items_payload)
    write_json(HAZARDS_OUTFILE, hazards_payload)

    print(f"Wrote {ITEMS_OUTFILE.name} and {HAZARDS_OUTFILE.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
