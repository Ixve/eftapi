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
    "query Items { items { id name shortName category { parent { name } } "
    "avg24hPrice height width weight usedInTasks { kappaRequired id name } "
    "sellFor { source priceRUB } } }"
)
HAZARDS_QUERY = (
    "query Hazards { maps { nameId hazards { name outline { x y z } position { x y z } } } }"
)
ZONES_QUERY = (
    "query Zones { "
    "tasks { "
    "id "
    "name "
    "objectives { "
    "... on TaskObjectiveQuestItem { "
    "questItem { id shortName } "
    "zones { id map { normalizedName } position { x y z } } "
    "} "
    "... on TaskObjectiveMark { "
    "zones { id map { normalizedName } position { x y z } } "
    "} "
    "... on TaskObjectiveItem { "
    "items { id shortName } "
    "zones { id map { normalizedName } position { x y z } } "
    "} "
    "... on TaskObjectiveShoot { "
    "zones { id map { normalizedName } position { x y z } } "
    "} "
    "... on TaskObjectiveBasic { "
    "zones { id map { normalizedName } position { x y z } } "
    "} "
    "... on TaskObjectiveUseItem { "
    "zones { id map { normalizedName } position { x y z } } "
    "} "
    "} "
    "} "
    "}"
)

REPO_ROOT = Path(__file__).resolve().parents[1]
ITEMS_OUTFILE = REPO_ROOT / "items.json"
HAZARDS_OUTFILE = REPO_ROOT / "hazards.json"
ZONES_OUTFILE = REPO_ROOT / "zones.json"
HASHES_OUTFILE = REPO_ROOT / "hashes"

FNV1A64_OFFSET = 1469598103934665603
FNV1A64_PRIME = 1099511628211
UINT64_MASK = (1 << 64) - 1


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


def validate_zones_payload(payload: dict) -> None:
    data = payload.get("data")
    if not isinstance(data, dict):
        raise FetchError("Zones payload has invalid 'data' section")

    tasks = data.get("tasks")
    if not isinstance(tasks, list) or len(tasks) == 0:
        raise FetchError("Zones payload is missing a non-empty data.tasks array")

    first = tasks[0]
    if not isinstance(first, dict) or "id" not in first or "objectives" not in first:
        raise FetchError("Zones payload is missing expected task fields")


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


def fnv1a64_text(data: str) -> int:
    h = FNV1A64_OFFSET
    for b in data.encode("utf-8"):
        h ^= b
        h = (h * FNV1A64_PRIME) & UINT64_MASK
    return h or 1


def write_json(path: Path, payload: dict) -> str:
    serialized = json.dumps(payload, indent=2)
    content = serialized + "\n"
    path.write_text(content, encoding="utf-8")
    return content


def build_items(items_payload: dict) -> dict:
    items = items_payload["data"]["items"]
    out: dict = {}
    count = 0
    for it in items:
        item_id = it.get("id")
        if not item_id:
            continue
        name = it.get("name") or it.get("shortName") or "Unknown Item"
        short = it.get("shortName") or name
        avg = it.get("avg24hPrice") or 0

        sell_for: dict = {}
        trader_max = 0
        flea_entry = None
        for s in (it.get("sellFor") or []):
            src = s.get("source")
            price = s.get("priceRUB") or 0
            if src:
                sell_for[src] = int(price)
            if src == "fleaMarket":
                flea_entry = price
            elif price > trader_max:
                trader_max = price
        has_flea = flea_entry is not None
        flea = flea_entry if has_flea else avg
        banned = 1 if (avg in (None, 0) and not has_flea) else 0

        used_in_tasks: dict = {}
        kappa = 0
        for task in (it.get("usedInTasks") or []):
            kr = 1 if task.get("kappaRequired") else 0
            if kr:
                kappa = 1
            tid = task.get("id")
            if tid:
                used_in_tasks[tid] = {"k": kr, "n": task.get("name") or ""}

        cat = ""
        c = it.get("category")
        if isinstance(c, dict):
            p = c.get("parent")
            if isinstance(p, dict):
                cat = p.get("name") or ""

        out[item_id] = {
            "n": name,
            "s": short,
            "c": cat,
            "a": int(avg or 0),
            "h": int(it.get("height") or 0),
            "w": int(it.get("width") or 0),
            "wt": float(it.get("weight") or 0),
            "t": int(trader_max),
            "f": int(flea or 0),
            "b": banned,
            "k": kappa,
            "sf": sell_for,
            "ut": used_in_tasks,
        }
        count += 1
    return {"count": count, "items": out}


def write_items(path: Path, items_payload: dict) -> str:
    total = len(items_payload["data"]["items"])
    print(f"[convert] compacting {total} api items", flush=True)
    compact = build_items(items_payload)
    kept = compact["count"]
    bad_ids = sum(1 for k in compact["items"] if len(k) != 24)
    print(
        f"[convert] kept {kept}/{total} items, skipped {total - kept} without id, "
        f"{bad_ids} keys not 24-char",
        flush=True,
    )
    if kept == 0:
        raise FetchError("conversion produced 0 id-keyed items")
    serialized = json.dumps(compact, ensure_ascii=True, separators=(",", ":"))
    path.write_text(serialized, encoding="utf-8")
    print(f"[convert] wrote {path.name} ({len(serialized)} bytes)", flush=True)
    return serialized


def write_hashes(path: Path, items_text: str, hazards_text: str, zones_text: str) -> None:
    hashes = [
        fnv1a64_text(items_text),
        fnv1a64_text(hazards_text),
        fnv1a64_text(zones_text),
    ]
    path.write_text("\n".join(str(h) for h in hashes) + "\n", encoding="utf-8")
    print(f"[hashes] wrote {path.name}", flush=True)


def main() -> int:
    try:
        items_payload = fetch_dataset("items", ITEMS_QUERY, validate_items_payload)
        hazards_payload = fetch_dataset("hazards", HAZARDS_QUERY, validate_hazards_payload)
        zones_payload = fetch_dataset("zones", ZONES_QUERY, validate_zones_payload)
    except FetchError as err:
        print(f"Fetch run failed: {err}", file=sys.stderr)
        return 1

    items_text = write_items(ITEMS_OUTFILE, items_payload)
    hazards_text = write_json(HAZARDS_OUTFILE, hazards_payload)
    zones_text = write_json(ZONES_OUTFILE, zones_payload)
    write_hashes(HASHES_OUTFILE, items_text, hazards_text, zones_text)

    print(
        f"Wrote {ITEMS_OUTFILE.name}, {HAZARDS_OUTFILE.name}, "
        f"{ZONES_OUTFILE.name}, and {HASHES_OUTFILE.name}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
