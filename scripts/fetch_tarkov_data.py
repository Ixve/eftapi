#!/usr/bin/env python3

import json
import sys
import time
from pathlib import Path

import requests

BASE_URL = "https://json.tarkov.dev"
GAME_MODE = "regular"
LANGUAGE = "en"
USER_AGENT = "eftapi-cache-updater/1.0"
TIMEOUT_SECONDS = 30.0
MAX_ATTEMPTS_PER_DATASET = 3

# The JSON API serves whole datasets keyed by id and stores every localized
# string as a "<id> Name" placeholder. Real text lives in a sibling dictionary
# at <endpoint>_<lang> that maps placeholder -> string.
DATASETS = (
    ("items", f"/{GAME_MODE}/items", ("items", "itemCategories")),
    ("items_lang", f"/{GAME_MODE}/items_{LANGUAGE}", ()),
    ("maps", f"/{GAME_MODE}/maps", ("maps", "lootContainers")),
    ("maps_lang", f"/{GAME_MODE}/maps_{LANGUAGE}", ()),
    ("tasks", f"/{GAME_MODE}/tasks", ("tasks", "questItems")),
    ("tasks_lang", f"/{GAME_MODE}/tasks_{LANGUAGE}", ()),
    ("traders", f"/{GAME_MODE}/traders", ()),
)

# Objective type -> GraphQL type, mirroring TaskObjective.__resolveType in
# tarkov-api. The old ZONES_QUERY spread fragments over QuestItem, Mark, Item,
# Shoot, Basic and UseItem; anything else resolved to a type the query did not
# ask about and came back as an empty object.
QUEST_ITEM_TYPES = frozenset(("findQuestItem", "giveQuestItem", "plantQuestItem"))
ITEM_TYPES = frozenset(("findItem", "giveItem", "plantItem", "sellItem", "haveItem"))
UNSELECTED_TYPES = frozenset((
    "extract", "hideoutStation", "skill", "traderLevel", "taskStatus",
    "playerLevel", "experience", "buildWeapon", "traderStanding",
))

# Objective fields that make a task count as "uses this item", matching
# getTasksRequiringItem in tarkov-api.
USED_IN_TASK_FIELDS = (
    "item", "markerItem", "containsOne", "containsAll",
    "wearing", "usingWeapon", "usingWeaponMods",
)

# getTasksRequiringItem also tests the singular `obj.item`, which the JSON API
# no longer emits for item objectives -- it folds that value into `items`. A
# one-entry `items` is therefore the old singular field, while a longer list is
# an "any of these" set that GraphQL never counted. Including whole lists here
# inflates the index from ~640 items to ~3600.
USED_IN_TASK_SINGLETON_FIELD = "items"

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


def get_json(path: str, required_keys: tuple) -> dict:
    url = BASE_URL + path
    try:
        response = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
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

    data = parsed.get("data")
    if not isinstance(data, dict) or len(data) == 0:
        raise FetchError("Response is missing a non-empty 'data' object")

    for key in required_keys:
        section = data.get(key)
        if not isinstance(section, dict) or len(section) == 0:
            raise FetchError(f"Response is missing a non-empty data.{key} object")

    return data


def fetch_dataset(name: str, path: str, required_keys: tuple) -> dict:
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS_PER_DATASET + 1):
        print(f"[{name}] attempt {attempt}/{MAX_ATTEMPTS_PER_DATASET}", flush=True)
        try:
            data = get_json(path, required_keys)
            print(f"[{name}] response validated ({len(data)} sections)", flush=True)
            return data
        except FetchError as err:
            last_error = err
            print(f"[{name}] failed: {err}", file=sys.stderr, flush=True)
            if attempt < MAX_ATTEMPTS_PER_DATASET:
                time.sleep(attempt)

    raise FetchError(
        f"{name} failed after {MAX_ATTEMPTS_PER_DATASET} attempts: {last_error}"
    )


def xyz(point) -> dict:
    if not isinstance(point, dict):
        return None
    return {"x": point.get("x"), "y": point.get("y"), "z": point.get("z")}


def collect_item_ids(value, out: list) -> None:
    if isinstance(value, str):
        out.append(value)
    elif isinstance(value, dict):
        if isinstance(value.get("id"), str):
            out.append(value["id"])
    elif isinstance(value, list):
        for entry in value:
            collect_item_ids(entry, out)


def build_used_in_tasks(tasks: dict, tasks_lang: dict) -> dict:
    index = {}
    for task_id, task in tasks["tasks"].items():
        entry = {
            "kappaRequired": bool(task.get("kappaRequired")),
            "id": task_id,
            "name": tasks_lang.get(task.get("name"), task.get("name")),
        }
        referenced = []
        for objective in task.get("objectives") or []:
            for field in USED_IN_TASK_FIELDS:
                if field in objective:
                    collect_item_ids(objective[field], referenced)
            singleton = objective.get(USED_IN_TASK_SINGLETON_FIELD)
            if isinstance(singleton, list) and len(singleton) == 1:
                collect_item_ids(singleton, referenced)
        for item_id in dict.fromkeys(referenced):
            index.setdefault(item_id, []).append(entry)
    return index


def build_loot_containers(maps: dict, maps_lang: dict) -> list:
    # Static world containers are not in the items dataset; they live under
    # maps.lootContainers, with names placeheld against maps_<lang>.
    out = []
    for container_id, container in maps["lootContainers"].items():
        name = container.get("name")
        out.append({
            "id": container_id,
            "name": maps_lang.get(name, name),
            "normalizedName": container.get("normalizedName"),
        })
    return out


def build_items_payload(items: dict, items_lang: dict, traders: dict,
                        tasks: dict, tasks_lang: dict,
                        maps: dict, maps_lang: dict) -> dict:
    categories = items["itemCategories"]
    trader_names = {tid: t.get("normalizedName") for tid, t in traders.items()}
    used_in_tasks = build_used_in_tasks(tasks, tasks_lang)

    out = []
    for item_id, item in items["items"].items():
        # GraphQL exposed a single `category`; the JSON API lists the whole
        # ancestry, most specific first.
        category = None
        item_categories = item.get("categories") or []
        if item_categories and item_categories[0] in categories:
            parent_id = categories[item_categories[0]].get("parent")
            if parent_id in categories:
                parent_name = categories[parent_id].get("name")
                category = {"parent": {"name": items_lang.get(parent_name, parent_name)}}

        sell_for = []
        for offer in item.get("sellToTrader") or []:
            source = trader_names.get(offer.get("trader"))
            if source:
                sell_for.append({"source": source, "priceRUB": offer.get("priceRUB")})

        # Matches the flea entry tarkov-api pushes onto sellFor: lastLowPrice,
        # and only when the item is actually tradable.
        last_low = item.get("lastLowPrice")
        if "noFlea" not in (item.get("types") or []) and last_low:
            sell_for.append({"source": "fleaMarket", "priceRUB": last_low})

        name = item.get("name")
        short_name = item.get("shortName")
        out.append({
            "id": item_id,
            "name": items_lang.get(name, name),
            "shortName": items_lang.get(short_name, short_name),
            "category": category,
            "avg24hPrice": item.get("avg24hPrice"),
            "height": item.get("height"),
            "width": item.get("width"),
            "weight": item.get("weight"),
            "usedInTasks": used_in_tasks.get(item_id, []),
            "sellFor": sell_for,
        })

    return {"data": {"items": out, "lootContainers": build_loot_containers(maps, maps_lang)}}


def build_hazards_payload(maps: dict, maps_lang: dict) -> dict:
    out = []
    for map_entry in maps["maps"].values():
        hazards = []
        for hazard in map_entry.get("hazards") or []:
            name = hazard.get("name")
            outline = hazard.get("outline")
            hazards.append({
                "name": maps_lang.get(name, name),
                "outline": [xyz(p) for p in outline] if isinstance(outline, list) else None,
                "position": xyz(hazard.get("position")),
            })
        out.append({"nameId": map_entry.get("nameId"), "hazards": hazards})
    return {"data": {"maps": out}}


def build_zones_payload(tasks: dict, tasks_lang: dict, items: dict,
                        items_lang: dict, maps: dict) -> dict:
    map_names = {mid: m.get("normalizedName") for mid, m in maps["maps"].items()}
    item_records = items["items"]
    quest_items = tasks["questItems"]

    def short_name(record, table):
        value = record.get("shortName")
        return table.get(value, value)

    def zone_list(objective):
        out = []
        for zone in objective.get("zones") or []:
            out.append({
                "id": zone.get("id"),
                "position": xyz(zone.get("position")),
                # GraphQL nested the map object; the JSON API stores its id.
                "map": {"normalizedName": map_names.get(zone.get("map"))},
            })
        return out

    out = []
    for task_id, task in tasks["tasks"].items():
        objectives = []
        for objective in task.get("objectives") or []:
            kind = objective.get("type")
            if kind in QUEST_ITEM_TYPES:
                quest_item = None
                record = quest_items.get(objective.get("questItem"))
                if record:
                    quest_item = {
                        "id": record.get("id"),
                        "shortName": short_name(record, tasks_lang),
                    }
                objectives.append({"zones": zone_list(objective), "questItem": quest_item})
            elif kind in ITEM_TYPES:
                referenced = []
                for item_id in objective.get("items") or []:
                    record = item_records.get(item_id)
                    if record:
                        referenced.append({
                            "id": item_id,
                            "shortName": short_name(record, items_lang),
                        })
                objectives.append({"zones": zone_list(objective), "items": referenced})
            elif kind in UNSELECTED_TYPES:
                objectives.append({})
            else:
                objectives.append({"zones": zone_list(objective)})

        out.append({
            "id": task_id,
            "name": tasks_lang.get(task.get("name"), task.get("name")),
            "objectives": objectives,
        })

    return {"data": {"tasks": out}}


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

    containers = data.get("lootContainers")
    if not isinstance(containers, list) or len(containers) == 0:
        raise FetchError("Items payload is missing a non-empty data.lootContainers array")

    first_container = containers[0]
    if not isinstance(first_container, dict) or "id" not in first_container:
        raise FetchError("Items payload is missing expected loot container fields")


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

    if not any(m.get("hazards") for m in maps_data):
        raise FetchError("Hazards payload has no hazards on any map")


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

    zones = 0
    for task in tasks:
        for objective in task["objectives"]:
            zones += len(objective.get("zones") or [])
    if zones == 0:
        raise FetchError("Zones payload has no zones on any objective")


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

    containers: dict = {}
    for c in items_payload["data"].get("lootContainers") or []:
        container_id = c.get("id")
        if not container_id:
            continue
        name = c.get("name") or c.get("normalizedName") or "Container"
        containers[container_id] = {
            "n": name,
            "s": c.get("normalizedName") or "",
        }

    return {"count": count, "items": out, "containers": containers}


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
    containers = len(compact["containers"])
    print(f"[convert] kept {containers} loot containers", flush=True)
    if containers == 0:
        raise FetchError("conversion produced 0 loot containers")
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
        raw = {}
        for name, path, required_keys in DATASETS:
            raw[name] = fetch_dataset(name, path, required_keys)

        items_payload = build_items_payload(
            raw["items"], raw["items_lang"], raw["traders"],
            raw["tasks"], raw["tasks_lang"],
            raw["maps"], raw["maps_lang"],
        )
        hazards_payload = build_hazards_payload(raw["maps"], raw["maps_lang"])
        zones_payload = build_zones_payload(
            raw["tasks"], raw["tasks_lang"], raw["items"],
            raw["items_lang"], raw["maps"],
        )

        validate_items_payload(items_payload)
        validate_hazards_payload(hazards_payload)
        validate_zones_payload(zones_payload)
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
