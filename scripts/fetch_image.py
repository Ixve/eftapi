#!/usr/bin/env python3

import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

BASE_URL = "https://json.tarkov.dev"
GAME_MODE = "regular"
LANGUAGE = "en"
ASSET_URL = "https://assets.tarkov.dev/{item_id}-8x.webp"
USER_AGENT = "eftapi-image-updater/1.0"

JSON_TIMEOUT_SECONDS = 30.0
IMAGE_TIMEOUT_SECONDS = 60.0
MAX_ATTEMPTS = 4
WORKERS = 6
THROTTLE_SECONDS = 0.05
BACKOFF_CAP_SECONDS = 30.0

REPO_ROOT = Path(__file__).resolve().parents[1]
IMAGE_DIR = REPO_ROOT / "images"
INDEX_PATH = IMAGE_DIR / "index.json"

UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

_local = threading.local()
_counts_lock = threading.Lock()
_counts = {"saved": 0, "skipped": 0, "missing": 0, "failed": 0}


class FetchError(RuntimeError):
    pass


def session():
    existing = getattr(_local, "session", None)
    if existing is None:
        existing = requests.Session()
        existing.headers.update({"User-Agent": USER_AGENT})
        _local.session = existing
    return existing


def get_json(path):
    url = BASE_URL + path
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = session().get(url, timeout=JSON_TIMEOUT_SECONDS)
            if response.status_code != 200:
                raise FetchError(f"HTTP {response.status_code}")
            payload = response.json()
        except (requests.RequestException, ValueError) as err:
            last_error = err
        else:
            data = payload.get("data")
            if isinstance(data, dict) and data:
                return data
            last_error = FetchError("missing data object")
        if attempt < MAX_ATTEMPTS:
            time.sleep(attempt)
    raise FetchError(f"{path} failed after {MAX_ATTEMPTS} attempts: {last_error}")


def safe_name(text, fallback):
    cleaned = UNSAFE_CHARS.sub("_", text).strip().strip(".")
    return cleaned if cleaned else fallback


def retry_delay(response, attempt):
    if response is not None:
        header = response.headers.get("Retry-After")
        if header:
            try:
                return min(float(header), BACKOFF_CAP_SECONDS)
            except ValueError:
                pass
    return min(2.0 ** attempt, BACKOFF_CAP_SECONDS)


def build_targets(data, lang):
    items = data.get("items")
    categories = data.get("itemCategories")
    if not isinstance(items, dict) or not items:
        raise FetchError("items payload is missing a non-empty items object")
    if not isinstance(categories, dict) or not categories:
        raise FetchError("items payload is missing a non-empty itemCategories object")

    targets = []
    for item_id, item in items.items():
        if len(item_id) != 24:
            continue

        category = ""
        ancestry = item.get("categories") or []
        if ancestry and ancestry[0] in categories:
            raw = categories[ancestry[0]].get("name") or ""
            category = lang.get(raw, raw)

        slug = item.get("normalizedName") or item_id
        folder = IMAGE_DIR / safe_name(category, "Uncategorized")
        dest = folder / f"{safe_name(slug, item_id)}_{item_id}.webp"
        targets.append((item_id, dest))
    return targets


def record(outcome):
    with _counts_lock:
        _counts[outcome] += 1


def download(target):
    item_id, dest = target
    if dest.exists() and dest.stat().st_size > 0:
        record("skipped")
        return

    url = ASSET_URL.format(item_id=item_id)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        response = None
        try:
            response = session().get(url, timeout=IMAGE_TIMEOUT_SECONDS)
            if response.status_code == 404:
                record("missing")
                return
            if response.status_code == 200:
                body = response.content
                if not body:
                    raise FetchError("empty body")
                dest.parent.mkdir(parents=True, exist_ok=True)
                temp = dest.with_suffix(".part")
                temp.write_bytes(body)
                temp.replace(dest)
                record("saved")
                time.sleep(THROTTLE_SECONDS)
                return
            if response.status_code != 429 and response.status_code < 500:
                break
        except requests.RequestException:
            pass
        if attempt < MAX_ATTEMPTS:
            time.sleep(retry_delay(response, attempt))

    record("failed")
    print(f"[image] failed {item_id}", file=sys.stderr, flush=True)


def write_index(targets):
    index = {
        item_id: dest.relative_to(IMAGE_DIR).as_posix()
        for item_id, dest in targets
        if dest.exists() and dest.stat().st_size > 0
    }
    temp = INDEX_PATH.with_suffix(".part")
    temp.write_text(
        json.dumps(index, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    temp.replace(INDEX_PATH)
    return len(index)


def main():
    try:
        data = get_json(f"/{GAME_MODE}/items")
        lang = get_json(f"/{GAME_MODE}/items_{LANGUAGE}")
        targets = build_targets(data, lang)
    except FetchError as err:
        print(f"Image run failed: {err}", file=sys.stderr)
        return 1

    print(f"[image] {len(targets)} items, writing to {IMAGE_DIR.name}/", flush=True)
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for index, _ in enumerate(pool.map(download, targets), start=1):
            if index % 250 == 0:
                print(f"[image] {index}/{len(targets)}", flush=True)

    indexed = write_index(targets)
    print(
        f"[image] saved {_counts['saved']}, skipped {_counts['skipped']}, "
        f"missing {_counts['missing']}, failed {_counts['failed']}, "
        f"indexed {indexed}",
        flush=True,
    )
    return 1 if _counts["failed"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
