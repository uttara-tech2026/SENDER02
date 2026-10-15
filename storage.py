"""
Minimal JSON-file persistence.

State this bot remembers:
  - which media items it has already seen (by Telegram's
    file_unique_id), so it can tell a sender whether their item is a
    duplicate
  - a running count of how many non-duplicate media items have been
    sent to the admin in total

Everything is global -- there's no per-chat/per-user split, since every
sender funnels into the same admin relay.

{
  "seen_unique_ids": [str, ...],
  "media_sent_count": int
}
"""

import json
import os
import asyncio

import config

_lock = asyncio.Lock()
_data = None


def _default():
    return {"seen_unique_ids": [], "media_sent_count": 0}


def _load():
    global _data
    if _data is not None:
        return _data
    if os.path.exists(config.DATA_FILE):
        try:
            with open(config.DATA_FILE, "r", encoding="utf-8") as f:
                _data = json.load(f)
        except (json.JSONDecodeError, OSError):
            _data = _default()
    else:
        _data = _default()
    _data.setdefault("seen_unique_ids", [])
    _data.setdefault("media_sent_count", 0)
    return _data


def _save():
    tmp = config.DATA_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_data, f, indent=2)
    os.replace(tmp, config.DATA_FILE)


async def record_media(unique_id: str) -> tuple:
    """Atomically check whether unique_id has been seen before and, if
    not, record it and bump the running sent-count.

    Returns (is_new, media_sent_count):
      - is_new=True  -> brand-new item, caller should relay it;
                        media_sent_count is the new total (includes this one)
      - is_new=False -> duplicate, caller should not relay it again;
                        media_sent_count is the total unchanged by this call

    Doing the check-and-mark under a single lock (rather than a separate
    is_duplicate() + mark_seen()) avoids a race where two near-identical
    uploads arriving at the same instant both pass the check before
    either has recorded itself.
    """
    async with _lock:
        data = _load()
        seen = data["seen_unique_ids"]
        if unique_id in seen:
            return False, data["media_sent_count"]
        seen.append(unique_id)
        # keep this bounded so the JSON file doesn't grow forever
        if len(seen) > 5000:
            data["seen_unique_ids"] = seen[-5000:]
        data["media_sent_count"] += 1
        _save()
        return True, data["media_sent_count"]
