"""
SQLite storage for the relay bot (stdlib only, no extra dependency).

Keeps record_media() compatible with the old version, plus users/roles,
settings (destination, route, caption, delays) and counters for stats.
All functions are async to match the old API, but SQLite calls are tiny
and run directly on the event loop thread.
"""

import sqlite3
import time

import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS media (
    unique_id TEXT PRIMARY KEY,
    sender_id INTEGER,
    created   REAL
);
CREATE TABLE IF NOT EXISTS users (
    user_id  INTEGER PRIMARY KEY,
    name     TEXT,
    username TEXT,
    role     TEXT NOT NULL DEFAULT 'pending',
    joined   REAL
);
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE IF NOT EXISTS stats (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);
"""

_db = None


def _conn() -> sqlite3.Connection:
    global _db
    if _db is None:
        _db = sqlite3.connect(getattr(config, "DB_PATH", "relay.db"))
        _db.row_factory = sqlite3.Row
        _db.executescript(SCHEMA)
        _db.commit()
    return _db


# ------------------------------------------------------------------ media
async def record_media(unique_id: str, sender_id: int | None = None):
    """Returns (is_new, total_unique_count)."""
    db = _conn()
    cur = db.execute(
        "INSERT OR IGNORE INTO media (unique_id, sender_id, created) VALUES (?, ?, ?)",
        (unique_id, sender_id, time.time()),
    )
    db.commit()
    total = db.execute("SELECT COUNT(*) FROM media").fetchone()[0]
    return cur.rowcount > 0, total


async def count_media() -> int:
    return _conn().execute("SELECT COUNT(*) FROM media").fetchone()[0]


# ------------------------------------------------------------------ users
async def upsert_user(user_id: int, name: str, username: str | None) -> bool:
    """Insert or refresh a user. Returns True if the user was newly created."""
    db = _conn()
    exists = db.execute("SELECT 1 FROM users WHERE user_id=?", (user_id,)).fetchone()
    if exists:
        db.execute(
            "UPDATE users SET name=?, username=? WHERE user_id=?",
            (name, username, user_id),
        )
    else:
        db.execute(
            "INSERT INTO users (user_id, name, username, role, joined) VALUES (?, ?, ?, 'pending', ?)",
            (user_id, name, username, time.time()),
        )
    db.commit()
    return not exists


async def get_user(user_id: int):
    row = _conn().execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()
    return dict(row) if row else None


async def set_role(user_id: int, role: str):
    db = _conn()
    db.execute("UPDATE users SET role=? WHERE user_id=?", (role, user_id))
    db.commit()


async def list_users(role: str | None = None):
    order = (
        "ORDER BY CASE role WHEN 'pending' THEN 0 WHEN 'sender' THEN 1 "
        "WHEN 'receiver' THEN 2 ELSE 3 END, joined"
    )
    if role:
        rows = _conn().execute(f"SELECT * FROM users WHERE role=? {order}", (role,)).fetchall()
    else:
        rows = _conn().execute(f"SELECT * FROM users {order}").fetchall()
    return [dict(r) for r in rows]


# --------------------------------------------------------------- settings
async def get_setting(key: str, default=None):
    row = _conn().execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


async def set_setting(key: str, value):
    db = _conn()
    db.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )
    db.commit()


async def del_setting(key: str):
    db = _conn()
    db.execute("DELETE FROM settings WHERE key=?", (key,))
    db.commit()


# ------------------------------------------------------------------ stats
async def incr(key: str, n: int = 1):
    db = _conn()
    db.execute(
        "INSERT INTO stats (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = value + excluded.value",
        (key, n),
    )
    db.commit()


async def all_stats() -> dict:
    rows = _conn().execute("SELECT key, value FROM stats").fetchall()
    return {r["key"]: r["value"] for r in rows}
