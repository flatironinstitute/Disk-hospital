"""
Very small wrapper around sqlite3:
* Ensures schema is present.
* Yields a cursor that commits/rolls back automatically.
"""
from contextlib import contextmanager
import os, sqlite3
from pathlib import Path

_DB_PATH = Path.home() / ".dlc" / "dlc.sqlite"
_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

TABLE_NAME = "testing_table"
HISTORY_TABLE = "history_testing_table"

DDL = f"""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    case_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    hostname         TEXT DEFAULT NULL,
    host_serial      TEXT DEFAULT NULL,
    smart_passed     TEXT DEFAULT NULL,
    state            TEXT NOT NULL,
    block_dev        TEXT DEFAULT NULL,
    osd_id           INTEGER DEFAULT NULL,
    cluster          TEXT DEFAULT NULL,
    crush_weight     REAL DEFAULT -1.0,
    mount            TEXT DEFAULT NULL,
    action           TEXT DEFAULT NULL,
    wait_reason      TEXT DEFAULT NULL,
    active           INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_hostdev
    ON {TABLE_NAME}(hostname, block_dev)
    WHERE active = 1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_osdcluster
    ON {TABLE_NAME}(osd_id, cluster)
    WHERE active = 1;

CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
    case_id          INTEGER NOT NULL,
    hostname         TEXT DEFAULT NULL,
    host_serial      TEXT DEFAULT NULL,
    smart_passed     TEXT DEFAULT NULL,
    state            TEXT NOT NULL,
    block_dev        TEXT DEFAULT NULL,
    osd_id           INTEGER DEFAULT NULL,
    cluster          TEXT DEFAULT NULL,
    crush_weight     REAL DEFAULT -1.0,
    mount            TEXT DEFAULT NULL,
    action           TEXT DEFAULT NULL,
    wait_reason      TEXT DEFAULT NULL,
    active           INTEGER NOT NULL DEFAULT 1
);
"""


def _open_conn():
    c = sqlite3.connect(_DB_PATH, isolation_level=None)  # autocommit
    c.row_factory = sqlite3.Row
    c.executescript(DDL)
    return c


@contextmanager
def db_cursor():
    conn = _open_conn()
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    finally:
        conn.close()

_open_conn()
