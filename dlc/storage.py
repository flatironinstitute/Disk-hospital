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


DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS dlc_case (
    rowid            INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id          INTEGER NOT NULL,
    hostname         TEXT NOT NULL,
    state            TEXT NOT NULL,
    block_dev        TEXT NOT NULL,
    osd_id           INTEGER NOT NULL,
    ceph_cluster     TEXT NOT NULL,
    crush_weight     REAL NOT NULL,
    mount            TEXT,
    action           TEXT NOT NULL,
    wait_reason      TEXT NOT NULL DEFAULT None,
    version_number   INTEGER NOT NULL,
    -- Store as RFC-3339 in UTC, filled by SQLite
    version_date_time TEXT NOT NULL
        DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    active           INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_hostdev
    ON dlc_case(hostname, block_dev)
    WHERE active = 1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_osdcluster
    ON dlc_case(osd_id, ceph_cluster)
    WHERE active = 1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_caseid_version
    ON dlc_case(case_id, version_number);

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

