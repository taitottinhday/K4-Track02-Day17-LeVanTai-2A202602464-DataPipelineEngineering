"""Bronze — land each source's daily delivery as an immutable Parquet file.

Commitment (slide "Bronze — Cam kết"): what you read here is exactly what left
the source system. We never UPDATE or DELETE Bronze. Duplicates, Kafka
tombstones, malformed events — all of it is kept; cleaning is Silver's job.

Every row carries its lineage, like the slide's `bronze_tickets` table:
    _payload          the original record, verbatim (JSON text)
    _source           which source system
    _op               CDC operation c / u / d / r  (tickets only)
    _ingested_at      when it reached us (Kafka record timestamp / export time)
    _batch_id         which daily batch landed it  (= ingest day)
    _kafka_partition, _kafka_offset   where to replay it from

Idempotent landing: one file per (source, day). If the file exists the batch was
already landed and we do nothing — re-running a day never duplicates Bronze, and
we still never modify a row that is already there.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import config

BRONZE_COLUMNS = (
    "_source VARCHAR, _batch_id VARCHAR, _ingested_at TIMESTAMP, "
    "_kafka_partition INTEGER, _kafka_offset BIGINT, _op VARCHAR, _payload VARCHAR"
)


def batch_path(source: str, day: str) -> Path:
    return config.BRONZE_DIR / source / f"ingest_date={day}" / "part-0.parquet"


def bronze_files(source: str) -> list[Path]:
    return sorted((config.BRONZE_DIR / source).glob("ingest_date=*/part-0.parquet"))


def bronze_scan(source: str) -> str:
    """SQL table expression over every landed Bronze file of `source`."""
    if not bronze_files(source):   # nothing landed yet -> typed empty relation
        cols = ", ".join(f"NULL::{c.split()[1]} AS {c.split()[0]}"
                         for c in BRONZE_COLUMNS.split(", "))
        return f"(SELECT {cols} WHERE false)"
    glob = (config.BRONZE_DIR / source / "ingest_date=*" / "part-0.parquet").as_posix()
    return f"read_parquet('{glob}', hive_partitioning = false)"


def _utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def _iso_utc(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)


def _read_raw(source: str, day: str) -> list[tuple]:
    raw = config.DATA_DIR / config.SOURCES[source].format(day=day)
    if not raw.exists():
        return []
    rows: list[tuple] = []
    if source in ("tickets", "events"):            # Kafka records, one JSON per line
        for line in raw.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            value = rec.get("value")
            op = value.get("op") if (source == "tickets" and value) else None
            rows.append((source, day, _utc(rec["timestamp"]), rec.get("partition"),
                         rec.get("offset"), op, line))
    else:                                          # transcripts: a JSON array export
        for obj in json.loads(raw.read_text(encoding="utf-8")):
            rows.append((source, day, _iso_utc(obj["exported_at"]), None, None, None,
                         json.dumps(obj, ensure_ascii=False)))
    return rows


def land_batch(con: duckdb.DuckDBPyConnection, source: str, day: str) -> dict:
    out = batch_path(source, day)
    if out.exists():
        (n,) = con.execute(f"SELECT count(*) FROM read_parquet('{out.as_posix()}')").fetchone()
        return {"source": source, "day": day, "status": "already-landed", "rows": n}
    rows = _read_raw(source, day)
    if not rows:
        return {"source": source, "day": day, "status": "no-data", "rows": 0}
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"CREATE OR REPLACE TEMP TABLE _landing ({BRONZE_COLUMNS})")
    con.executemany("INSERT INTO _landing VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    # Let DuckDB own file publication.  A Python-level replace of a just-written
    # Parquet file is denied by DuckDB 1.5 on Windows because COPY retains a
    # Windows handle until the connection closes.
    con.execute(f"COPY _landing TO '{out.as_posix()}' (FORMAT parquet)")
    con.execute("DROP TABLE _landing")
    return {"source": source, "day": day, "status": "landed", "rows": len(rows)}


def land_day(con: duckdb.DuckDBPyConnection, day: str) -> dict:
    return {src: land_batch(con, src, day) for src in config.SOURCES}
