#!/usr/bin/env python3
"""
maintain_db.py  –  Tiered retention for batt.db

Run nightly via cron, e.g.:
    0 3 * * * /path/env/bin/python3 /path/to/maintain_db.py -d /path/to/batt.db

Retention tiers
───────────────
  0 – 24 h      keep every raw row
  1 – 7 days    keep one raw row per minute   (delete the rest)
  7 – 30 days   keep one raw row per 10 min   (delete the rest)
  30 – 365 days summarise to hourly buckets   (raw rows deleted)
  365 days +    summarise to daily buckets    (raw rows deleted)

Summarisation writes into battery_summary before deleting raw rows,
so min/max/avg for any period is never lost.
"""

import sqlite3
import time
import argparse
import sys

# ── Tier boundaries (seconds ago) ──────────────────────────────────
T_1D   =     86_400
T_7D   =    604_800
T_30D  =  2_592_000
T_365D = 31_536_000

# ── Summary bucket sizes ────────────────────────────────────────────
BUCKET_HOUR = 3600
BUCKET_DAY  = 86_400


def init_summary_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS battery_summary (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_start  REAL    NOT NULL,
            ts_end    REAL    NOT NULL,
            bucket    TEXT    NOT NULL,   -- 'hour' | 'day'
            macaddr   TEXT    NOT NULL,
            volts_avg REAL, volts_min REAL, volts_max REAL,
            amps_avg  REAL, amps_min  REAL, amps_max  REAL,
            soc_avg   REAL, soc_min   REAL, soc_max   REAL,
            temp_avg  REAL, temp_min  REAL, temp_max  REAL,
            net_ah    REAL,
            n_samples INTEGER
        )
    """)
    con.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_summary_bucket
        ON battery_summary (ts_start, bucket, macaddr)
    """)
    con.commit()


def summarise_and_delete(con, older_than_ts, newer_than_ts, bucket_size, bucket_label, dry_run):
    """
    For raw rows between newer_than_ts and older_than_ts:
      1. Group into fixed-size time buckets
      2. Insert a summary row per bucket (skip if already exists)
      3. Delete the raw rows
    """
    rows = con.execute("""
        SELECT id, ts, macaddr, volts, amps, soc_ah, soc_pct, temp
        FROM battery_log
        WHERE ts < ? AND ts >= ?
        ORDER BY ts ASC
    """, (older_than_ts, newer_than_ts)).fetchall()

    if not rows:
        return 0, 0

    # Group by (bucket_start, macaddr)
    buckets = {}
    for r in rows:
        key = (int(r["ts"] // bucket_size) * bucket_size, r["macaddr"])
        buckets.setdefault(key, []).append(r)

    inserted = 0
    deleted_ids = []

    for (ts_start, macaddr), pts in buckets.items():
        ts_end = ts_start + bucket_size

        amps_list  = [p["amps"]    for p in pts]
        volts_list = [p["volts"]   for p in pts]
        soc_list   = [p["soc_pct"] for p in pts]
        temp_list  = [p["temp"]    for p in pts]

        # Trapezoidal net Ah
        net_ah = 0.0
        ts_list = [p["ts"] for p in pts]
        for i in range(1, len(pts)):
            dt_h = (ts_list[i] - ts_list[i-1]) / 3600.0
            net_ah += ((amps_list[i] + amps_list[i-1]) / 2.0) * dt_h

        try:
            con.execute("""
                INSERT OR IGNORE INTO battery_summary
                (ts_start, ts_end, bucket, macaddr,
                 volts_avg, volts_min, volts_max,
                 amps_avg,  amps_min,  amps_max,
                 soc_avg,   soc_min,   soc_max,
                 temp_avg,  temp_min,  temp_max,
                 net_ah, n_samples)
                VALUES (?,?,?,?, ?,?,?, ?,?,?, ?,?,?, ?,?,?, ?,?)
            """, (
                ts_start, ts_end, bucket_label, macaddr,
                round(sum(volts_list)/len(volts_list), 3),
                round(min(volts_list), 3),
                round(max(volts_list), 3),
                round(sum(amps_list)/len(amps_list), 3),
                round(min(amps_list), 3),
                round(max(amps_list), 3),
                round(sum(soc_list)/len(soc_list), 2),
                round(min(soc_list), 2),
                round(max(soc_list), 2),
                round(sum(temp_list)/len(temp_list), 2),
                round(min(temp_list), 2),
                round(max(temp_list), 2),
                round(net_ah, 4),
                len(pts),
            ))
            inserted += 1
        except Exception as e:
            print(f"  Warning: summary insert failed for bucket {ts_start}: {e}")

        deleted_ids.extend([p["id"] for p in pts])

    if not dry_run and deleted_ids:
        con.executemany("DELETE FROM battery_log WHERE id = ?",
                        [(i,) for i in deleted_ids])

    return inserted, len(deleted_ids)


def thin_raw_rows(con, older_than_ts, newer_than_ts, keep_interval_s, dry_run):
    """
    Within the given time range, keep only the first row in each
    keep_interval_s bucket per macaddr. Delete the rest.
    """
    rows = con.execute("""
        SELECT id, ts, macaddr
        FROM battery_log
        WHERE ts < ? AND ts >= ?
        ORDER BY ts ASC
    """, (older_than_ts, newer_than_ts)).fetchall()

    if not rows:
        return 0

    keep_ids = set()
    seen_buckets = set()

    for r in rows:
        bucket_key = (int(r["ts"] // keep_interval_s), r["macaddr"])
        if bucket_key not in seen_buckets:
            seen_buckets.add(bucket_key)
            keep_ids.add(r["id"])

    delete_ids = [r["id"] for r in rows if r["id"] not in keep_ids]

    if not dry_run and delete_ids:
        con.executemany("DELETE FROM battery_log WHERE id = ?",
                        [(i,) for i in delete_ids])

    return len(delete_ids)


def run_maintenance(db_path, dry_run=False, verbose=False):
    now = time.time()
    label = "(DRY RUN) " if dry_run else ""

    print(f"{label}Running maintenance on {db_path}")

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    init_summary_table(con)

    # Count rows before
    total_before = con.execute("SELECT COUNT(*) FROM battery_log").fetchone()[0]
    print(f"  Raw rows before: {total_before:,}")

    # ── Tier 1→2: thin 1d–7d to 1 row/minute ───────────────────────
    deleted = thin_raw_rows(con,
        older_than_ts=now - T_1D,
        newer_than_ts=now - T_7D,
        keep_interval_s=60,
        dry_run=dry_run)
    print(f"  {label}1–7 days   (keep 1/min):   deleted {deleted:,} raw rows")

    # ── Tier 2→3: thin 7d–30d to 1 row/10min ───────────────────────
    deleted = thin_raw_rows(con,
        older_than_ts=now - T_7D,
        newer_than_ts=now - T_30D,
        keep_interval_s=600,
        dry_run=dry_run)
    print(f"  {label}7–30 days  (keep 1/10min): deleted {deleted:,} raw rows")

    # ── Tier 3: summarise 30d–365d to hourly, delete raw ────────────
    ins, deleted = summarise_and_delete(con,
        older_than_ts=now - T_30D,
        newer_than_ts=now - T_365D,
        bucket_size=BUCKET_HOUR,
        bucket_label='hour',
        dry_run=dry_run)
    print(f"  {label}30–365 days (hourly):      {ins:,} summary buckets, deleted {deleted:,} raw rows")

    # ── Tier 4: summarise 365d+ to daily, delete raw ────────────────
    ins, deleted = summarise_and_delete(con,
        older_than_ts=now - T_365D,
        newer_than_ts=0,
        bucket_size=BUCKET_DAY,
        bucket_label='day',
        dry_run=dry_run)
    print(f"  {label}365 days+  (daily):        {ins:,} summary buckets, deleted {deleted:,} raw rows")

    if not dry_run:
        con.commit()
        con.execute("VACUUM")

    total_after = con.execute("SELECT COUNT(*) FROM battery_log").fetchone()[0]
    summary_rows = con.execute("SELECT COUNT(*) FROM battery_summary").fetchone()[0]
    con.close()

    print(f"  Raw rows after:  {total_after:,}")
    print(f"  Summary rows:    {summary_rows:,}")
    print("  Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Battery DB maintenance / tiered retention")
    parser.add_argument("-d", "--database", required=True, help="Path to batt.db")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without deleting")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    run_maintenance(args.database, dry_run=args.dry_run, verbose=args.verbose)
