from flask import Flask, jsonify, render_template, request
import sqlite3
import time
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--port",     type=int, default=5001)
parser.add_argument("-D", "--debug",    type=bool, default=True)
parser.add_argument("-d", "--database", type=str,  required=True)
args = parser.parse_args()

print(f"Port: {args.port}  DB: {args.database}")

app = Flask(__name__)
DB_PATH = args.database

# Protection flag bit definitions (must match monitor)
PROTECTION_BITS = {
    0:  'cell_overvolt',
    1:  'cell_undervolt',
    2:  'pack_overvolt',
    3:  'pack_undervolt',
    4:  'chg_overtemp',
    5:  'chg_undertemp',
    6:  'dsg_overtemp',
    7:  'dsg_undertemp',
    8:  'chg_overcurrent',
    9:  'dsg_overcurrent',
    10: 'short_circuit',
    11: 'ic_error',
    12: 'mos_locked',
}

PROTECTION_LABELS = {
    'cell_overvolt':   'Cell Overvoltage',
    'cell_undervolt':  'Cell Undervoltage',
    'pack_overvolt':   'Pack Overvoltage',
    'pack_undervolt':  'Pack Undervoltage',
    'chg_overtemp':    'Charge Overtemp',
    'chg_undertemp':   'Charge Undertemp',
    'dsg_overtemp':    'Discharge Overtemp',
    'dsg_undertemp':   'Discharge Undertemp',
    'chg_overcurrent': 'Charge Overcurrent',
    'dsg_overcurrent': 'Discharge Overcurrent',
    'short_circuit':   'Short Circuit',
    'ic_error':        'IC Error',
    'mos_locked':      'MOS Locked',
}


# ── Helpers ────────────────────────────────────────────────────────

def decode_protection(raw: int) -> list:
    if not raw:
        return []
    return [PROTECTION_LABELS.get(name, name)
            for bit, name in PROTECTION_BITS.items()
            if raw & (1 << bit)]


def decode_balance(raw: int, n_cells: int) -> list:
    """Return list of 0/1 per cell indicating active balancing."""
    if raw is None or n_cells is None:
        return []
    return [1 if raw & (1 << i) else 0 for i in range(n_cells)]


def db_rows_to_dicts(rows):
    return [
        {
            "volts":          r["volts"],
            "amps":           r["amps"],
            "soc_ah":         r["soc_ah"],
            "soc_pct":        r["soc_pct"],
            "temp":           r["temp_c"],      # first probe, kept for compat
            "timestamp":      r["ts"],
            "cycles":         r["cycles"],
            "rsoc":           r["rsoc"],
            "n_cells":        r["n_cells"],
            "protection_raw": r["protection_raw"],
            "protection_flags": decode_protection(r["protection_raw"] or 0),
            "balance_raw":    r["balance_raw"],
        }
        for r in rows
    ]


def summary_rows_to_dicts(rows):
    return [
        {
            "volts":     r["volts_avg"],
            "amps":      r["amps_avg"],
            "soc_ah":    r["soc_avg"],
            "soc_pct":   r["soc_avg"],
            "temp":      r["temp_avg"],
            "timestamp": r["ts_start"] + (r["ts_end"] - r["ts_start"]) / 2,
            "amps_min":  r["amps_min"],
            "amps_max":  r["amps_max"],
            # Fields not available in summary rows
            "cycles":           None,
            "rsoc":             None,
            "protection_raw":   None,
            "protection_flags": [],
            "balance_raw":      None,
        }
        for r in rows
    ]


def get_latest():
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        row = con.execute("""
            SELECT ts, volts, amps, soc_ah, soc_pct, temp_c,
                   cycles, rsoc, n_cells, n_ntc,
                   protection_raw, balance_raw, switches, bms_version
            FROM battery_readings ORDER BY id DESC LIMIT 1
        """).fetchone()
        con.close()
        return db_rows_to_dicts([row])[0] if row else None
    except Exception as e:
        print("Error reading DB:", e)
        return None


def get_latest_full():
    """Return the most recent reading with all NTC temps and cell voltages."""
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row

        row = con.execute("""
            SELECT id, ts, mac_addr, volts, amps, soc_ah, cap_ah, soc_pct, temp_c,
                   cycles, rsoc, n_cells, n_ntc, bms_version,
                   protection_raw, balance_raw, switches, prod_date
            FROM battery_readings ORDER BY id DESC LIMIT 1
        """).fetchone()

        if not row:
            con.close()
            return None

        rid = row["id"]

        temps = con.execute("""
            SELECT probe_index, temp_c FROM temp_readings
            WHERE reading_id = ? ORDER BY probe_index ASC
        """, (rid,)).fetchall()

        cells = con.execute("""
            SELECT cell_index, cell_volts FROM cell_readings
            WHERE reading_id = ? ORDER BY cell_index ASC
        """, (rid,)).fetchall()

        con.close()

        n_cells = row["n_cells"] or 0
        result = dict(row)
        result["temps"]           = [t["temp_c"] for t in temps]
        result["cell_volts"]      = [c["cell_volts"] for c in cells]
        result["protection_flags"] = decode_protection(row["protection_raw"] or 0)
        result["balance_cells"]   = decode_balance(row["balance_raw"], n_cells)
        result["bms_version_str"] = f"{(row['bms_version'] or 0) >> 4}.{(row['bms_version'] or 0) & 0x0F}"
        return result

    except Exception as e:
        print("Error reading full status:", e)
        return None


def get_device_info():
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            "SELECT * FROM device_info ORDER BY last_seen DESC"
        ).fetchall()
        con.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print("Error reading device info:", e)
        return []


# Tier boundary: raw rows older than this are summarised
RAW_CUTOFF_DAYS = 30
RAW_CUTOFF_S    = RAW_CUTOFF_DAYS * 86_400


def query_window(seconds=None, max_points=600, from_ts=None, to_ts=None):
    """
    Fetch a time window of battery readings.

    Either pass seconds (rolling window back from now) or from_ts/to_ts
    (absolute Unix timestamps for a custom date range).
    Blends raw rows (recent) with summary rows (older than RAW_CUTOFF_S).
    """
    now = time.time()

    if from_ts is not None and to_ts is not None:
        cutoff     = from_ts
        window_end = to_ts
    else:
        cutoff     = now - (seconds or 1800)
        window_end = now

    span = window_end - cutoff
    points = []

    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row

        raw_start = max(cutoff, now - RAW_CUTOFF_S)

        # ── Raw rows ──────────────────────────────────────────────
        raw_rows = con.execute("""
            SELECT ts, volts, amps, soc_ah, soc_pct, temp_c,
                   cycles, rsoc, n_cells, protection_raw, balance_raw
            FROM battery_readings
            WHERE ts >= ? AND ts <= ?
            ORDER BY ts ASC
        """, (raw_start, window_end)).fetchall()
        points.extend(db_rows_to_dicts(raw_rows))

        # ── Summary rows (only when window extends into summarised history) ──
        if cutoff < now - RAW_CUTOFF_S:
            # Use daily buckets for spans > 60 days, hourly otherwise
            bucket = 'day' if span > 60 * 86_400 else 'hour'
            summary_rows = con.execute("""
                SELECT ts_start, ts_end, bucket, mac_addr,
                       volts_avg, amps_avg, amps_min, amps_max,
                       soc_avg, temp_avg, net_ah, n_samples
                FROM battery_summary
                WHERE ts_start >= ? AND ts_end <= ? AND bucket = ?
                ORDER BY ts_start ASC
            """, (cutoff, min(window_end, now - RAW_CUTOFF_S), bucket)).fetchall()
            points = summary_rows_to_dicts(summary_rows) + points

        con.close()

    except Exception as e:
        print("Error querying window:", e)
        return []

    if len(points) > max_points:
        step = len(points) / max_points
        points = [points[int(i * step)] for i in range(max_points)]

    return points


def compute_stats(data_points):
    if not data_points:
        return {}

    all_amps_max = [d.get("amps_max", d["amps"]) for d in data_points]
    all_amps_min = [d.get("amps_min", d["amps"]) for d in data_points]
    amps_vals    = [d["amps"] for d in data_points]
    soc_vals     = [d["soc_pct"] for d in data_points]

    net_ah = 0.0
    for i in range(1, len(data_points)):
        dt_h  = (data_points[i]["timestamp"] - data_points[i-1]["timestamp"]) / 3600.0
        avg_a = (data_points[i]["amps"] + data_points[i-1]["amps"]) / 2.0
        net_ah += avg_a * dt_h

    # Collect any protection events seen in this window
    protection_events = []
    for d in data_points:
        for flag in (d.get("protection_flags") or []):
            if flag not in protection_events:
                protection_events.append(flag)

    return {
        "amps_min":          round(min(all_amps_min), 2),
        "amps_max":          round(max(all_amps_max), 2),
        "amps_avg":          round(sum(amps_vals) / len(amps_vals), 2),
        "net_ah":            round(net_ah, 3),
        "soc_start":         round(soc_vals[0], 1),
        "soc_end":           round(soc_vals[-1], 1),
        "soc_delta":         round((soc_vals[-1] - soc_vals[0]) / 100, 4),
        "n_samples":         len(data_points),
        "protection_events": protection_events,
    }


# ── Routes ─────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def status():
    data = get_latest_full()
    if not data:
        return jsonify({"error": "no data"})

    window = query_window(1800)

    return jsonify({
        "current": data,
        "history": window,
        "stats":   compute_stats(window),
    })


@app.route("/api/history")
def historical():
    try:
        max_points = int(request.args.get("max_points", 600))
        max_points = max(10, min(max_points, 2000))

        from_ts = request.args.get("from_ts")
        to_ts   = request.args.get("to_ts")

        if from_ts is not None and to_ts is not None:
            # Absolute date range mode
            from_ts = float(from_ts)
            to_ts   = float(to_ts)
            if to_ts <= from_ts:
                return jsonify({"error": "to_ts must be after from_ts"}), 400
            points = query_window(max_points=max_points, from_ts=from_ts, to_ts=to_ts)
            return jsonify({
                "from_ts": from_ts,
                "to_ts":   to_ts,
                "data":    points,
                "stats":   compute_stats(points),
            })
        else:
            # Rolling window mode
            seconds = int(request.args.get("seconds", 1800))
            seconds = max(60, min(seconds, 60 * 60 * 24 * 365 * 2))
            points  = query_window(seconds=seconds, max_points=max_points)
            return jsonify({
                "window_seconds": seconds,
                "data":  points,
                "stats": compute_stats(points),
            })

    except (ValueError, TypeError):
        return jsonify({"error": "invalid parameters"}), 400


@app.route("/api/device")
def device_info():
    """Return hardware version, production date, etc. for all known devices."""
    return jsonify(get_device_info())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=args.port, debug=args.debug)
