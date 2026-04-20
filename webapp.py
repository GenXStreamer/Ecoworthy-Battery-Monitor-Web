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


# ── Helpers ────────────────────────────────────────────────────────

def db_rows_to_dicts(rows):
    return [
        {
            "volts":     r["volts"],
            "amps":      r["amps"],
            "soc_ah":    r["soc_ah"],
            "soc_pct":   r["soc_pct"],
            "temp":      r["temp_c"],       # exposed as "temp" to keep API stable
            "timestamp": r["ts"],
        }
        for r in rows
    ]


def summary_rows_to_dicts(rows):
    """Convert battery_summary rows to the same dict shape as raw rows.
    Uses avg values for line charts; amps_max/amps_min come through stats."""
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
        }
        for r in rows
    ]


def get_latest():
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        row = con.execute("""
            SELECT ts, volts, amps, soc_ah, soc_pct, temp_c
            FROM battery_readings ORDER BY id DESC LIMIT 1
        """).fetchone()
        con.close()
        return db_rows_to_dicts([row])[0] if row else None
    except Exception as e:
        print("Error reading DB:", e)
        return None


# Tier boundary: raw rows older than this are summarised
RAW_CUTOFF_DAYS = 30
RAW_CUTOFF_S    = RAW_CUTOFF_DAYS * 86_400


def query_window(seconds, max_points=600):
    """
    Fetch a time window, blending raw rows (recent) with summary rows (older).
    Returns a list of point dicts, downsampled to max_points total.
    """
    now    = time.time()
    cutoff = now - seconds

    points = []

    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row

        raw_start = max(cutoff, now - RAW_CUTOFF_S)

        # ── Raw rows (recent) ──────────────────────────────────────
        raw_rows = con.execute("""
            SELECT ts, volts, amps, soc_ah, soc_pct, temp_c
            FROM battery_readings
            WHERE ts >= ?
            ORDER BY ts ASC
        """, (raw_start,)).fetchall()
        points.extend(db_rows_to_dicts(raw_rows))

        # ── Summary rows (older, only if window extends past raw cutoff) ──
        if cutoff < now - RAW_CUTOFF_S:
            bucket = 'day' if seconds > 60 * 86_400 else 'hour'
            summary_rows = con.execute("""
                SELECT ts_start, ts_end, bucket, mac_addr,
                       volts_avg, amps_avg, amps_min, amps_max,
                       soc_avg, temp_avg, net_ah, n_samples
                FROM battery_summary
                WHERE ts_start >= ? AND ts_end <= ? AND bucket = ?
                ORDER BY ts_start ASC
            """, (cutoff, now - RAW_CUTOFF_S, bucket)).fetchall()
            points = summary_rows_to_dicts(summary_rows) + points

        con.close()

    except Exception as e:
        print("Error querying window:", e)
        return []

    # ── Downsample ─────────────────────────────────────────────────
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

    return {
        "amps_min":  round(min(all_amps_min), 2),
        "amps_max":  round(max(all_amps_max), 2),
        "amps_avg":  round(sum(amps_vals) / len(amps_vals), 2),
        "net_ah":    round(net_ah, 3),
        "soc_start": round(soc_vals[0], 1),
        "soc_end":   round(soc_vals[-1], 1),
        "soc_delta": round((soc_vals[-1] - soc_vals[0]) / 100, 4),
        "n_samples": len(data_points),
    }


# ── Routes ─────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def status():
    data = get_latest()
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
        seconds    = int(request.args.get("seconds", 1800))
        max_points = int(request.args.get("max_points", 600))
    except ValueError:
        return jsonify({"error": "invalid parameters"}), 400

    seconds    = max(60, min(seconds, 60 * 60 * 24 * 365))
    max_points = max(10, min(max_points, 2000))

    points = query_window(seconds, max_points)

    return jsonify({
        "window_seconds": seconds,
        "data":  points,
        "stats": compute_stats(points),
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=args.port, debug=args.debug)
