#!/usr/bin/env python3
"""
victron-monitor.py  –  Victron SmartSolar MPPT BLE monitor
Written by GenXStreamer, 2026.

Listens for Instant Readout BLE advertisements from a Victron SmartSolar
MPPT controller, stores readings in SQLite, and survives BLE dropouts.

Requires:
    pip install victron_ble

Usage:
    python victron-monitor.py \\
        -a e6:48:60:86:5f:74 \\
        -k 77c7a452364b4fe7de1d0d407949797f \\
        -d /home/dan/BatMon/batt.db \\
        -i 10

The address and key come from the Victron Connect app:
  Device → Settings (gear) → Product Info → Instant Readout Details
"""

import argparse
import asyncio
import datetime
import sqlite3
import sys
import time

from victron_ble.devices import detect_device_type
from victron_ble.devices.base import DeviceData
from bleak import BleakScanner


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def db_initialise(path: str) -> None:
    with sqlite3.connect(path) as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS solar_readings (
                id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                ts                       REAL    NOT NULL,
                recorded_date            TEXT    NOT NULL,
                recorded_time            TEXT    NOT NULL,
                address                  TEXT    NOT NULL,
                device_name              TEXT,
                solar_power_w            REAL,
                battery_voltage_v        REAL,
                battery_charging_amps    REAL,
                battery_charging_watts   REAL,
                load_watts               REAL,
                load_amps                REAL,
                solar_amps               REAL,
                charge_state             TEXT,
                charger_error            TEXT,
                yield_today_wh           REAL
            );

            CREATE TABLE IF NOT EXISTS solar_device_info (
                address     TEXT PRIMARY KEY,
                device_name TEXT,
                model_name  TEXT,
                enc_key     TEXT,
                first_seen  REAL,
                last_seen   REAL
            );

            CREATE INDEX IF NOT EXISTS idx_solar_ts
                ON solar_readings (ts);
            CREATE INDEX IF NOT EXISTS idx_solar_addr
                ON solar_readings (address, ts);
        """)


def db_insert_reading(path: str, row: dict) -> int:
    sql = """
        INSERT INTO solar_readings
            (ts, recorded_date, recorded_time, address, device_name,
             solar_power_w, solar_amps, battery_voltage_v, battery_charging_amps,
             battery_charging_watts, load_watts, load_amps,
             charge_state, charger_error, yield_today_wh)
        VALUES
            (:ts, :date, :time, :address, :device_name,
             :solar_power_w, :solar_amps, :battery_voltage_v, :battery_charging_amps,
             :battery_charging_watts, :load_watts, :load_amps,
             :charge_state, :charger_error, :yield_today_wh)
    """
    with sqlite3.connect(path) as con:
        cur = con.execute(sql, row)
        return cur.lastrowid


def db_upsert_device(path: str, address: str, name: str, model: str, key: str) -> None:
    with sqlite3.connect(path) as con:
        now = time.time()
        con.execute("""
            INSERT INTO solar_device_info
                (address, device_name, model_name, enc_key, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(address) DO UPDATE SET
                device_name = excluded.device_name,
                model_name  = excluded.model_name,
                last_seen   = excluded.last_seen
        """, (address, name, model, key, now, now))


# ---------------------------------------------------------------------------
# Payload parsing
# ---------------------------------------------------------------------------

def parse_payload(payload: DeviceData, device_name: str, address: str) -> dict:
    """
    Extract fields from the victron_ble parsed payload object.
    victron_ble returns a parsed dataclass-like object; access via get_data()
    which returns a plain dict, then fall back to direct attribute access.
    """
    # parse() returns a SolarChargerData object whose fields live in _data dict.
    # Enums (charge_state, charger_error) have a .name attribute.
    data = getattr(payload, '_data', {}) or {}

    def get(key, default=None):
        v = data.get(key, default)
        if hasattr(v, 'name'):      # coerce enum → lowercase string
            v = v.name.lower()
        return v

    solar_w   = get('solar_power')
    batt_v    = get('battery_voltage')
    batt_a    = get('battery_charging_current')
    charge_st = get('charge_state')
    error     = get('charger_error')
    yield_wh  = get('yield_today')

    # Normalise charge state / error — may already be strings or enums
    if hasattr(charge_st, 'name'):
        charge_st = charge_st.name.lower()
    if hasattr(error, 'name'):
        error = error.name.lower()

    # Watts going into the battery
    batt_w = None
    if batt_v is not None and batt_a is not None:
        batt_w = round(batt_v * batt_a, 1)

    # Solar amps = solar watts / battery voltage (MPPT output current)
    solar_a = None
    if solar_w is not None and batt_v is not None and batt_v > 0:
        solar_a = round(solar_w / batt_v, 2)

    # Load watts = solar minus what goes to battery
    load_w = None
    if solar_w is not None and batt_w is not None:
        load_w = round(solar_w - batt_w, 1)

    # Load amps = load watts / battery voltage
    load_a = None
    if load_w is not None and batt_v is not None and batt_v > 0:
        load_a = round(load_w / batt_v, 2)

    now = datetime.datetime.now()

    return dict(
        ts                    = time.time(),
        date                  = now.strftime('%Y-%m-%d'),
        time                  = now.strftime('%H:%M:%S'),
        address               = address.upper(),
        device_name           = device_name,
        solar_power_w         = solar_w,
        solar_amps            = solar_a,
        battery_voltage_v     = batt_v,
        battery_charging_amps = batt_a,
        battery_charging_watts= batt_w,
        load_watts            = load_w,
        load_amps             = load_a,
        charge_state          = charge_st,
        charger_error         = error,
        yield_today_wh        = yield_wh,
    )


# ---------------------------------------------------------------------------
# BLE scanner
# ---------------------------------------------------------------------------

class VictronScanner:
    """
    Listens for Victron Instant Readout BLE advertisements using BleakScanner.
    Uses detect_device_type() exactly as documented in the victron_ble README.
    """

    VICTRON_MANUFACTURER_ID = 0x02E1

    def __init__(self, address: str, key: str, on_reading, interval: int):
        self._address     = address.upper()
        self._key         = key           # hex string, passed straight to parser()
        self._on_reading  = on_reading
        self._interval    = interval
        self._last_stored = 0.0
        self._device_name = None

    async def run(self):
        print(f"Scanning for {self._address} …")

        def callback(device, advertisement_data):
            if device.address.upper() != self._address:
                return

            now = time.time()
            if now - self._last_stored < self._interval:
                return

            # Extract Victron manufacturer data
            raw = advertisement_data.manufacturer_data.get(self.VICTRON_MANUFACTURER_ID)
            if raw is None:
                return

            try:
                # Exactly as per the README:
                #   parser = detect_device_type(data)
                #   parsed_data = parser(key).parse(data)
                parser_class = detect_device_type(raw)
                if parser_class is None:
                    return

                parsed = parser_class(self._key).parse(raw)
                name   = device.name or 'Victron'

                if self._device_name != name:
                    self._device_name = name
                    print(f"  Device: {name}", flush=True)

                self._last_stored = now
                self._on_reading(parsed, name, device.address)

            except Exception as exc:
                print(f"  [parse error] {exc}", file=sys.stderr)

        async with BleakScanner(callback) as _:
            print("Scanner running — press Ctrl+C to stop.")
            while True:
                await asyncio.sleep(1)


# ---------------------------------------------------------------------------
# Reading handler
# ---------------------------------------------------------------------------

def make_handler(db_path: str, enc_key: str):
    model_recorded = [False]

    def handler(payload, device_name: str, address: str):
        row = parse_payload(payload, device_name, address)

        # Console summary
        solar  = row['solar_power_w']
        batt_w = row['battery_charging_watts']
        load   = row['load_watts']
        state  = row['charge_state'] or '?'
        error  = row['charger_error'] or 'ok'
        today  = row['yield_today_wh']

        solar_a = row['solar_amps']
        load_a  = row['load_amps']
        err_str = '' if error in ('ok', 'no_error') else f'  ⚠ {error}'
        print(
            f"[{row['date']} {row['time']}] {device_name}  "
            f"Solar: {solar}W ({solar_a}A)  "
            f"→Batt: {batt_w}W ({row['battery_charging_amps']}A)  "
            f"Load: {load}W ({load_a}A)  "
            f"State: {state}  Today: {today}Wh{err_str}",
            flush=True
        )

        if db_path:
            try:
                db_insert_reading(db_path, row)

                if not model_recorded[0]:
                    model = getattr(payload, 'model_name', None)
                    if hasattr(model, '__call__'):
                        model = model()
                    db_upsert_device(db_path, address, device_name,
                                     str(model or ''), enc_key)
                    model_recorded[0] = True

            except Exception as exc:
                print(f"  [db error] {exc}", file=sys.stderr)

    return handler


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Victron SmartSolar MPPT BLE monitor"
    )
    p.add_argument('-a', '--address', required=True,
                   help='Device BLE address, e.g. e6:48:60:86:5f:74')
    p.add_argument('-k', '--key',     required=True,
                   help='Instant Readout encryption key (hex string)')
    p.add_argument('-d', '--db',      metavar='FILE',
                   help='SQLite database path (shared with battery monitor)')
    p.add_argument('-i', '--interval', type=int, default=10,
                   help='Store a reading every N seconds (default: 10)')
    return p


def main():
    args = build_arg_parser().parse_args()

    if args.db:
        db_initialise(args.db)

    handler = make_handler(args.db, args.key)
    scanner = VictronScanner(
        address    = args.address,
        key        = args.key,
        on_reading = handler,
        interval   = args.interval,
    )

    try:
        asyncio.run(scanner.run())
    except KeyboardInterrupt:
        print("\nStopped.")
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
