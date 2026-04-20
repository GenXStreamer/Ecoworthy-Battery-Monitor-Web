#!/usr/bin/env python3
"""
Eco-Worthy / JBD BMS Bluetooth Battery Monitor
Written by GenXStreamer, 2026.

Connects to a JBD-protocol BMS over BLE, polls battery status, and stores
readings in SQLite.  Survives BLE disconnects with exponential back-off.

Protocol reference: JDB RS485/RS232/UART/Bluetooth Communication Protocol
  (https://jiabaida-bms.com/pages/download-files)

Requires: bluepy
"""

import argparse
import datetime
import os
import sqlite3
import sys
import time

from bluepy.btle import DefaultDelegate, Peripheral


# ---------------------------------------------------------------------------
# BLE / protocol constants
# ---------------------------------------------------------------------------

BLE_SERVICE_UUID        = 0xFF00
BLE_CHARACTERISTIC_UUID = 0xFF02

# Request payloads (JBD protocol)
CMD_BASIC_INFO    = b'\xdd\xa5\x03\x00\xff\xfd\x77'
CMD_CELL_VOLTAGES = b'\xdd\xa5\x04\x00\xff\xfc\x77'

# Response header bytes
HDR_BASIC_INFO    = b'\xdd\x03'
HDR_CELL_VOLTAGES = b'\xdd\x04'

PACKET_END_BYTE = 0x77

# ---------------------------------------------------------------------------
# Reconnect / timing
# ---------------------------------------------------------------------------

INITIAL_RECONNECT_DELAY_S = 5
MAX_RECONNECT_DELAY_S     = 60
STALE_CONNECTION_TIMEOUT_S = 15


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def db_initialise(path: str) -> None:
    """Create tables if they do not already exist."""
    with sqlite3.connect(path) as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS battery_readings (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                ts       REAL    NOT NULL,
                recorded_date  TEXT NOT NULL,
                recorded_time  TEXT NOT NULL,
                mac_addr TEXT NOT NULL,
                volts    REAL, amps     REAL,
                soc_ah   REAL, cap_ah   REAL,
                watts    REAL, soc_pct  REAL,
                temp_c   REAL, switches TEXT
            );

            CREATE TABLE IF NOT EXISTS cell_readings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                reading_id  INTEGER NOT NULL
                                REFERENCES battery_readings(id),
                cell_index  INTEGER NOT NULL,
                cell_volts  REAL    NOT NULL
            );
        """)


def db_insert_reading(path: str, row: dict) -> int:
    """Insert one battery reading; return the new row id."""
    sql = """
        INSERT INTO battery_readings
            (ts, recorded_date, recorded_time, mac_addr,
             volts, amps, soc_ah, cap_ah, watts, soc_pct, temp_c, switches)
        VALUES
            (:ts, :date, :time, :mac,
             :volts, :amps, :soc_ah, :cap_ah, :watts, :soc_pct, :temp_c, :switches)
    """
    with sqlite3.connect(path) as con:
        cur = con.execute(sql, row)
        return cur.lastrowid


def db_insert_cells(path: str, reading_id: int, cell_volts: list) -> None:
    """Insert per-cell voltage rows for a given reading."""
    rows = [(reading_id, idx, mv / 1000.0)
            for idx, mv in enumerate(cell_volts)]
    with sqlite3.connect(path) as con:
        con.executemany(
            "INSERT INTO cell_readings (reading_id, cell_index, cell_volts) VALUES (?,?,?)",
            rows
        )


# ---------------------------------------------------------------------------
# CSV helper
# ---------------------------------------------------------------------------

def csv_append(path: str, row: dict, cell_volts: list) -> None:
    file_existed = os.path.isfile(path)
    with open(path, 'a') as fh:
        if not file_existed:
            header = ("date,time,mac,volts,amps,soc_ah,cap_ah,"
                      "watts,soc_pct,temp_c,switches")
            if cell_volts:
                header += "," + ",".join(f"cell_{i:02d}"
                                         for i in range(len(cell_volts)))
            fh.write(header + "\n")

        line = (f"{row['date']},{row['time']},{row['mac']},"
                f"{row['volts']:.2f},{row['amps']:.2f},"
                f"{row['soc_ah']:.2f},{row['cap_ah']:.2f},"
                f"{row['watts']:.2f},{row['soc_pct']:.2f},"
                f"{row['temp_c']:.2f},{row['switches']}")
        if cell_volts:
            line += "," + ",".join(f"{mv/1000:.3f}" for mv in cell_volts)
        fh.write(line + "\n")


# ---------------------------------------------------------------------------
# Packet parsing  (pure functions — no class state needed)
# ---------------------------------------------------------------------------

def parse_basic_info(packet: bytes) -> dict:
    """
    Decode a 0xDD03 response packet.
    All register offsets from the JBD communication protocol document.
    """
    raw_volts   = int.from_bytes(packet[4:6],  'big')
    raw_current = int.from_bytes(packet[6:8],  'big')
    raw_soc_ah  = int.from_bytes(packet[8:10], 'big')
    raw_cap_ah  = int.from_bytes(packet[10:12],'big')
    raw_temp    = int.from_bytes(packet[27:29],'big')
    sw_byte     = packet[24]

    # Current is a signed 16-bit value
    if raw_current > 0x7FFF:
        raw_current -= 0x10000

    volts   = raw_volts   / 100.0
    amps    = raw_current / 100.0
    soc_ah  = raw_soc_ah  / 100.0
    cap_ah  = raw_cap_ah  / 100.0
    watts   = volts * amps
    soc_pct = (soc_ah / cap_ah * 100.0) if cap_ah else 0.0
    temp_c  = (raw_temp - 2731) / 10.0      # Kelvin × 10 → °C

    charge_sw  = 'C+' if (sw_byte & 0x01) else 'C-'
    discharge_sw = 'D+' if (sw_byte & 0x02) else 'D-'

    return dict(volts=volts, amps=amps, soc_ah=soc_ah, cap_ah=cap_ah,
                watts=watts, soc_pct=soc_pct, temp_c=temp_c,
                switches=charge_sw + discharge_sw)


def parse_cell_voltages(packet: bytes) -> list:
    """
    Decode a 0xDD04 response packet.
    Returns a list of cell voltages in millivolts (integers).
    """
    n_cells = packet[3] // 2        # each cell is 2 bytes
    cells   = []
    offset  = 4
    for _ in range(n_cells):
        cells.append(int.from_bytes(packet[offset:offset + 2], 'big'))
        offset += 2
    return cells


# ---------------------------------------------------------------------------
# BLE notification delegate
# ---------------------------------------------------------------------------

class BmsDelegate(DefaultDelegate):
    """
    Accumulates fragmented BLE notifications into complete JBD packets,
    then fires callbacks when a full reading is ready.
    """

    def __init__(self, on_reading, want_cells: bool):
        super().__init__()
        self._on_reading  = on_reading
        self._want_cells  = want_cells
        self._reset()

    def _reset(self):
        self._buf1      = b''   # basic-info accumulation buffer
        self._buf2      = b''   # cell-voltage accumulation buffer
        self._len1      = None
        self._len2      = None
        self._waiting_cells = False
        self._basic     = None  # parsed basic info dict
        self._ts        = None
        self.reading_ready = False

    # --- public ---

    def handleNotification(self, cHandle, data):
        if data.startswith(HDR_BASIC_INFO):
            self._buf1  = data
            self._len1  = int.from_bytes(data[2:4], 'big')
            now         = datetime.datetime.now()
            self._ts    = (time.time(),
                           now.strftime('%Y-%m-%d'),
                           now.strftime('%H:%M:%S'))
        elif data.startswith(HDR_CELL_VOLTAGES):
            self._buf2  = data
            self._len2  = int.from_bytes(data[2:4], 'big')
            self._waiting_cells = True
        elif self._waiting_cells:
            self._buf2 += data
        else:
            self._buf1 += data

        self._try_assemble()

    # --- private ---

    def _packet_complete(self, buf, expected_len):
        needed = expected_len + 7           # header(2) + len(2) + data + chk(2) + end(1)
        return (expected_len is not None
                and len(buf) >= needed
                and buf[-1] == PACKET_END_BYTE)

    def _try_assemble(self):
        if self._packet_complete(self._buf1, self._len1):
            self._basic = parse_basic_info(self._buf1)
            if not self._want_cells:
                self._fire(cell_volts=[])
                return

        if (self._basic is not None
                and self._waiting_cells
                and self._packet_complete(self._buf2, self._len2)):
            cells = parse_cell_voltages(self._buf2)
            self._fire(cell_volts=cells)

    def _fire(self, cell_volts):
        ts, date, t = self._ts
        self._on_reading(
            ts=ts, date=date, time=t,
            basic=self._basic,
            cell_volts=cell_volts
        )
        self.reading_ready = True
        self._reset()


# ---------------------------------------------------------------------------
# Reading handler  (called by the delegate)
# ---------------------------------------------------------------------------

def make_reading_handler(mac: str, csv_path, db_path):
    """Return a callback that persists and prints each complete reading."""

    def handler(ts, date, time, basic, cell_volts):
        row = dict(ts=ts, date=date, time=time, mac=mac, **basic)

        # Console summary
        print(f"[{date} {time}] {mac}  "
              f"{basic['volts']:.2f}V  {basic['amps']:+.2f}A  "
              f"{basic['soc_pct']:.1f}%  {basic['temp_c']:.1f}°C",
              flush=True)

        if csv_path:
            csv_append(csv_path, row, cell_volts)

        if db_path:
            try:
                rid = db_insert_reading(db_path, row)
                if cell_volts:
                    db_insert_cells(db_path, rid, cell_volts)
            except Exception as exc:
                print(f"  [db error] {exc}", file=sys.stderr)

    return handler


# ---------------------------------------------------------------------------
# BLE connection / polling loop
# ---------------------------------------------------------------------------

def ble_send(characteristic, payload: bytes) -> None:
    characteristic.write(payload, withResponse=False)


def run_monitor(mac: str, interval: int, want_cells: bool,
                csv_path, db_path) -> None:
    """
    Main loop.  Connects to the BMS, polls at *interval* seconds, and
    reconnects automatically if the link drops.
    """
    on_reading   = make_reading_handler(mac, csv_path, db_path)
    reconnect_delay = INITIAL_RECONNECT_DELAY_S

    while True:
        device = None
        try:
            print(f"Connecting to {mac} …")
            device = Peripheral(mac)

            delegate = BmsDelegate(on_reading, want_cells)
            device.withDelegate(delegate)

            service    = device.getServiceByUUID(BLE_SERVICE_UUID)
            char       = service.getCharacteristics(BLE_CHARACTERISTIC_UUID)[0]

            reconnect_delay = INITIAL_RECONNECT_DELAY_S   # reset on success
            last_rx         = time.time()
            last_poll       = 0.0
            pending_cells   = False

            print("Connected.  Polling …")

            while True:
                if device.waitForNotifications(1.0):
                    last_rx = time.time()
                    continue

                # Stale-connection watchdog
                if time.time() - last_rx > STALE_CONNECTION_TIMEOUT_S:
                    raise RuntimeError("No data received — connection appears stale")

                now = time.time()

                if pending_cells:
                    ble_send(char, CMD_CELL_VOLTAGES)
                    pending_cells = False

                elif now - last_poll >= interval:
                    ble_send(char, CMD_BASIC_INFO)
                    last_poll     = now
                    pending_cells = want_cells

        except Exception as exc:
            print(f"  [error] {exc}", file=sys.stderr)
        finally:
            if device:
                try:
                    device.disconnect()
                except Exception:
                    pass

        print(f"Reconnecting in {reconnect_delay}s …")
        time.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_DELAY_S)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Eco-Worthy / JBD BMS Bluetooth monitor"
    )
    p.add_argument('-m', '--mac',      required=True,
                   help='BMS Bluetooth MAC address, e.g. a5:c2:37:01:2f:ed')
    p.add_argument('-i', '--interval', type=int, default=10,
                   help='Poll interval in seconds (default: 10)')
    p.add_argument('-l', '--csv',      metavar='FILE',
                   help='Append readings to this CSV file')
    p.add_argument('-d', '--db',       metavar='FILE',
                   help='Store readings in this SQLite database')
    p.add_argument('-v', '--cells',    action='store_true',
                   help='Also read individual cell voltages')
    return p


def main():
    args = build_arg_parser().parse_args()

    if args.db:
        db_initialise(args.db)

    run_monitor(
        mac        = args.mac,
        interval   = args.interval,
        want_cells = args.cells,
        csv_path   = args.csv,
        db_path    = args.db,
    )


if __name__ == '__main__':
    main()
