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
CMD_HW_VERSION    = b'\xdd\xa5\x05\x00\xff\xfb\x77'

# Response header bytes
HDR_BASIC_INFO    = b'\xdd\x03'
HDR_CELL_VOLTAGES = b'\xdd\x04'
HDR_HW_VERSION    = b'\xdd\x05'

PACKET_END_BYTE = 0x77

# ---------------------------------------------------------------------------
# Protection status bit definitions (from protocol spec)
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Reconnect / timing
# ---------------------------------------------------------------------------

INITIAL_RECONNECT_DELAY_S  = 5
MAX_RECONNECT_DELAY_S      = 60
STALE_CONNECTION_TIMEOUT_S = 15


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def db_initialise(path: str) -> None:
    """Create tables if they do not already exist."""
    with sqlite3.connect(path) as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS battery_readings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ts              REAL    NOT NULL,
                recorded_date   TEXT    NOT NULL,
                recorded_time   TEXT    NOT NULL,
                mac_addr        TEXT    NOT NULL,
                volts           REAL,
                amps            REAL,
                soc_ah          REAL,
                cap_ah          REAL,
                watts           REAL,
                soc_pct         REAL,
                temp_c          REAL,           -- first NTC (backwards compat)
                switches        TEXT,
                cycles          INTEGER,
                rsoc            INTEGER,        -- BMS-reported SOC %
                n_cells         INTEGER,
                n_ntc           INTEGER,
                bms_version     INTEGER,        -- raw byte e.g. 0x10 = v1.0
                protection_raw  INTEGER,        -- raw 16-bit protection flags
                balance_raw     INTEGER,        -- raw 32-bit balance flags
                prod_date       TEXT            -- decoded YYYY-MM-DD
            );

            -- One row per NTC probe per reading (covers multi-probe packs)
            CREATE TABLE IF NOT EXISTS temp_readings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                reading_id  INTEGER NOT NULL REFERENCES battery_readings(id),
                probe_index INTEGER NOT NULL,
                temp_c      REAL    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cell_readings (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                reading_id  INTEGER NOT NULL REFERENCES battery_readings(id),
                cell_index  INTEGER NOT NULL,
                cell_volts  REAL    NOT NULL
            );

            -- One row per physical device; updated on connect
            CREATE TABLE IF NOT EXISTS device_info (
                mac_addr    TEXT PRIMARY KEY,
                hw_version  TEXT,
                first_seen  REAL,
                last_seen   REAL,
                prod_date   TEXT
            );
        """)


def db_insert_reading(path: str, row: dict) -> int:
    sql = """
        INSERT INTO battery_readings
            (ts, recorded_date, recorded_time, mac_addr,
             volts, amps, soc_ah, cap_ah, watts, soc_pct,
             temp_c, switches, cycles, rsoc, n_cells, n_ntc,
             bms_version, protection_raw, balance_raw, prod_date)
        VALUES
            (:ts, :date, :time, :mac,
             :volts, :amps, :soc_ah, :cap_ah, :watts, :soc_pct,
             :temp_c, :switches, :cycles, :rsoc, :n_cells, :n_ntc,
             :bms_version, :protection_raw, :balance_raw, :prod_date)
    """
    with sqlite3.connect(path) as con:
        cur = con.execute(sql, row)
        return cur.lastrowid


def db_insert_temps(path: str, reading_id: int, temps: list) -> None:
    rows = [(reading_id, idx, t) for idx, t in enumerate(temps)]
    with sqlite3.connect(path) as con:
        con.executemany(
            "INSERT INTO temp_readings (reading_id, probe_index, temp_c) VALUES (?,?,?)",
            rows
        )


def db_insert_cells(path: str, reading_id: int, cell_mv: list) -> None:
    rows = [(reading_id, idx, mv / 1000.0) for idx, mv in enumerate(cell_mv)]
    with sqlite3.connect(path) as con:
        con.executemany(
            "INSERT INTO cell_readings (reading_id, cell_index, cell_volts) VALUES (?,?,?)",
            rows
        )


def db_upsert_device(path: str, mac: str, hw_version: str, prod_date: str) -> None:
    with sqlite3.connect(path) as con:
        now = time.time()
        con.execute("""
            INSERT INTO device_info (mac_addr, hw_version, first_seen, last_seen, prod_date)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(mac_addr) DO UPDATE SET
                hw_version = excluded.hw_version,
                last_seen  = excluded.last_seen,
                prod_date  = COALESCE(excluded.prod_date, prod_date)
        """, (mac, hw_version, now, now, prod_date))


# ---------------------------------------------------------------------------
# CSV helper
# ---------------------------------------------------------------------------

def csv_append(path: str, row: dict, temps: list, cell_mv: list) -> None:
    file_existed = os.path.isfile(path)
    with open(path, 'a') as fh:
        if not file_existed:
            header = ("date,time,mac,volts,amps,soc_ah,cap_ah,watts,soc_pct,"
                      "cycles,rsoc,protection_raw,balance_raw")
            for i in range(len(temps)):
                header += f",temp_c_{i}"
            for i in range(len(cell_mv)):
                header += f",cell_{i:02d}"
            fh.write(header + "\n")

        line = (f"{row['date']},{row['time']},{row['mac']},"
                f"{row['volts']:.2f},{row['amps']:.2f},"
                f"{row['soc_ah']:.2f},{row['cap_ah']:.2f},"
                f"{row['watts']:.2f},{row['soc_pct']:.2f},"
                f"{row['cycles']},{row['rsoc']},"
                f"{row['protection_raw']},{row['balance_raw']}")
        for t in temps:
            line += f",{t:.1f}"
        for mv in cell_mv:
            line += f",{mv/1000:.3f}"
        fh.write(line + "\n")


# ---------------------------------------------------------------------------
# Packet parsing
# ---------------------------------------------------------------------------

def decode_prod_date(raw: int) -> str:
    """Decode JBD packed production date to ISO string."""
    day   =  raw & 0x1F
    month = (raw >> 5)  & 0x0F
    year  = (raw >> 9)  + 2000
    try:
        return datetime.date(year, month, day).isoformat()
    except ValueError:
        return None


def parse_basic_info(packet: bytes) -> dict:
    """
    Decode a full 0xDD03 response packet.
    Register offsets per JBD communication protocol V4.

    Packet layout (data section, starting at byte 4):
      [4:6]   total voltage      (10mV units)
      [6:8]   current            (10mA, signed 16-bit)
      [8:10]  remaining cap      (10mAh)
      [10:12] nominal cap        (10mAh)
      [12:14] cycle count
      [14:16] production date    (packed)
      [16:18] balance flags low  (cells 1-16)
      [18:20] balance flags high (cells 17-32)
      [20:22] protection status
      [22]    software version
      [23]    RSOC %
      [24]    FET / switch status
      [25]    number of cell strings
      [26]    number of NTC probes
      [27+]   NTC temperatures   (2 bytes each, 0.1K absolute)
    """
    # Minimum packet length: header(4) + fixed fields up to n_ntc(27) = 27 bytes
    # plus at least the checksum(2) + end(1) = 30 bytes total before NTC data.
    if len(packet) < 30:
        raise ValueError(f"Packet too short for basic info: {len(packet)} bytes — {packet.hex()}")

    raw_volts    = int.from_bytes(packet[4:6],   'big')
    raw_current  = int.from_bytes(packet[6:8],   'big')
    raw_soc_ah   = int.from_bytes(packet[8:10],  'big')
    raw_cap_ah   = int.from_bytes(packet[10:12], 'big')
    cycles       = int.from_bytes(packet[12:14], 'big')
    raw_date     = int.from_bytes(packet[14:16], 'big')
    bal_low      = int.from_bytes(packet[16:18], 'big')
    bal_high     = int.from_bytes(packet[18:20], 'big')
    protection   = int.from_bytes(packet[20:22], 'big')
    bms_version  = packet[22]
    rsoc         = packet[23]
    sw_byte      = packet[24]
    n_cells      = packet[25]
    n_ntc        = packet[26]

    # Validate we have enough bytes for all NTC probes
    expected_min = 27 + (n_ntc * 2) + 3   # data + checksum(2) + end(1)
    if len(packet) < expected_min:
        raise ValueError(
            f"Packet too short for {n_ntc} NTC probes: "
            f"need {expected_min} bytes, got {len(packet)} — {packet.hex()}"
        )

    # Signed current
    if raw_current > 0x7FFF:
        raw_current -= 0x10000

    volts    = raw_volts   / 100.0
    amps     = raw_current / 100.0
    soc_ah   = raw_soc_ah  / 100.0
    cap_ah   = raw_cap_ah  / 100.0
    watts    = volts * amps
    soc_pct  = (soc_ah / cap_ah * 100.0) if cap_ah else 0.0

    # All NTC temperatures
    temps = []
    offset = 27
    for _ in range(n_ntc):
        raw_t = int.from_bytes(packet[offset:offset + 2], 'big')
        temps.append((raw_t - 2731) / 10.0)
        offset += 2

    charge_sw    = 'C+' if (sw_byte & 0x01) else 'C-'
    discharge_sw = 'D+' if (sw_byte & 0x02) else 'D-'

    # Combined 32-bit balance register
    balance_raw = (bal_high << 16) | bal_low

    return dict(
        volts        = volts,
        amps         = amps,
        soc_ah       = soc_ah,
        cap_ah       = cap_ah,
        watts        = watts,
        soc_pct      = soc_pct,
        temp_c       = temps[0] if temps else None,   # first probe for compat
        switches     = charge_sw + discharge_sw,
        cycles       = cycles,
        rsoc         = rsoc,
        n_cells      = n_cells,
        n_ntc        = n_ntc,
        bms_version  = bms_version,
        protection_raw = protection,
        balance_raw  = balance_raw,
        prod_date    = decode_prod_date(raw_date),
        temps        = temps,                         # all probes
    )


def parse_cell_voltages(packet: bytes) -> list:
    """Decode 0xDD04 response. Returns list of millivolt integers."""
    n_cells = packet[3] // 2
    cells   = []
    offset  = 4
    for _ in range(n_cells):
        cells.append(int.from_bytes(packet[offset:offset + 2], 'big'))
        offset += 2
    return cells


def parse_hw_version(packet: bytes) -> str:
    """Decode 0xDD05 response. Returns ASCII device/version string."""
    length = packet[3]
    return packet[4:4 + length].decode('ascii', errors='replace').strip()


def decode_protection_flags(raw: int) -> list:
    """Return list of active protection flag names."""
    return [name for bit, name in PROTECTION_BITS.items() if raw & (1 << bit)]


# ---------------------------------------------------------------------------
# BLE notification delegate
# ---------------------------------------------------------------------------

class BmsDelegate(DefaultDelegate):
    """
    Accumulates fragmented BLE notifications into complete JBD packets,
    then fires callbacks when a full reading is ready.
    """

    def __init__(self, on_reading, on_hw_version, want_cells: bool):
        super().__init__()
        self._on_reading    = on_reading
        self._on_hw_version = on_hw_version
        self._want_cells    = want_cells
        self._reset()

    def _reset(self):
        self._buf1          = b''
        self._buf2          = b''
        self._buf3          = b''
        self._len1          = None
        self._len2          = None
        self._len3          = None
        self._waiting_cells = False
        self._basic         = None
        self._ts            = None
        self.reading_ready  = False

    def handleNotification(self, cHandle, data):
        print(f"  [rx] {data.hex()}", flush=True)

        if data.startswith(HDR_BASIC_INFO):
            self._buf1 = data
            self._len1 = int.from_bytes(data[2:4], 'big')
            now = datetime.datetime.now()
            self._ts = (time.time(),
                        now.strftime('%Y-%m-%d'),
                        now.strftime('%H:%M:%S'))
            print(f"  [rx] basic-info header, expecting {self._len1} data bytes", flush=True)
        elif data.startswith(HDR_CELL_VOLTAGES):
            self._buf2 = data
            self._len2 = int.from_bytes(data[2:4], 'big')
            self._waiting_cells = True
            print(f"  [rx] cell-voltage header, expecting {self._len2} data bytes", flush=True)
        elif data.startswith(HDR_HW_VERSION):
            self._buf3 = data
            self._len3 = int.from_bytes(data[2:4], 'big')
            print(f"  [rx] hw-version header, expecting {self._len3} data bytes", flush=True)
        elif self._waiting_cells:
            self._buf2 += data
            print(f"  [rx] cell continuation, buf={len(self._buf2)}/{self._len2+7 if self._len2 else '?'}", flush=True)
        elif self._len3 is not None and not self._packet_complete(self._buf3, self._len3):
            self._buf3 += data
            print(f"  [rx] hw-version continuation, buf={len(self._buf3)}/{self._len3+7 if self._len3 else '?'}", flush=True)
        else:
            self._buf1 += data
            print(f"  [rx] basic continuation, buf={len(self._buf1)}/{self._len1+7 if self._len1 else '?'}", flush=True)

        self._try_assemble()

    def _packet_complete(self, buf, expected_len):
        # Packet structure: header(2) + status(1) + length(1) + data(N) + checksum(2) + end(1)
        # Total = expected_len + 7.  We use ONLY the length field as the
        # completion signal — the end byte 0x77 can appear in data and is
        # unreliable as a terminator when checked mid-accumulation.
        if expected_len is None:
            return False
        needed = expected_len + 7
        if len(buf) < needed:
            return False
        if buf[-1] != PACKET_END_BYTE:
            print(f"  [warn] packet end byte expected 0x77, got 0x{buf[-1]:02x} — {buf.hex()}",
                  flush=True)
            return False
        return True

    def _try_assemble(self):
        # Hardware version — fire immediately, no reading cycle needed
        if self._packet_complete(self._buf3, self._len3):
            hw = parse_hw_version(self._buf3)
            self._on_hw_version(hw)
            self._buf3 = b''
            self._len3 = None

        # Basic info
        if self._packet_complete(self._buf1, self._len1):
            self._basic = parse_basic_info(self._buf1)
            if not self._want_cells:
                self._fire(cell_mv=[])
                return

        # Cell voltages
        if (self._basic is not None
                and self._waiting_cells
                and self._packet_complete(self._buf2, self._len2)):
            cells = parse_cell_voltages(self._buf2)
            self._fire(cell_mv=cells)

    def _fire(self, cell_mv):
        ts, date, t = self._ts
        self._on_reading(
            ts=ts, date=date, time=t,
            basic=self._basic,
            cell_mv=cell_mv,
        )
        self.reading_ready = True
        self._reset()


# ---------------------------------------------------------------------------
# Reading handler
# ---------------------------------------------------------------------------

def make_reading_handler(mac: str, csv_path, db_path):

    def handler(ts, date, time, basic, cell_mv):
        temps = basic.pop('temps')          # extract multi-probe list
        row   = dict(ts=ts, date=date, time=time, mac=mac, **basic)

        # Active protection flags for console
        flags = decode_protection_flags(basic['protection_raw'])
        flag_str = ' [' + ','.join(flags) + ']' if flags else ''

        # All temps for console
        temp_str = '  '.join(f"T{i}:{t:.1f}°C" for i, t in enumerate(temps))

        print(f"[{date} {time}] {mac}  "
              f"{basic['volts']:.2f}V  {basic['amps']:+.2f}A  "
              f"{basic['soc_pct']:.1f}% ({basic['rsoc']}% BMS)  "
              f"{temp_str}  "
              f"cycles:{basic['cycles']}{flag_str}",
              flush=True)

        if csv_path:
            csv_append(csv_path, row, temps, cell_mv)

        if db_path:
            try:
                rid = db_insert_reading(db_path, row)
                if temps:
                    db_insert_temps(db_path, rid, temps)
                if cell_mv:
                    db_insert_cells(db_path, rid, cell_mv)
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
    reconnect_delay = INITIAL_RECONNECT_DELAY_S
    hw_version_cache = [None]   # mutable so closure can update it

    def on_hw_version(version: str):
        if version != hw_version_cache[0]:
            hw_version_cache[0] = version
            print(f"  Hardware version: {version}")
            if db_path:
                # prod_date will be filled in by the first reading
                db_upsert_device(db_path, mac, version, None)

    on_reading = make_reading_handler(mac, csv_path, db_path)

    while True:
        device = None
        try:
            print(f"Connecting to {mac} …")
            device = Peripheral(mac)

            delegate = BmsDelegate(on_reading, on_hw_version, want_cells)
            device.withDelegate(delegate)

            service = device.getServiceByUUID(BLE_SERVICE_UUID)
            char    = service.getCharacteristics(BLE_CHARACTERISTIC_UUID)[0]

            reconnect_delay = INITIAL_RECONNECT_DELAY_S
            last_rx         = time.time()
            last_poll       = 0.0
            pending_cells   = False
            hw_queried      = False

            print("Connected.  Polling …")

            while True:
                if device.waitForNotifications(1.0):
                    last_rx = time.time()
                    continue

                if time.time() - last_rx > STALE_CONNECTION_TIMEOUT_S:
                    raise RuntimeError("No data received — connection appears stale")

                now = time.time()

                # Request hardware version once per connection
                if not hw_queried:
                    ble_send(char, CMD_HW_VERSION)
                    hw_queried = True

                elif pending_cells:
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
