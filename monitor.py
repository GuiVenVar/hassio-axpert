#! /usr/bin/python
# -*- coding: utf-8 -*-

import os, time, errno, struct, re, random, logging
from datetime import datetime
import crcmod.predefined
import paho.mqtt.client as mqtt

# ---------------- Logging ----------------
def _get_log_level():
    lvl = os.environ.get("LOG_LEVEL", "INFO").upper()
    return getattr(logging, lvl, logging.INFO)

logging.basicConfig(
    level=_get_log_level(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("axpert")

# Throttle para logs repetitivos
_last_log = {}
def log_every(key: str, level: int, msg: str, every_s: float = 60.0):
    now = time.monotonic()
    last = _last_log.get(key, 0.0)
    if now - last >= every_s:
        _last_log[key] = now
        logger.log(level, msg)

# ----------------------------------------------------
battery_types = {'0': 'AGM', '1': 'Flooded', '2': 'User', '3': 'Lithium' }
voltage_ranges = {'0': 'Appliance', '1': 'UPS'}
output_sources = {'0': 'utility', '1': 'solar', '2': 'battery'}
charger_sources = {'0': 'utility first', '1': 'solar first', '2': 'solar + utility', '3': 'solar only' }
machine_types = {'00': 'Grid tie', '01': 'Off Grid', '10': 'Hybrid'}
topologies = {'0': 'transformerless', '1': 'transformer'}
output_modes = {'0': 'single machine output', '1': 'parallel output', '2': 'Phase 1 of 3 Phase output', '3': 'Phase 2 of 3 Phase output', '4': 'Phase 3 of 3 Phase output'}
pv_ok_conditions = {'0': 'As long as one unit of inverters has connect PV, parallel system will consider PV OK', '1': 'Only All of inverters have connect PV, parallel system will consider PV OK'}
pv_power_balance = {'0': 'PV input max current will be the max charged current', '1': 'PV input max power will be the sum of the max charged power and loads power'}

client = None
DEBUG_RAW = True  # <-- si quieres ver todo lo que llega, pon True

def now(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def connect():
    logger.info("[MQTT] Connecting…")
    global client
    client = mqtt.Client(client_id=os.environ['MQTT_CLIENT_ID'])
    client.username_pw_set(os.environ['MQTT_USER'], os.environ['MQTT_PASS'])
    client.connect(os.environ['MQTT_SERVER'])
    logger.info("[MQTT] Connected")

# ---------- helpers ----------
def sanitize_id(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9_-]+', '', s or '')

def safe_number(value):
    try: return int(value)
    except ValueError:
        try: return float(value)
        except ValueError: return value

def map_with_log(table: dict, value: str, label: str) -> str:
    if value in table: 
        return table[value]
    log_every(f"map-{label}", logging.WARNING,
              f"[QPIRI] Valor inesperado {label}={value} (válidos: {list(table.keys())})",
              every_s=120.0)
    return f"{label}_invalid({value})"

def send_data(data, topic):
    try:
        client.publish(topic, data, 0, True)
        logger.debug(f"[MQTT] topic={topic} bytes={len(data)}")
        return 1
    except Exception as e:
        log_every("mqtt-error", logging.ERROR, f"[MQTT] publish failed: {e}", every_s=30.0)
        return 0

# ---------- HID/Serie ----------
def _build_frame(cmd: str) -> bytes:
    xmodem_crc_func = crcmod.predefined.mkCrcFun('xmodem')
    cb = cmd.encode('ascii')
    crc = xmodem_crc_func(cb)
    crc_b = struct.pack('>H', crc)
    return cb + crc_b + b'\x0d'

def _read_until_cr(fd: int, timeout_s: float = 5.0) -> bytes:
    deadline = time.time() + timeout_s
    r = b''
    while b'\r' not in r:
        if time.time() > deadline:
            raise TimeoutError("Read operation timed out")
        try:
            c = os.read(fd, 128)
            if c: r += c
            else: time.sleep(0.01)
        except OSError as e:
            if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                time.sleep(0.01); continue
            raise
    if DEBUG_RAW:
        logger.debug(f"[RAW IN] bytes={len(r)} raw={r} hex={r.hex()}")
    return r

def _flush_input(fd: int):
    try:
        while True:
            if not os.read(fd, 512): break
    except OSError:
        pass

def _write_oneshot(fd: int, frame: bytes): os.write(fd, frame)
def _write_split_cr_padded(fd: int, frame: bytes):
    cmd_crc, cr = frame[:-1], frame[-1:]
    os.write(fd, cmd_crc); os.write(fd, cr + b'\x00' * 7)
def _write_blocks8(fd: int, frame: bytes):
    CH = 8; off = 0; n = len(frame)
    while off < n:
        end = min(off + CH, n)
        chunk = frame[off:end]
        off = end
        if len(chunk) < CH: chunk = chunk + b'\x00' * (CH - len(chunk))
        os.write(fd, chunk)

def serial_command(command: str):
    """
    Todos: probar one-shot → split-cr-padded → blocks8 (en ese orden).
    EXCEPTO QPIGS2: usar split-cr-padded directamente.
    """
    DEVICE = os.environ.get('DEVICE', '/dev/ttyUSB0')
    frame = _build_frame(command)
    fd = None
    t0 = time.monotonic()
    writer_name = "n/a"
    try:
        fd = os.open(DEVICE, os.O_RDWR | os.O_NONBLOCK)
        _flush_input(fd)

        if command == "QPIGS2":
            writer_name = "split-cr-padded"
            _write_split_cr_padded(fd, frame)
        else:
            tried = []
            last_err = None
            for writer_name, writer in (("one-shot", _write_oneshot),
                                        ("split-cr-padded", _write_split_cr_padded),
                                        ("blocks8", _write_blocks8)):
                try:
                    _flush_input(fd)
                    writer(fd, frame)
                    break
                except Exception as e:
                    tried.append(writer_name); last_err = e
                    continue
            else:
                raise last_err if last_err else OSError("all write strategies failed")

        resp = _read_until_cr(fd, timeout_s=5.0)
        dt = (time.monotonic() - t0) * 1000.0  # ms

        # decodificamos solo para procesar
        try:
            s = resp.decode('utf-8', errors='ignore')
        except Exception:
            s = resp.decode('iso-8859-1', errors='ignore')

        if DEBUG_RAW:
            logger.debug(f"[SERIAL] {command} via {writer_name} in {dt:.0f}ms bytes={len(resp)} decoded_preview='{s[:40]}'")

        b = s.find('('); e = s.find('\r')
        payload = s[b+1:e] if (b != -1 and e != -1 and e > b) else s.strip()
        return payload

    except Exception as e:
        dt = (time.monotonic() - t0) * 1000.0
        log_every(f"serial-{command}", logging.ERROR,
                  f"[SERIAL] {command} failed after {dt:.0f}ms ({writer_name}): {e}", every_s=10.0)
        raise
    finally:
        if fd is not None:
            try: os.close(fd)
            except: pass

# ---------- Lecturas ----------
def get_healthcheck(value):
    try:
        data = '{"Health":"OK"}' if value == 'true' else '{"Health":"NO OK"}'
        logger.debug("[HEALTH] prepared payload")
        return data
    except Exception as e:
        log_every("health-error", logging.ERROR, f"[HEALTH] build failed: {e}", every_s=30.0)
        return ''

# ... aquí seguirías con get_data(), get_parallel_data(), get_qpigs2_json(), get_settings() ...

# ---------- MAIN ----------
def main():
    time.sleep(random.randint(0, 5))
    connect()

    try:
        raw_sn = serial_command('QID')
    except Exception:
        raw_sn = 'unknown'
    sn = sanitize_id(raw_sn.strip())
    logger.info(f"Reading from inverter {sn} (raw='{raw_sn}')")

    FAST_INTERVAL    = int(os.environ.get("DATA_INTERVAL", 2))
    HEALTH_INTERVAL  = int(os.environ.get("HEALTH_INTERVAL", 20))
    SLOW_INTERVAL    = int(os.environ.get("SETTINGS_INTERVAL", 600))

    logger.info(f"Intervals → FAST={FAST_INTERVAL}s, HEALTH={HEALTH_INTERVAL}s, SETTINGS={SLOW_INTERVAL}s")
    last_fast = last_health = last_slow = 0.0

    while True:
        nowm = time.monotonic()
        if nowm - last_fast >= FAST_INTERVAL:
            last_fast = nowm
            try: d = get_data(); send_data(d, os.environ['MQTT_TOPIC'].replace('{sn}', sn))
            except Exception as e: log_every("qpigs-cycle", logging.ERROR, f"[CYCLE] QPIGS error: {e}", 10.0)
            try: pv2 = get_qpigs2_json(); send_data(pv2, os.environ['MQTT_TOPIC'].replace('{sn}', sn + '_pv2'))
            except Exception as e: log_every("qpigs2-cycle", logging.ERROR, f"[CYCLE] QPIGS2 error: {e}", 10.0)
            try: d = get_parallel_data(); send_data(d, os.environ['MQTT_TOPIC_PARALLEL'])
            except Exception as e: log_every("qpgs0-cycle", logging.ERROR, f"[CYCLE] QPGS0 error: {e}", 10.0)

        if nowm - last_health >= HEALTH_INTERVAL:
            last_health = nowm
            try: d = get_healthcheck('true'); send_data(d, os.environ['MQTT_HEALTHCHECK'])
            except Exception as e:
                try: send_data(get_healthcheck('false'), os.environ['MQTT_HEALTHCHECK'])
                except: pass
                log_every("health-cycle", logging.ERROR, f"[CYCLE] HealthCheck error: {e}", 20.0)

        if nowm - last_slow >= SLOW_INTERVAL:
            last_slow = nowm
            try: d = get_settings(); send_data(d, os.environ['MQTT_TOPIC_SETTINGS'])
            except Exception as e: log_every("qpiri-cycle", logging.ERROR, f"[CYCLE] QPIRI error: {e}", 30.0)

        time.sleep(0.1)

if __name__ == '__main__':
    main()
