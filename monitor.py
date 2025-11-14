#! /usr/bin/python
# -*- coding: utf-8 -*-

import os, time, errno, struct, re
from datetime import datetime
import crcmod.predefined
import paho.mqtt.client as mqtt
from random import randint

DEFAULT_SN = "96342210104295"  # SN por defecto si no se obtiene uno válido

battery_types = {'0': 'AGM', '1': 'Flooded', '2': 'User', '3': 'Lithium'}
voltage_ranges = {'0': 'Appliance', '1': 'UPS'}
output_sources = {'0': 'utility', '1': 'solar', '2': 'battery'}
charger_sources = {'0': 'utility first', '1': 'solar first', '2': 'solar + utility', '3': 'solar only'}
machine_types = {'00': 'Grid tie', '01': 'Off Grid', '10': 'Hybrid'}
topologies = {'0': 'transformerless', '1': 'transformer'}
output_modes = {'0': 'single machine output', '1': 'parallel output', '2': 'Phase 1 of 3 Phase output', '3': 'Phase 2 of 3 Phase output', '4': 'Phase 3 of 3 Phase output'}
pv_ok_conditions = {'0': 'As long as one unit of inverters has connect PV, parallel system will consider PV OK', '1': 'Only All of inverters have connect PV, parallel system will consider PV OK'}
pv_power_balance = {'0': 'PV input max current will be the max charged current', '1': 'PV input max power will be the sum of the max charged power and loads power'}

client = None

def now(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def connect():
    print(f'\n\n\n[{now()}] - [monitor.py] - [ MQTT Connect ]: INIT')
    global client
    client = mqtt.Client(client_id=os.environ['MQTT_CLIENT_ID'])
    client.username_pw_set(os.environ['MQTT_USER'], os.environ['MQTT_PASS'])
    client.connect(os.environ['MQTT_SERVER'])
    print(os.environ['DEVICE'])

# ---------- helpers ----------
def sanitize_id(s: str) -> str:
    if not s: return ''
    s = s.strip()
    s = s.replace('\r', '').replace('\n', '')
    return re.sub(r'[^A-Za-z0-9_-]+', '', s)

def safe_number(value):
    try: return int(value)
    except ValueError:
        try: return float(value)
        except ValueError: return value

def map_with_log(table: dict, value: str, label: str) -> str:
    if value in table: return table[value]
    print(f"[get_settings] Valor inesperado en {label}: {value} (claves válidas: {list(table.keys())})")
    return f"{label}_invalid({value})"

def send_data(data, topic):
    try:
        client.publish(topic, data, 0, True)
    except Exception as e:
        print(f'[{now()}] - [monitor.py] - [ send_data ] - Error sending to MQTT...: {e}')
        return 0
    return 1

# ---------- HID/serie ----------
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
    return r

def _flush_input(fd: int):
    try:
        while True:
            if not os.read(fd, 512): break
    except OSError:
        pass

def _write_oneshot(fd: int, frame: bytes):
    os.write(fd, frame)

def _write_split_cr_padded(fd: int, frame: bytes):
    cmd_crc, cr = frame[:-1], frame[-1:]
    os.write(fd, cmd_crc)
    os.write(fd, cr + b'\x00' * 7)

def _write_blocks8(fd: int, frame: bytes):
    CH = 8
    off = 0
    n = len(frame)
    while off < n:
        end = min(off + CH, n)
        chunk = frame[off:end]
        off = end
        if len(chunk) < CH:
            chunk = chunk + b'\x00' * (CH - len(chunk))
        os.write(fd, chunk)

def serial_command(command: str):
    DEVICE = os.environ['DEVICE']
    frame = _build_frame(command)
    print(f"[{now()}] - [monitor.py] - [ serial_command ]: Command: " + command)
    fd = None
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

        try:
            s = resp.decode('utf-8')
        except UnicodeDecodeError:
            s = resp.decode('iso-8859-1')

        print(s)
        print(f"[{now()}] - [monitor.py] - [ serial_command ]: END ({writer_name})\n")

        b = s.find('('); e = s.find('\r')
        payload = s[b+1:e] if (b != -1 and e != -1 and e > b) else s.strip()
        os.close(fd)
        return payload

    except Exception as e:
        print(f"[{now()}] - [monitor.py] - [ serial_command ] - Error: {e}")
        if fd is not None:
            try: os.close(fd)
            except: pass
        raise

def get_healthcheck(value):
    try:
        data = '{'
        data += '"Health": "OK"' if value == 'true' else '"Health": "NO OK"'
        data += '}'
    except Exception as e:
        print(f'[{now()}] - [monitor.py] - [ get_healthcheck ] - Error: {e}')
        return ''
    return data

# ---------- Lecturas ----------
# (get_parallel_data, get_data, get_qpigs2_json, get_settings siguen igual)

# ---------- MAIN ----------
def main():
    time.sleep(randint(0, 5))
    connect()

    # Obtener SN
    try:
        raw_sn = serial_command('QID')
    except Exception:
        raw_sn = ''
    sn = sanitize_id(raw_sn)
    if not sn:
        print(f"SN no válido detectado ('{raw_sn}'), usando por defecto: {DEFAULT_SN}")
        sn = DEFAULT_SN
    else:
        print(f"Raw SN: '{raw_sn}' -> Sanitized SN: '{sn}'")

    while True:
        try:
            # HealthCheck
            d = get_healthcheck('true')
            if d: send_data(d, os.environ['MQTT_HEALTHCHECK'])
            time.sleep(1)

            # QPGS0
            d = get_parallel_data()
            if d: send_data(d, os.environ['MQTT_TOPIC_PARALLEL'])
            time.sleep(1)

            # QPIGS
            d = get_data()
            if d: send_data(d, os.environ['MQTT_TOPIC'].replace('{sn}', sn))
            time.sleep(1)

            # QPIGS2
            pv2 = get_qpigs2_json()
            if pv2: send_data(pv2, os.environ['MQTT_TOPIC'].replace('{sn}', sn + '_pv2'))
            time.sleep(1)

            # QPIRI
            d = get_settings()
            if d: send_data(d, os.environ['MQTT_TOPIC_SETTINGS'])
            update_time = 2
            try:
                update_time = int(os.environ.get("UPDATE_TIME", 2))
            except ValueError:
                update_time = 2
            time.sleep(update_time)

        except Exception as e:
            d = get_healthcheck('false')
            if d: send_data(d, os.environ['MQTT_HEALTHCHECK'])
            print("Error occurred:", e)
            time.sleep(10)

if __name__ == '__main__':
    main()
