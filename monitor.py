#! /usr/bin/python
# -*- coding: utf-8 -*-

import os, time, re, random, logging
from datetime import datetime
import serial
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

# Throttle para logs repetitivos (no saturar stdout)
_last_log = {}
def log_every(key: str, level: int, msg: str, every_s: float = 60.0):
    now = time.monotonic()
    last = _last_log.get(key, 0.0)
    if now - last >= every_s:
        _last_log[key] = now
        logger.log(level, msg)

# ---------------- Helpers ----------------
battery_types = {'0': 'AGM', '1': 'Flooded', '2': 'User', '3': 'Lithium' }
voltage_ranges = {'0': 'Appliance', '1': 'UPS'}
output_sources = {'0': 'utility', '1': 'solar', '2': 'battery'}
charger_sources = {'0': 'utility first', '1': 'solar first', '2': 'solar + utility', '3': 'solar only'}
machine_types = {'00': 'Grid tie', '01': 'Off Grid', '10': 'Hybrid'}
topologies = {'0': 'transformerless', '1': 'transformer'}
output_modes = {'0': 'single machine output', '1': 'parallel output', '2': 'Phase 1 of 3 Phase output', '3': 'Phase 2 of 3 Phase output', '4': 'Phase 3 of 3 Phase output'}

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
              f"Valor inesperado {label}={value} (válidos: {list(table.keys())})",
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

# ---------------- Serial ----------------
def open_serial():
    device = os.environ.get("DEVICE", "/dev/ttyUSB0")
    baudrate = int(os.environ.get("BAUDRATE", 2400))
    ser = serial.Serial(device, baudrate=baudrate, bytesize=8, parity='N', stopbits=1, timeout=1)
    logger.info(f"Serial port {device} opened at {baudrate} baud")
    return ser

def serial_command(ser, command: str) -> str:
    """Envía comando ASCII terminado en \r y lee respuesta hasta \r"""
    try:
        cmd = (command + '\r').encode('ascii')
        ser.reset_input_buffer()
        ser.write(cmd)
        response = ser.read_until(b'\r')
        return response.decode('utf-8', errors='ignore').strip()
    except Exception as e:
        log_every(f"serial-{command}", logging.ERROR, f"{command} failed: {e}", every_s=10.0)
        return ''

# ---------------- Lecturas ----------------
def get_data(ser):
    r = serial_command(ser, 'QPIGS')
    nums = r.split(' ')
    if len(nums) < 21:
        log_every("qpigs-short", logging.WARNING, f"Respuesta corta len={len(nums)} raw='{r[:40]}…'", 30.0)
        return ''
    data = '{'
    data += '"BusVoltage":' + str(safe_number(nums[7]))
    data += ',"InverterHeatsinkTemperature":' + str(safe_number(nums[11]))
    data += ',"BatteryVoltageFromScc":' + str(safe_number(nums[14]))
    data += ',"PvInputCurrent":' + str(safe_number(nums[12]))
    data += ',"PvInputVoltage":' + str(safe_number(nums[13]))
    data += ',"PvInputPower":' + str(safe_number(nums[19]))
    data += ',"BatteryChargingCurrent": ' + str(safe_number(nums[9]))
    data += ',"BatteryDischargeCurrent": ' + str(safe_number(nums[15]))
    data += ',"DeviceStatus":"' + nums[16] + '"}'
    return data

def get_settings(ser):
    r = serial_command(ser, 'QPIRI')
    nums = r.split(' ')
    if len(nums) < 21:
        log_every("qpiri-short", logging.WARNING, f"Respuesta corta len={len(nums)} raw='{r[:40]}…'", 60.0)
        return ''
    data = '{'
    data += '"AcInputVoltage":' + str(safe_number(nums[0]))
    data += ',"AcInputCurrent":' + str(safe_number(nums[1]))
    data += ',"AcOutputVoltage":' + str(safe_number(nums[2]))
    data += ',"AcOutputFrequency":' + str(safe_number(nums[3]))
    data += ',"AcOutputCurrent":' + str(safe_number(nums[4]))
    data += ',"BatteryVoltage":' + str(safe_number(nums[7]))
    data += ',"BatteryType":"' + map_with_log(battery_types, nums[12], "BatteryType") + '"}'
    return data

# ---------------- MQTT ----------------
client = None
def connect_mqtt():
    global client
    client = mqtt.Client(client_id=os.environ['MQTT_CLIENT_ID'])
    client.username_pw_set(os.environ['MQTT_USER'], os.environ['MQTT_PASS'])
    client.connect(os.environ['MQTT_SERVER'])
    logger.info("[MQTT] Connected")

# ---------------- MAIN ----------------
def main():
    time.sleep(random.randint(0,5))
    connect_mqtt()
    ser = open_serial()

    try:
        raw_sn = serial_command(ser, 'QID')
    except Exception:
        raw_sn = 'unknown'
    sn = sanitize_id(raw_sn.strip())
    logger.info(f"Reading from inverter {sn} (raw='{raw_sn}')")

    FAST_INTERVAL    = int(os.environ.get("DATA_INTERVAL", 2))
    SETTINGS_INTERVAL= int(os.environ.get("SETTINGS_INTERVAL", 600))

    last_fast = last_settings = 0.0

    while True:
        nowm = time.monotonic()

        if nowm - last_fast >= FAST_INTERVAL:
            last_fast = nowm
            d = get_data(ser)
            if d: send_data(d, os.environ['MQTT_TOPIC'].replace('{sn}', sn))

        if nowm - last_settings >= SETTINGS_INTERVAL:
            last_settings = nowm
            d = get_settings(ser)
            if d: send_data(d, os.environ['MQTT_TOPIC_SETTINGS'].replace('{sn}', sn))

        time.sleep(0.1)

if __name__ == '__main__':
    main()
