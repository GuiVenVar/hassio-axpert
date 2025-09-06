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

# Throttle para logs repetitivos (no saturar stdout)
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
charger_sources = {'0': 'utility first', '1': 'solar first', '2': 'solar + utility', '3': 'solar only'}
machine_types = {'00': 'Grid tie', '01': 'Off Grid', '10': 'Hybrid'}
topologies = {'0': 'transformerless', '1': 'transformer'}
output_modes = {'0': 'single machine output', '1': 'parallel output', '2': 'Phase 1 of 3 Phase output', '3': 'Phase 2 of 3 Phase output', '4': 'Phase 3 of 3 Phase output'}
pv_ok_conditions = {'0': 'As long as one unit of inverters has connect PV, parallel system will consider PV OK', '1': 'Only All of inverters have connect PV, parallel system will consider PV OK'}
pv_power_balance = {'0': 'PV input max current will be the max charged current', '1': 'PV input max power will be the sum of the max charged power and loads power'}

client = None

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
    DEVICE = os.environ['DEVICE']
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

        try:
            s = resp.decode('utf-8', errors='ignore')
        except Exception:
            s = resp.decode('iso-8859-1', errors='ignore')

        # No volcamos s completa para no saturar; medimos longitud y primeros tokens
        preview = s.strip().split()
        preview = " ".join(preview[:3])  # 3 primeros tokens para diagnóstico
        logger.debug(f"[SERIAL] {command} via {writer_name} in {dt:.0f}ms bytes={len(resp)} prev='{preview}'")

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

def get_healthcheck(value):
    try:
        data = '{"Health":"OK"}' if value == 'true' else '{"Health":"NO OK"}'
        logger.debug("[HEALTH] prepared payload")
        return data
    except Exception as e:
        log_every("health-error", logging.ERROR, f"[HEALTH] build failed: {e}", every_s=30.0)
        return ''

# ---------- Lecturas ----------
def get_parallel_data():
    try:
        r = serial_command('QPGS0')
        nums = r.split(' ')
        if len(nums) < 27: 
            log_every("qpgs0-short", logging.WARNING, f"[QPGS0] respuesta corta len={len(nums)} raw='{r[:40]}…'", 30.0)
            return ''
        data = '{'
        data += '"Gridmode":' + ('1' if nums[2]=='L' else '0')
        data += ',"SerialNumber": ' + str(safe_number(nums[1]))
        data += ',"BatteryChargingCurrent": ' + str(safe_number(nums[12]))
        data += ',"BatteryDischargeCurrent": ' + str(safe_number(nums[26]))
        data += ',"TotalChargingCurrent": ' + str(safe_number(nums[15]))
        data += ',"GridVoltage": ' + str(safe_number(nums[4]))
        data += ',"GridFrequency": ' + str(safe_number(nums[5]))
        data += ',"OutputVoltage": ' + str(safe_number(nums[6]))
        data += ',"OutputFrequency": ' + str(safe_number(nums[7]))
        data += ',"OutputAparentPower": ' + str(safe_number(nums[8]))
        data += ',"OutputActivePower": ' + str(safe_number(nums[9]))
        data += ',"LoadPercentage": ' + str(safe_number(nums[10]))
        data += ',"BatteryVoltage": ' + str(safe_number(nums[11]))
        data += ',"BatteryCapacity": ' + str(safe_number(nums[13]))
        data += ',"PvInputVoltage": ' + str(safe_number(nums[14]))
        data += ',"TotalAcOutputApparentPower": ' + str(safe_number(nums[16]))
        data += ',"TotalAcOutputActivePower": ' + str(safe_number(nums[17]))
        data += ',"TotalAcOutputPercentage": ' + str(safe_number(nums[18]))
        data += ',"OutputMode": ' + str(safe_number(nums[20]))
        data += ',"ChargerSourcePriority": ' + str(safe_number(nums[21]))
        data += ',"MaxChargeCurrent": ' + str(safe_number(nums[22]))
        data += ',"MaxChargerRange": ' + str(safe_number(nums[23]))
        data += ',"MaxAcChargerCurrent": ' + str(safe_number(nums[24]))
        data += ',"PvInputCurrentForBattery": ' + str(safe_number(nums[25]))
        data += ',"Solarmode":' + ('1' if nums[2]=='B' else '0') + '}'
        logger.debug("[QPGS0] payload built")
        return data
    except Exception as e:
        log_every("qpgs0-error", logging.ERROR, f"[QPGS0] error: {e}", every_s=10.0)
        return ''

def get_data():
    try:
        r = serial_command('QPIGS')
        nums = r.split(' ')
        if len(nums) < 21:
            log_every("qpigs-short", logging.WARNING, f"[QPIGS] respuesta corta len={len(nums)} raw='{r[:40]}…'", 30.0)
            return ''
        data = '{'
        data += '"BusVoltage":' + str(safe_number(nums[7]))
        data += ',"InverterHeatsinkTemperature":' + str(safe_number(nums[11]))
        data += ',"BatteryVoltageFromScc":' + str(safe_number(nums[14]))
        data += ',"PvInputCurrent":' + str(safe_number(nums[12]))
        data += ',"PvInputVoltage":' + str(safe_number(nums[13]))
        data += ',"PvInputPower":' + str(safe_number(nums[19]))
        data += ',"BatteryChargingCurrent": ' + str(safe_number(nums[9]))
        data += ',"BatteryDischargeCurrent":' + str(safe_number(nums[15]))
        data += ',"DeviceStatus":"' + nums[16] + '"}'
        logger.debug("[QPIGS] payload built")
        return data
    except Exception as e:
        log_every("qpigs-error", logging.ERROR, f"[QPIGS] error: {e}", every_s=10.0)
        return ''

def get_qpigs2_json():
    try:
        r = serial_command('QPIGS2')
        parts = r.split()
        if len(parts) >= 3:
            try:
                pv2_i = float(parts[0]); pv2_v = float(parts[1]); pv2_p = float(parts[2])
                if pv2_p <= 0: pv2_p = round(pv2_v * pv2_i, 1)
                logger.debug(f"[QPIGS2] I={pv2_i}A V={pv2_v}V P={pv2_p}W")
                return '{' + f'"Pv2InputCurrent": {pv2_i}, "Pv2InputVoltage": {pv2_v}, "Pv2InputPower": {pv2_p}' + '}'
            except Exception as pe:
                log_every("qpigs2-parse", logging.WARNING, f"[QPIGS2] parse error: {pe} raw='{r[:40]}…'", 30.0)
                return ''
        else:
            log_every("qpigs2-short", logging.WARNING, f"[QPIGS2] respuesta corta: '{r}'", 30.0)
            return ''
    except Exception as e:
        log_every("qpigs2-error", logging.ERROR, f"[QPIGS2] error: {e}", every_s=10.0)
        return ''

def get_settings():
    try:
        r = serial_command('QPIRI')
        nums = r.split(' ')
        if len(nums) < 21:
            log_every("qpiri-short", logging.WARNING, f"[QPIRI] respuesta corta len={len(nums)} raw='{r[:40]}…'", 60.0)
            return ''
        data = '{'
        data += '"AcInputVoltage":' + str(safe_number(nums[0]))
        data += ',"AcInputCurrent":' + str(safe_number(nums[1]))
        data += ',"AcOutputVoltage":' + str(safe_number(nums[2]))
        data += ',"AcOutputFrequency":' + str(safe_number(nums[3]))
        data += ',"AcOutputCurrent":' + str(safe_number(nums[4]))
        data += ',"AcOutputApparentPower":' + str(safe_number(nums[5]))
        data += ',"AcOutputActivePower":' + str(safe_number(nums[6]))
        data += ',"BatteryVoltage":' + str(safe_number(nums[7]))
        data += ',"BatteryRechargeVoltage":' + str(safe_number(nums[8]))
        data += ',"BatteryUnderVoltage":' + str(safe_number(nums[9]))
        data += ',"BatteryBulkVoltage":' + str(safe_number(nums[10]))
        data += ',"BatteryFloatVoltage":' + str(safe_number(nums[11]))
        data += ',"BatteryType":"' + map_with_log(battery_types, nums[12], "BatteryType") + '"'
        data += ',"MaxAcChargingCurrent":' + str(safe_number(nums[13]))
        data += ',"MaxChargingCurrent":' + str(safe_number(nums[14]))
        data += ',"InputVoltageRange":"' + map_with_log(voltage_ranges, nums[15], "InputVoltageRange") + '"'
        data += ',"OutputSourcePriority":"' + map_with_log(output_sources, nums[16], "OutputSourcePriority") + '"'
        data += ',"ChargerSourcePriority":"' + map_with_log(charger_sources, nums[17], "ChargerSourcePriority") + '"'
        data += ',"MaxParallelUnits":' + str(safe_number(nums[18]))
        data += ',"MachineType":"' + map_with_log(machine_types, nums[19], "MachineType") + '"'
        data += ',"Topology":"' + map_with_log(topologies, nums[20], "Topology") + '"'
        data += ',"OutputMode":"' + map_with_log(output_modes, nums[21], "OutputMode") + '"'
        data += ',"BatteryRedischargeVoltage":' + str(safe_number(nums[22]))
        data += ',"PvOkCondition":"' + map_with_log(pv_ok_conditions, nums[23], "PvOkCondition") + '"'
        data += ',"PvPowerBalance":"' + map_with_log(pv_power_balance, nums[24], "PvPowerBalance") + '"'
        data += ',"MaxBatteryCvChargingTime":' + str(safe_number(nums[25])) + '}'
        logger.debug("[QPIRI] payload built")
        return data
    except Exception as e:
        log_every("qpiri-error", logging.ERROR, f"[QPIRI] error: {e}", every_s=20.0)
        return ''

# ---------- MAIN ----------
def main():
    # Arranque aleatorio (evitar tormenta)
    time.sleep(random.randint(0, 5))
    connect()

    try:
        raw_sn = serial_command('QID')
    except Exception:
        raw_sn = 'unknown'
    sn = sanitize_id(raw_sn.strip())
    logger.info(f"Reading from inverter {sn} (raw='{raw_sn}')")

    # Intervalos (segundos) con env y fallback
    try:
        FAST_INTERVAL    = int(os.environ.get("DATA_INTERVAL", 2))      # QPIGS / QPIGS2 / QPGS0
    except ValueError:
        FAST_INTERVAL    = 2
    try:
        HEALTH_INTERVAL  = int(os.environ.get("HEALTH_INTERVAL", 20))   # HealthCheck
    except ValueError:
        HEALTH_INTERVAL  = 20
    try:
        SLOW_INTERVAL    = int(os.environ.get("SETTINGS_INTERVAL", 600))  # QPIRI
    except ValueError:
        SLOW_INTERVAL    = 600

    logger.info(f"Intervals → FAST={FAST_INTERVAL}s, HEALTH={HEALTH_INTERVAL}s, SETTINGS={SLOW_INTERVAL}s")

    last_fast = last_health = last_slow = 0.0

    while True:
        nowm = time.monotonic()

        # FAST
        if nowm - last_fast >= FAST_INTERVAL:
            last_fast = nowm
            try:
                d = get_data()
                if d: send_data(d, os.environ['MQTT_TOPIC'].replace('{sn}', sn))
            except Exception as e:
                log_every("qpigs-cycle", logging.ERROR, f"[CYCLE] QPIGS error: {e}", 10.0)

            try:
                pv2 = get_qpigs2_json()
                if pv2: send_data(pv2, os.environ['MQTT_TOPIC'].replace('{sn}', sn + '_pv2'))
            except Exception as e:
                log_every("qpigs2-cycle", logging.ERROR, f"[CYCLE] QPIGS2 error: {e}", 10.0)

            try:
                d = get_parallel_data()
                if d: send_data(d, os.environ['MQTT_TOPIC_PARALLEL'])
            except Exception as e:
                log_every("qpgs0-cycle", logging.ERROR, f"[CYCLE] QPGS0 error: {e}", 10.0)

        # HEALTH
        if nowm - last_health >= HEALTH_INTERVAL:
            last_health = nowm
            try:
                d = get_healthcheck('true')
                if d: send_data(d, os.environ['MQTT_HEALTHCHECK'])
            except Exception as e:
                try:
                    d = get_healthcheck('false')
                    send_data(d, os.environ['MQTT_HEALTHCHECK'])
                except Exception:
                    pass
                log_every("health-cycle", logging.ERROR, f"[CYCLE] HealthCheck error: {e}", 20.0)

        # SETTINGS
        if nowm - last_slow >= SLOW_INTERVAL:
            last_slow = nowm
            try:
                d = get_settings()
                if d: send_data(d, os.environ['MQTT_TOPIC_SETTINGS'])
            except Exception as e:
                log_every("qpiri-cycle", logging.ERROR, f"[CYCLE] QPIRI error: {e}", 30.0)

        time.sleep(0.1)

if __name__ == '__main__':
    main()
