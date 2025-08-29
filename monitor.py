#! /usr/bin/python
# Axpert Inverter control script (safe discovery + MQTT export)
# Non-destructive queries only (commands starting with 'Q')

import time, sys, string
import sqlite3
import json
import datetime
import calendar
import os
import fcntl
import re
import unicodedata
import crcmod.predefined
from datetime import datetime
from binascii import unhexlify
import paho.mqtt.client as mqtt
from random import randint

# ================= Mapeos =================
battery_types = {'0': 'AGM', '1': 'Flooded', '2': 'User', '3': 'Lithium' }
voltage_ranges = {'0': 'Appliance', '1': 'UPS'}
output_sources = {'0': 'utility', '1': 'solar', '2': 'battery'}
charger_sources = {'0': 'utility first', '1': 'solar first', '2': 'solar + utility', '3': 'solar only'}
machine_types = {'00': 'Grid tie', '01': 'Off Grid', '10': 'Hybrid'}
topologies = {'0': 'transformerless', '1': 'transformer'}
output_modes = {'0': 'single machine output', '1': 'parallel output', '2': 'Phase 1 of 3 Phase output', '3': 'Phase 2 of 3 Phase output', '4': 'Phase 3 of 3 Phase output'}
pv_ok_conditions = {'0': 'As long as one unit of inverters has connect PV, parallel system will consider PV OK', '1': 'Only All of inverters have connect PV, parallel system will consider PV OK'}
pv_power_balance = {'0': 'PV input max current will be the max charged current', '1': 'PV input max power will be the sum of the max charged power and loads power'}

# ================ Globals =================
SUPPORTED = set()   # comandos que respondieron OK en discovery

# ============== Utilidades ===============

def safe_number(value):
    """Convierte a número limpiando basura. Devuelve float/int; si no hay nada numérico, 0."""
    s = str(value).strip().replace(',', '.')
    m = re.search(r'-?\d+(?:\.\d+)?', s)
    if not m:
        return 0
    s2 = m.group(0)
    try:
        return int(s2) if re.fullmatch(r'-?\d+', s2) else float(s2)
    except Exception:
        return 0

# ------------- MQTT -------------

def connect():
    date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('\n\n\n['+date+'] - [monitor.py] - [ MQTT Connect ]: INIT')
    global client
    client = mqtt.Client(client_id=os.environ['MQTT_CLIENT_ID'])
    client.username_pw_set(os.environ['MQTT_USER'], os.environ['MQTT_PASS'])
    client.connect(os.environ['MQTT_SERVER'])
    print(os.environ['DEVICE'])

def send_data(data_str, topic):
    try:
        client.publish(topic, data_str, 0, True)
        return 1
    except Exception as e:
        print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [ send_data ] - MQTT error: ' + str(e))
        return 0

def publish_json(topic, payload: dict):
    return send_data(json.dumps(payload, ensure_ascii=False), topic)

# ------------- HID/Serie -------------

def _open_device():
    file = open(os.environ['DEVICE'], 'r+')
    fd = file.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    return file, fd

def _encode_command(command: str) -> bytes:
    # XMODEM CRC 16-bit, SIEMPRE 4 dígitos hex (arregla "odd length string")
    xmodem_crc_func = crcmod.predefined.mkCrcFun('xmodem')
    command_bytes = command.encode('ascii')
    crc = xmodem_crc_func(command_bytes) & 0xFFFF
    command_crc_hex = f'{crc:04x}'  # <-- clave
    return command_bytes + unhexlify(command_crc_hex) + b'\r'

def _trim_paren_payload(decoded: str) -> str:
    # Devuelve lo que hay entre '(' y '\r'; quita 2 chars finales (CRC en ASCII-decoded)
    try:
        i0 = decoded.find('(')
        i1 = decoded.find('\r')
        if i0 >= 0 and i1 > i0:
            inner = decoded[i0+1:i1]
            return inner[:-2] if len(inner) >= 2 else inner
    except Exception:
        pass
    return decoded.strip()

def serial_command(command):
    print(command)
    try:
        print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [ serial_command ]: INIT')
        cmd_bytes = _encode_command(command)
        try:
            file, fd = _open_device()
        except Exception as e:
            print('error open file descriptor: ' + str(e))
            sys.exit(1)
        os.write(fd, cmd_bytes)
        response = b''
        timeout_counter = 0
        while b'\r' not in response:
            if timeout_counter > 500:
                raise Exception('Read operation timed out')
            timeout_counter += 1
            try:
                response += os.read(fd, 200)
            except Exception:
                time.sleep(0.01)
            if len(response) > 0 and (response[0] != ord('(') or b'NAKss' in response):
                raise Exception('NAKss')
        try:
            decoded = response.decode('utf-8')
        except UnicodeDecodeError:
            decoded = response.decode('iso-8859-1')
        print(decoded)
        print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [ serial_command ]: END')
        trimmed = _trim_paren_payload(decoded)
        file.close()
        return trimmed
    except Exception as e:
        print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [ serial_command ] - Error: ' + str(e))
        try:
            file.close()
        except Exception:
            pass
        time.sleep(0.1)
        connect()
        return serial_command(command)

def serial_try(command, timeout_ms=1500):
    out = {"ok": False, "raw": None, "trimmed": None, "error": None}
    try:
        cmd_bytes = _encode_command(command)
        file, fd = _open_device()
        os.write(fd, cmd_bytes)
        response = b''
        waited = 0
        while b'\r' not in response and waited < timeout_ms:
            try:
                chunk = os.read(fd, 200)
                if chunk:
                    response += chunk
                else:
                    time.sleep(0.01); waited += 10
            except Exception:
                time.sleep(0.01); waited += 10
            if len(response) > 0 and (response[0] != ord('(') or b'NAKss' in response):
                raise Exception('NAK/invalid')
        try:
            file.close()
        except Exception:
            pass
        if b'\r' not in response:
            out["error"] = "timeout"; return out
        try:
            decoded = response.decode('utf-8')
        except UnicodeDecodeError:
            decoded = response.decode('iso-8859-1')
        out["raw"] = decoded
        out["trimmed"] = _trim_paren_payload(decoded)
        out["ok"] = True
        return out
    except Exception as e:
        out["error"] = str(e); return out

# ------------- Parsers -------------

def map_with_log(table: dict, value: str, label: str) -> str:
    if value in table:
        return table[value]
    print(f"[get_settings] Valor inesperado en {label}: {value} (claves válidas: {list(table.keys())})")
    return f"{label}_invalid({value})"

def qpigs_to_dict(nums):
    d = {}
    # índices clásicos PI30
    if len(nums) >= 17:
        d.update({
            "GridVoltage":        safe_number(nums[0]),
            "GridFrequency":      safe_number(nums[1]),
            "OutputVoltage":      safe_number(nums[2]),
            "OutputFrequency":    safe_number(nums[3]),
            "OutputApparentPower":safe_number(nums[4]),
            "OutputActivePower":  safe_number(nums[5]),
            "LoadPercentage":     safe_number(nums[6]),
            "BusVoltage":         safe_number(nums[7]),
            "BatteryVoltage":     safe_number(nums[8]),
            "BatteryChargingCurrent": safe_number(nums[9]),
            "BatteryCapacity":    safe_number(nums[10]),
            "InverterHeatsinkTemperature": safe_number(nums[11]),
            "PvInputCurrent":     safe_number(nums[12]),   # PV1 I
            "PvInputVoltage":     safe_number(nums[13]),   # PV1 V
            "BatteryVoltageFromScc": safe_number(nums[14]),
            "BatteryDischargeCurrent": safe_number(nums[15]),
            "DeviceStatus":       str(nums[16])
        })
    # Campos extra frecuentes (observados en tus dumps)
    if len(nums) > 19:
        pv_power = safe_number(nums[19])
        if pv_power == 0:
            # calcular si falta
            pv_power = round(d.get("PvInputVoltage",0)*d.get("PvInputCurrent",0), 1)
        d["PvInputPower"] = pv_power
    return d

# ------------- Publicación parsers -------------

def get_parallel_data():
    try:
        print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [ get_parallel_data ]: QPGS0')
        resp = serial_command('QPGS0')
        n = resp.split(' ')
        if len(n) < 27:
            return None
        data = {
            "Gridmode": 1 if n[2] == 'L' else 0,
            "SerialNumber": safe_number(n[1]),
            "BatteryChargingCurrent": safe_number(n[12]),
            "BatteryDischargeCurrent": safe_number(n[26]),
            "TotalChargingCurrent": safe_number(n[15]),
            "GridVoltage": safe_number(n[4]),
            "GridFrequency": safe_number(n[5]),
            "OutputVoltage": safe_number(n[6]),
            "OutputFrequency": safe_number(n[7]),
            "OutputAparentPower": safe_number(n[8]),
            "OutputActivePower": safe_number(n[9]),
            "LoadPercentage": safe_number(n[10]),
            "BatteryVoltage": safe_number(n[11]),
            "BatteryCapacity": safe_number(n[13]),
            "PvInputVoltage": safe_number(n[14]),
            "TotalAcOutputApparentPower": safe_number(n[16]),
            "TotalAcOutputActivePower": safe_number(n[17]),
            "TotalAcOutputPercentage": safe_number(n[18]),
            "OutputMode": safe_number(n[20]),
            "ChargerSourcePriority": safe_number(n[21]),
            "MaxChargeCurrent": safe_number(n[22]),
            "MaxChargerRange": safe_number(n[23]),
            "MaxAcChargerCurrent": safe_number(n[24]),
            "PvInputCurrentForBattery": safe_number(n[25]),
            "Solarmode": 1 if n[2] == 'B' else 0
        }
        return data
    except Exception as e:
        print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [ get_parallel_data ] - Error: ' + str(e))
        return None


def get_data(serial_number):
    try:
        print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [ get_data ]: QPIGS')
        resp = serial_command('QPIGS')
        nums = [t for t in resp.split(' ') if t!='']
        d = qpigs_to_dict(nums)
        # Intento PV2 SOLO si el discovery lo marcó como soportado
        if 'QPIGS2' in SUPPORTED:
            try:
                resp2 = serial_command('QPIGS2')
                n2 = [t for t in resp2.split(' ') if t!='']
                if len(n2) >= 15:
                    pv2_v = safe_number(n2[13])
                    pv2_i = safe_number(n2[12])
                    d.update({
                        "Pv2InputVoltage": pv2_v,
                        "Pv2InputCurrent": pv2_i
                    })
            except Exception as e:
                print('[get_data] QPIGS2 error:', e)
        # Derivados
        if 'Pv2InputVoltage' not in d:
            d["Pv2InputVoltage"] = 0.0
            d["Pv2InputCurrent"] = 0.0
        return d
    except Exception as e:
        print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [ get_data ] - Error: ' + str(e))
        return None


def get_settings():
    try:
        print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [ get_settings ]: QPIRI')
        resp = serial_command('QPIRI')
        n = [t for t in resp.split(' ') if t!='']
        if len(n) < 26:
            return None
        data = {
            "AcInputVoltage": safe_number(n[0]),
            "AcInputCurrent": safe_number(n[1]),
            "AcOutputVoltage": safe_number(n[2]),
            "AcOutputFrequency": safe_number(n[3]),
            "AcOutputCurrent": safe_number(n[4]),
            "AcOutputApparentPower": safe_number(n[5]),
            "AcOutputActivePower": safe_number(n[6]),
            "BatteryVoltage": safe_number(n[7]),
            "BatteryRechargeVoltage": safe_number(n[8]),
            "BatteryUnderVoltage": safe_number(n[9]),
            "BatteryBulkVoltage": safe_number(n[10]),
            "BatteryFloatVoltage": safe_number(n[11]),
            "BatteryType": map_with_log(battery_types, n[12], "BatteryType"),
            "MaxAcChargingCurrent": safe_number(n[13]),
            "MaxChargingCurrent": safe_number(n[14]),
            "InputVoltageRange": map_with_log(voltage_ranges, n[15], "InputVoltageRange"),
            "OutputSourcePriority": map_with_log(output_sources, n[16], "OutputSourcePriority"),
            "ChargerSourcePriority": map_with_log(charger_sources, n[17], "ChargerSourcePriority"),
            "MaxParallelUnits": safe_number(n[18]),
            "MachineType": map_with_log(machine_types, n[19], "MachineType"),
            "Topology": map_with_log(topologies, n[20], "Topology"),
            "OutputMode": map_with_log(output_modes, n[21], "OutputMode"),
            "BatteryRedischargeVoltage": safe_number(n[22]),
            "PvOkCondition": map_with_log(pv_ok_conditions, n[23], "PvOkCondition"),
            "PvPowerBalance": map_with_log(pv_power_balance, n[24], "PvPowerBalance"),
            "MaxBatteryCvChargingTime": safe_number(n[25])
        }
        return data
    except Exception as e:
        print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [ get_settings ] - Error: ' + str(e))
        return None

# ------------- Discovery -------------

SAFE_QUERY_COMMON = [
    # Identidad / protocolo
    "QPI", "QID", "QMN", "QGMN", "QMD",
    # Firmware
    "QVFW", "QVFW2",
    # Estado general
    "QPIGS", "QPIGS2",
    # Rated info / settings
    "QPIRI",
    # Alarmas / flags / modo
    "QPIWS", "QFLAG", "QMOD",
    # Paralelo (hasta 9 por si acaso)
    "QPGS0","QPGS1","QPGS2","QPGS3","QPGS4","QPGS5","QPGS6","QPGS7","QPGS8",
    # Extras consulta
    "QET", "QED", "QPIHF", "QBV", "QOPM", "QBEQI", "QBEQO", "QCHGCR", "QMCHGCR"
]
SAFE_QUERY_PI30 = SAFE_QUERY_COMMON
SAFE_QUERY_PI41 = SAFE_QUERY_COMMON


def parse_tokens(trimmed: str):
    tokens = [t for t in trimmed.strip().split(' ') if t != '']
    return tokens, len(tokens)


def discovery_publish(cmd, result_dict, serial_number, protocol):
    topic_tpl = os.environ.get('MQTT_TOPIC_DISCOVERY', 'axpert/discovery/{sn}/{cmd}')
    topic = topic_tpl.replace('{sn}', serial_number).replace('{cmd}', cmd)
    payload = {
        "timestamp": datetime.now().isoformat(),
        "serial": serial_number,
        "protocol": protocol,
        "command": cmd,
        "ok": result_dict.get("ok", False),
        "raw": result_dict.get("raw"),
        "trimmed": result_dict.get("trimmed"),
        "error": result_dict.get("error")
    }
    if result_dict.get("ok") and result_dict.get("trimmed"):
        tokens, n = parse_tokens(result_dict["trimmed"])
        payload["tokens"] = tokens
        payload["len"] = n
    publish_json(topic, payload)


def discover_commands(serial_number, protocol):
    print(f"[discovery] Start | serial={serial_number} protocol={protocol}")
    env_list = os.environ.get("MONITOR_SAFE_COMMANDS")
    if env_list:
        candidates = [c.strip().upper() for c in env_list.split(",") if c.strip()]
    else:
        candidates = SAFE_QUERY_PI41 if "PI41" in protocol else SAFE_QUERY_PI30
    seen = set(); uniq = []
    for c in candidates:
        if c not in seen and c.startswith('Q'):
            seen.add(c); uniq.append(c)
    for cmd in uniq:
        res = serial_try(cmd, timeout_ms=1500)
        if res.get("ok"):
            SUPPORTED.add(cmd)
        discovery_publish(cmd, res, serial_number, protocol)
        time.sleep(0.05)
    print("[discovery] Done.")

# ------------- Main -------------

def main():
    time.sleep(randint(0, 5))
    connect()

    # Protocolo y SN
    proto_try = serial_try('QPI', timeout_ms=800)
    protocol = proto_try["trimmed"] if proto_try["ok"] else "UNKNOWN"
    if protocol and not protocol.startswith('PI'):
        protocol = f"({protocol})"
    sn_try = serial_try('QID', timeout_ms=800)
    serial_number = sn_try["trimmed"] if sn_try["ok"] and sn_try["trimmed"] else "UNKNOWN_SN"
    print('Reading from inverter ' + serial_number + f' protocol {protocol}')

    # Discovery
    try:
        discover_commands(serial_number, protocol if protocol else "UNKNOWN")
    except Exception as e:
        print(f"[discovery] Error: {e}")

    if os.environ.get("DISCOVERY_ONLY", "0") == "1":
        print("[main] DISCOVERY_ONLY=1 -> exit.")
        return

    # Tópicos
    topic_status_tpl    = os.environ.get('MQTT_TOPIC', 'axpert/status/{sn}')
    topic_settings      = os.environ.get('MQTT_TOPIC_SETTINGS', 'axpert/settings/{sn}').replace('{sn}', serial_number)
    topic_parallel      = os.environ.get('MQTT_TOPIC_PARALLEL', 'axpert/parallel/{sn}').replace('{sn}', serial_number)
    topic_unit_tpl      = os.environ.get('MQTT_TOPIC_UNIT', 'axpert/status/{sn}/unit/{n}')

    # Unidades QPIGS<n> opcionales (e.g. "1,3,4")
    units_env = os.environ.get('MONITOR_QPIGS_UNITS', '')
    units = []
    if units_env.strip():
        for p in units_env.split(','):
            p = p.strip()
            if p.isdigit():
                units.append(int(p))

    # Bucle
    while True:
        try:
            pdata = get_parallel_data()
            if pdata:
                publish_json(topic_parallel, pdata)
            time.sleep(1)

            d = get_data(serial_number)
            if d:
                publish_json(topic_status_tpl.replace('{sn}', serial_number), d)
            time.sleep(1)

            s = get_settings()
            if s:
                publish_json(topic_settings, s)
            time.sleep(2)

            # QPIGS<n> por unidad si están soportados
            for n in units:
                cmd = f'QPIGS{n}'
                if cmd in SUPPORTED:
                    try:
                        respn = serial_command(cmd)
                        numsn = [t for t in respn.split(' ') if t!='']
                        dj = qpigs_to_dict(numsn)
                        publish_json(topic_unit_tpl.replace('{sn}', serial_number).replace('{n}', str(n)), dj)
                        time.sleep(0.05)
                    except Exception as e:
                        print(f"[QPIGS{n}] error: {e}")
            time.sleep(1)
        except Exception as e:
            print("[main] loop error:", e)
            time.sleep(5)

if __name__ == '__main__':
    main()
