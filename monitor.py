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

# ----------------- Tablas de mapeo -----------------
battery_types = {'0': 'AGM', '1': 'Flooded', '2': 'User', '3': 'Lithium' }
voltage_ranges = {'0': 'Appliance', '1': 'UPS'}
output_sources = {'0': 'utility', '1': 'solar', '2': 'battery'}
charger_sources = {'0': 'utility first', '1': 'solar first', '2': 'solar + utility', '3': 'solar only'}
machine_types = {'00': 'Grid tie', '01': 'Off Grid', '10': 'Hybrid'}
topologies = {'0': 'transformerless', '1': 'transformer'}
output_modes = {'0': 'single machine output', '1': 'parallel output', '2': 'Phase 1 of 3 Phase output', '3': 'Phase 2 of 3 Phase output', '4': 'Phase 3 of 3 Phase output'}
pv_ok_conditions = {'0': 'As long as one unit of inverters has connect PV, parallel system will consider PV OK', '1': 'Only All of inverters have connect PV, parallel system will consider PV OK'}
pv_power_balance = {'0': 'PV input max current will be the max charged current', '1': 'PV input max power will be the sum of the max charged power and loads power'}

# ----------------- Helpers de depuración -----------------
def dump_tokens(tag: str, resp: str):
    arr = resp.split(' ')
    print(f"\n[{tag}] len={len(arr)} raw='{resp}'")
    for i, t in enumerate(arr):
        print(f"  {tag}[{i:02d}] = {t}")
    return arr

def pv_from_nums(nums):
    pv_i = safe_number(nums[12]) if len(nums) > 12 else 0.0
    pv_v = safe_number(nums[13]) if len(nums) > 13 else 0.0
    return pv_v, pv_i

# ----------------- MQTT -----------------
def connect():
    date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('\n\n\n['+date+'] - [monitor.py] - [ MQTT Connect ]: INIT')
    global client
    client = mqtt.Client(client_id=os.environ['MQTT_CLIENT_ID'])
    client.username_pw_set(os.environ['MQTT_USER'], os.environ['MQTT_PASS'])
    client.connect(os.environ['MQTT_SERVER'])
    print(os.environ['DEVICE'])

# ----------------- Serie / HID -----------------
def _open_device():
    file = open(os.environ['DEVICE'], 'r+')
    fd = file.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    return file, fd

def _encode_command(command: str) -> bytes:
    xmodem_crc_func = crcmod.predefined.mkCrcFun('xmodem')
    command_bytes = command.encode('utf-8')
    command_crc_hex = hex(xmodem_crc_func(command_bytes)).replace('0x', '')
    return command_bytes + unhexlify(command_crc_hex.encode('utf-8')) + b'\x0d'

def _trim_paren_payload(decoded: str) -> str:
    # Intenta devolver lo que hay entre '(' y '\r', menos los 2 bytes de CRC antes del \r
    try:
        i0 = decoded.find('(')
        i1 = decoded.find('\r')
        if i0 >= 0 and i1 > i0:
            inner = decoded[i0+1:i1]
            # quitar últimos 2 chars (CRC) si parecen hex imprimible
            if len(inner) >= 2:
                return inner[:-2]
            return inner
    except Exception:
        pass
    return decoded.strip()

def serial_command(command):
    """Comportamiento original (con reintento y reconexión)."""
    print(command)
    try:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n\n\n['+date+'] - [monitor.py] - [ serial_command ]: INIT')
        cmd_bytes = _encode_command(command)

        try:
            file, fd = _open_device()
        except Exception as e:
            print('error open file descriptor: ' + str(e))
            exit()

        os.write(fd, cmd_bytes)

        response = b''
        timeout_counter = 0
        while b'\r' not in response:
            if timeout_counter > 500:
                raise Exception('Read operation timed out')
            timeout_counter += 1
            try:
                response += os.read(fd, 100)
            except Exception:
                time.sleep(0.01)
            if len(response) > 0 and (response[0] != ord('(') or b'NAKss' in response):
                raise Exception('NAKss')

        try:
            decoded = response.decode('utf-8')
        except UnicodeDecodeError:
            decoded = response.decode('iso-8859-1')

        print(decoded)
        print('\n\n\n['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [monitor.py] - [ serial_command ]: END \n\n')

        trimmed = _trim_paren_payload(decoded)
        file.close()
        return trimmed
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('['+date+'] - [monitor.py] - [ serial_command ] - Error reading inverter...: ' + str(e))
        try:
            file.close()
        except Exception:
            pass
        time.sleep(0.1)
        connect()
        return serial_command(command)

def serial_try(command, timeout_ms=1500):
    """
    Versión NO recursiva, para discovery.
    Devuelve dict: {ok, raw, trimmed, error}
    """
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
                    time.sleep(0.01)
                    waited += 10
            except Exception:
                time.sleep(0.01)
                waited += 10

            if len(response) > 0 and (response[0] != ord('(') or b'NAKss' in response):
                raise Exception('NAK/invalid')

        try:
            file.close()
        except Exception:
            pass

        if b'\r' not in response:
            out["error"] = "timeout"
            return out

        try:
            decoded = response.decode('utf-8')
        except UnicodeDecodeError:
            decoded = response.decode('iso-8859-1')

        out["raw"] = decoded
        out["trimmed"] = _trim_paren_payload(decoded)
        out["ok"] = True
        return out
    except Exception as e:
        out["error"] = str(e)
        return out

# ----------------- Parsers / Publicación -----------------
def get_parallel_data():
    try:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n\n\n['+date+'] - [monitor.py] - [ get_parallel_dat ]: INIT Serial Comand: QPGS0')
        data = '{'
        response = serial_command('QPGS0')
        nums = response.split(' ')
        if len(nums) < 27:
            return ''
        if nums[2] == 'L':
            data += '"Gridmode":1'
        else:
            data += '"Gridmode":0'
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
        if nums[2] == 'B':
            data += ',"Solarmode":1'
        else:
            data += ',"Solarmode":0'
        data += '}'
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('['+date+'] - [monitor.py] - [ get_parallel_data ] - Error parsing inverter data...: ' + str(e))
        try:
            print(response)
        except Exception:
            pass
        return ''
    print('\n\n\n['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [monitor.py] - [ get_parallel_dat ]: END \n\n')
    return data

def get_data():
    try:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n\n\n['+date+'] - [monitor.py] - [get_data]: INIT Serial Command: QPIGS')
        response = serial_command('QPIGS')
        nums = response.split(' ')
        dump_tokens('QPIGS', response)

        if len(nums) < 21:
            return ''

        pv2_v = 0.0
        pv2_i = 0.0
        try:
            resp2 = serial_command('QPIGS2')
            nums2 = resp2.split(' ')
            dump_tokens('QPIGS2', resp2)
            if len(nums2) >= 15:
                pv2_v, pv2_i = pv_from_nums(nums2)
        except Exception as e:
            print("[get_data] QPIGS2 no disponible:", e)

        pv1_v, pv1_i = pv_from_nums(nums)

        data = '{'
        data += '"BusVoltage":' + str(safe_number(nums[7]))
        data += ',"InverterHeatsinkTemperature":' + str(safe_number(nums[11]))
        data += ',"BatteryVoltageFromScc":' + str(safe_number(nums[14]))
        data += ',"PvInputCurrent":' + str(safe_number(nums[12]))
        data += ',"PvInputVoltage":' + str(safe_number(nums[13]))
        data += ',"PvInputPower":' + str(safe_number(nums[19]))
        data += ',"BatteryChargingCurrent": ' + str(safe_number(nums[9]))
        data += ',"BatteryDischargeCurrent":' + str(safe_number(nums[15]))
        data += ',"DeviceStatus":"' + nums[16] + '"'
        data += ',"Pv1InputVoltage":' + str(round(pv1_v, 2))
        data += ',"Pv1InputCurrent":' + str(round(pv1_i, 2))
        data += ',"Pv2InputVoltage":' + str(round(pv2_v, 2))
        data += ',"Pv2InputCurrent":' + str(round(pv2_i, 2))
        data += '}'
        print('\n\n\n['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [monitor.py] - [ get_data ]: END \n\n')
        return data
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('['+date+'] - [monitor.py] - [ get_data ] - Error parsing inverter data...: ' + str(e))
        return ''

def get_settings():
    try:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n\n\n['+date+'] - [monitor.py] - [get_settings]: INIT Serial Command: QPIRI')
        response = serial_command('QPIRI')
        nums = response.split(' ')
        if len(nums) < 21:
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
        data += ',"MaxBatteryCvChargingTime":' + str(safe_number(nums[25]))
        data += '}'

        print('\n\n\n['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [monitor.py] - [ get_settings ]: END \n\n')
        return data
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('['+date+'] - [monitor.py] - [ get_settings ] - Error parsing inverter data...: ' + str(e))
        return ''

def map_with_log(table: dict, value: str, label: str) -> str:
    if value in table:
        return table[value]
    else:
        print(f"[get_settings] Valor inesperado en {label}: {value} (claves válidas: {list(table.keys())})")
        return f"{label}_invalid({value})"

def send_data(data, topic):
    try:
        client.publish(topic, data, 0, True)
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n\n\n['+date+'] - [monitor.py] - [ send_data ] - Error sending to MQTT...: ' + str(e))
        return 0
    return 1

# ----------------- Discovery seguro -----------------
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
    # Extras conocidos (pueden no existir pero son de consulta)
    "QET", "QED", "QPIHF", "QBV", "QOPM", "QBEQI", "QBEQO", "QCHGCR", "QMCHGCR"
]

# Algunos equipos PI30 aceptan más, pero mantenemos solo "Q*"
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
    send_data(json.dumps(payload, ensure_ascii=False), topic)

def discover_commands(serial_number, protocol):
    print(f"[discovery] Start | serial={serial_number} protocol={protocol}")
    # Permitir override por env
    env_list = os.environ.get("MONITOR_SAFE_COMMANDS")
    if env_list:
        candidates = [c.strip().upper() for c in env_list.split(",") if c.strip()]
    else:
        candidates = SAFE_QUERY_PI41 if "PI41" in protocol else SAFE_QUERY_PI30

    # Quitar duplicados preservando orden
    seen = set()
    uniq = []
    for c in candidates:
        if c not in seen and c.startswith('Q'):
            seen.add(c)
            uniq.append(c)

    # Probar cada comando con un timeout corto
    for cmd in uniq:
        res = serial_try(cmd, timeout_ms=1500)
        discovery_publish(cmd, res, serial_number, protocol)
        # Pequeña pausa para no saturar
        time.sleep(0.05)

    print("[discovery] Done.")

# ----------------- Conversión numérica robusta -----------------
def safe_number(value):
    s = str(value).strip().replace(',', '.')
    m = re.search(r'-?\d+(?:\.\d+)?', s)
    if not m:
        return 0
    s2 = m.group(0)
    try:
        return int(s2) if re.fullmatch(r'-?\d+', s2) else float(s2)
    except Exception:
        return 0

# ----------------- Main loop -----------------
def main():
    time.sleep(randint(0, 5))
    connect()

    # Identificar protocolo y serie con intentos "suaves"
    proto_try = serial_try('QPI', timeout_ms=800)
    protocol = proto_try["trimmed"] if proto_try["ok"] else "UNKNOWN"
    if protocol and not protocol.startswith('PI'):
        protocol = f"({protocol})"

    sn_try = serial_try('QID', timeout_ms=800)
    serial_number = sn_try["trimmed"] if sn_try["ok"] and sn_try["trimmed"] else "UNKNOWN_SN"
    print('Reading from inverter ' + serial_number + f' protocol {protocol}')

    # Discovery en arranque
    try:
        discover_commands(serial_number, protocol if protocol else "UNKNOWN")
    except Exception as e:
        print(f"[discovery] Error: {e}")

    # Opción para solo discovery y salir
    if os.environ.get("DISCOVERY_ONLY", "0") == "1":
        print("[main] DISCOVERY_ONLY=1 -> exit.")
        return

    # Bucle normal
    while True:
        try:
            data = get_parallel_data()
            if data != '':
                send_data(data, os.environ['MQTT_TOPIC_PARALLEL'])
            time.sleep(1)

            data = get_data()
            if data != '':
                send_data(data, os.environ['MQTT_TOPIC'].replace('{sn}', serial_number))
            time.sleep(1)

            data = get_settings()
            if data != '':
                send_data(data, os.environ['MQTT_TOPIC_SETTINGS'])
            time.sleep(4)
        except Exception as e:
            print("Error occurred:", e)
            time.sleep(10)

if __name__ == '__main__':
    main()
