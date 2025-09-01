#! /usr/bin/python
# -*- coding: utf-8 -*-

# Axpert / Voltronic control script (HID-safe; QPIGS2 PV2 support)
# - CRC XMODEM en binario (2 bytes big-endian) + '\r'
# - Envío HID con fallback a chunking de 8 bytes
# - Sin recursiones en serial_command (reintentos controlados)
# - QPIGS2: lee PV2 (I,V,P) si el firmware lo expone

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
import paho.mqtt.client as mqtt
from random import randint

# --- NUEVO ---
import struct
import errno

battery_types = {'0': 'AGM', '1': 'Flooded', '2': 'User', '3': 'Lithium' }
voltage_ranges = {'0': 'Appliance', '1': 'UPS'}
output_sources = {'0': 'utility', '1': 'solar', '2': 'battery'}
charger_sources = {'0': 'utility first', '1': 'solar first', '2': 'solar + utility', '3': 'solar only'}
machine_types = {'00': 'Grid tie', '01': 'Off Grid', '10': 'Hybrid'}
topologies = {'0': 'transformerless', '1': 'transformer'}
output_modes = {'0': 'single machine output', '1': 'parallel output', '2': 'Phase 1 of 3 Phase output', '3': 'Phase 2 of 3 Phase output', '4': 'Phase 3 of 3 Phase output'}
pv_ok_conditions = {'0': 'As long as one unit of inverters has connect PV, parallel system will consider PV OK', '1': 'Only All of inverters have connect PV, parallel system will consider PV OK'}
pv_power_balance = {'0': 'PV input max current will be the max charged current', '1': 'PV input max power will be the sum of the max charged power and loads power'}

def connect():
    date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('\n\n\n['+date+'] - [monitor.py] - [ MQTT Connect ]: INIT')
    global client
    client = mqtt.Client(client_id=os.environ['MQTT_CLIENT_ID'])
    client.username_pw_set(os.environ['MQTT_USER'], os.environ['MQTT_PASS'])
    client.connect(os.environ['MQTT_SERVER'])
    print(os.environ['DEVICE'])

# --- REEMPLAZA POR COMPLETO: versión robusta HID ---
def serial_command(command):
    # Envío robusto para HID: CRC binario + \r, con chunking de 8 bytes y sin recursión
    MAX_TRIES = 3
    DEVICE = os.environ['DEVICE']

    def build_frame(cmd: str) -> bytes:
        xmodem_crc_func = crcmod.predefined.mkCrcFun('xmodem')
        cmd_b = cmd.encode('ascii')
        crc = xmodem_crc_func(cmd_b)
        crc_b = struct.pack('>H', crc)     # 2 bytes big-endian
        return cmd_b + crc_b + b'\x0d'     # frame completo

    def write_chunks(fd: int, buf: bytes, chunk: int = 8):
        off = 0
        n = len(buf)
        while off < n:
            end = min(off + chunk, n)
            try:
                written = os.write(fd, buf[off:end])
                if written <= 0:
                    raise OSError(errno.EIO, "short write")
                off += written
            except OSError as e:
                if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    time.sleep(0.01)
                    continue
                raise

    frame = build_frame(command)

    for attempt in range(1, MAX_TRIES + 1):
        print(command)
        print(f"\n\n\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - [monitor.py] - [ serial_command ]: INIT (try {attempt})")
        fd = None
        try:
            # abrir en binario (no 'r+'), no bloqueante
            fd = os.open(DEVICE, os.O_RDWR | os.O_NONBLOCK)

            # intento 1: todo de golpe; si falla, fallback chunking 8B
            try:
                os.write(fd, frame)
            except OSError:
                write_chunks(fd, frame, chunk=8)

            # leer hasta '\r'
            response = b''
            timeout_counter = 0
            while b'\r' not in response:
                if timeout_counter > 500:  # ~5s
                    raise TimeoutError("Read operation timed out")
                timeout_counter += 1
                try:
                    chunk = os.read(fd, 128)
                    if chunk:
                        response += chunk
                    else:
                        time.sleep(0.01)
                except OSError as e:
                    if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                        time.sleep(0.01)
                        continue
                    raise

            try:
                resp_str = response.decode('utf-8')
            except UnicodeDecodeError:
                resp_str = response.decode('iso-8859-1')

            print(resp_str)
            print(f"\n\n\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - [monitor.py] - [ serial_command ]: END \n\n")

            # recorte robusto: entre '(' y '\r'
            start = resp_str.find('(')
            end = resp_str.find('\r')
            payload = resp_str[start+1:end] if (start != -1 and end != -1 and end > start) else resp_str.strip()

            os.close(fd)
            return payload

        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] - [monitor.py] - [ serial_command ] - Error: {e}")
            if fd is not None:
                try:
                    os.close(fd)
                except:
                    pass
            time.sleep(0.1)
            if attempt == MAX_TRIES:
                # dejamos que el caller gestione el fallo (el main ya captura)
                raise
            # reintento: refrescamos MQTT por si acaso
            try:
                connect()
            except Exception as ee:
                print(f"[monitor.py] [serial_command] MQTT reconnect error (ignored): {ee}")

def get_parallel_data():
    # QPGS0
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
        print('\n ** Response **')
        try:
            print(response)
        except:
            pass
        print('\n ** END Response **')
        return ''

    print('\n\n\n['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [monitor.py] - [ get_parallel_dat ]: END \n\n')
    return data

def get_data():
    # QPIGS (normalmente PV1)
    try:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n\n\n['+date+'] - [monitor.py] - [get_data]: INIT Serial Command: QPIGS')
        response = serial_command('QPIGS')
        nums = response.split(' ')
        if len(nums) < 21:
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
        data += ',"DeviceStatus":"' + nums[16] + '"'
        data += '}'
        print('\n\n\n['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [monitor.py] - [ get_data ]: END \n\n')
        return data
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('['+date+'] - [monitor.py] - [ get_data ] - Error parsing inverter data...: ' + str(e))
        return ''

# --- NUEVO: lector QPIGS2 (PV2 crudo) ---
def get_qpigs2():
    """ Devuelve string crudo de QPIGS2 y, si detecta 3 campos, los loguea como PV2 I,V,P. """
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f'\n\n\n[{ts}] - [monitor.py] - [get_qpigs2]: INIT Serial Command: QPIGS2')
        response = serial_command('QPIGS2')  # muchos firmwares MAX: "II.II VV.VV PPPPP"
        parts = response.split()
        if len(parts) >= 3:
            try:
                pv2_i = float(parts[0]); pv2_v = float(parts[1]); pv2_p = float(parts[2])
                print(f"[QPIGS2] PV2 Current(A)={pv2_i}, PV2 Voltage(V)={pv2_v}, PV2 Power(W)={pv2_p}")
            except:
                pass
        print(f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}] - [monitor.py] - [get_qpigs2]: END')
        return response  # crudo (por si quieres publicarlo/verlo)
    except Exception as e:
        print('['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [monitor.py] - [ get_qpigs2 ] - Error...: ' + str(e))
        return ''

def get_settings():
    # QPIRI
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

def safe_number(value):
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value

def main():
    time.sleep(randint(0, 5))  # desincroniza inicios
    connect()

    serial_number = serial_command('QID')
    print('Reading from inverter ' + serial_number)

    while True:
        try:
            # QPGS0
            data = get_parallel_data()
            if data != '':
                send_data(data, os.environ['MQTT_TOPIC_PARALLEL'])
            time.sleep(1)

            # QPIGS (PV1)
            data = get_data()
            if data != '':
                send_data(data, os.environ['MQTT_TOPIC'].replace('{sn}', serial_number))
            time.sleep(1)

            # QPIGS2 (PV2) - publicamos crudo para verlo rápido
            qpigs2_raw = get_qpigs2()
            if qpigs2_raw != '':
                # Lo publico en un topic alterno para no pisar el principal
                send_data(qpigs2_raw, os.environ['MQTT_TOPIC'].replace('{sn}', serial_number + '_pv2_raw'))
            time.sleep(1)

            # QPIRI (settings)
            data = get_settings()
            if data != '':
                send_data(data, os.environ['MQTT_TOPIC_SETTINGS'])
            time.sleep(4)

        except Exception as e:
            print("Error occurred:", e)
            time.sleep(10)

if __name__ == '__main__':
    main()
