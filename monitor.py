#! /usr/bin/python
# -*- coding: utf-8 -*-

# Axpert / Voltronic control script
# - Lee valores del inversor
# - Publica en MQTT
# - CRC XMODEM en binario (2B big-endian)
# - Envío HID/USB en dos writes (frame y luego <CR>) para evitar NAK en QPIGS2

import time, sys, os, fcntl, struct
import json
import crcmod.predefined
from datetime import datetime
import paho.mqtt.client as mqtt
from random import randint

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

def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(msg):
    print(f"[{now()}] {msg}")

def connect():
    log("[monitor.py] [MQTT] Connect: INIT")
    global client
    client = mqtt.Client(client_id=os.environ['MQTT_CLIENT_ID'])
    client.username_pw_set(os.environ['MQTT_USER'], os.environ['MQTT_PASS'])
    client.connect(os.environ['MQTT_SERVER'])
    print(os.environ['DEVICE'])

def _open_device():
    try:
        f = open(os.environ['DEVICE'], 'r+')
        fd = f.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        return f, fd
    except Exception as e:
        print('error open file descriptor: ' + str(e))
        sys.exit(1)

def serial_command(command):
    # Envía comando + CRC (2B BE) y luego <CR> en write separado (HID acepta ~8 bytes)
    print(command)
    try:
        log("[monitor.py] [serial_command] INIT")
        xmodem_crc_func = crcmod.predefined.mkCrcFun('xmodem')
        cmd_bytes = command.encode('ascii')
        crc = xmodem_crc_func(cmd_bytes)
        crc_bytes = struct.pack('>H', crc)  # 2 bytes big-endian

        frame_no_cr = cmd_bytes + crc_bytes
        cr_byte = b'\x0d'

        f, fd = _open_device()

        # write en 2 pasos para evitar NAK con QPIGS2 por límite de bytes en HID
        os.write(fd, frame_no_cr)
        time.sleep(0.01)
        os.write(fd, cr_byte)

        response = b''
        timeout_counter = 0
        # esperamos hasta \r
        while b'\r' not in response:
            if timeout_counter > 500:  # ~5s
                raise Exception('Read operation timed out')
            timeout_counter += 1
            try:
                chunk = os.read(fd, 128)
                if chunk:
                    response += chunk
                else:
                    time.sleep(0.01)
            except Exception:
                time.sleep(0.01)

        # NAK real típico: (NAKxxx\r
        if response.startswith(b'(NAK'):
            raise Exception('NAK')

        try:
            resp_str = response.decode('utf-8')
        except UnicodeDecodeError:
            resp_str = response.decode('iso-8859-1')

        print(resp_str)
        log("[monitor.py] [serial_command] END")

        # Recorte robusto: buscar '(' y '\r'
        start = resp_str.find('(')
        end = resp_str.find('\r')
        if start != -1 and end != -1 and end > start:
            payload = resp_str[start+1:end]
        else:
            payload = resp_str.strip()

        try:
            f.close()
        except:
            pass

        return payload

    except Exception as e:
        log(f"[monitor.py] [serial_command] Error reading inverter...: {e}")
        try:
            f.close()
        except:
            pass
        time.sleep(0.1)
        connect()
        return serial_command(command)

def get_parallel_data():
    # QPGS0
    try:
        log("[monitor.py] [get_parallel_data] INIT Serial Command: QPGS0")
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
        log(f"[monitor.py] [get_parallel_data] Error parsing inverter data...: {e}")
        print('\n ** Response **')
        try:
            print(response)
        except:
            pass
        print('\n ** END Response **')
        return ''

    log("[monitor.py] [get_parallel_data] END")
    return data

def get_data():
    # QPIGS (estado “principal” – normalmente PV1)
    try:
        log("[monitor.py] [get_data] INIT Serial Command: QPIGS")
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

        log("[monitor.py] [get_data] END")
        return data
    except Exception as e:
        log(f"[monitor.py] [get_data] Error parsing inverter data...: {e}")
        return ''

def get_settings():
    # QPIRI (rated / settings)
    try:
        log("[monitor.py] [get_settings] INIT Serial Command: QPIRI")
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

        log("[monitor.py] [get_settings] END")
        return data
    except Exception as e:
        log(f"[monitor.py] [get_settings] Error parsing inverter data...: {e}")
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
        log(f"[monitor.py] [send_data] Error sending to MQTT...: {e}")
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

# --- Utilidad: probar QPIGS2 una sola vez al inicio ---
def probe_qpigs2_once():
    print("\n--- PROBANDO QPIGS2 ---")
    try:
        r = serial_command('QPIGS2')
        print(f"QPIGS2 payload: {r}")
    except Exception as e:
        print(f"QPIGS2 error: {e}")

def main():
    time.sleep(randint(0, 5))  # desincroniza inicios en paralelo
    connect()

    serial_number = serial_command('QID')
    print('Reading from inverter ' + serial_number)

    # Prueba única de QPIGS2 para ver PV2 crudo (si lo soporta tu FW/protocolo)
    probe_qpigs2_once()

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

            # (Opcional) Log crudo de QPIGS2 cada vuelta (descomenta si quieres spamear el log)
            # try:
            #     raw_qpigs2 = serial_command('QPIGS2')
            #     print(f"[RAW QPIGS2] {raw_qpigs2}")
            # except Exception as e:
            #     print(f"[RAW QPIGS2] error: {e}")

            data = get_settings()
            if data != '':
                send_data(data, os.environ['MQTT_TOPIC_SETTINGS'])
            time.sleep(4)

        except Exception as e:
            print("Error occurred:", e)
            time.sleep(10)

if __name__ == '__main__':
    main()
