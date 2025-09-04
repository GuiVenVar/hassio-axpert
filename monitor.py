#! /usr/bin/python
# -*- coding: utf-8 -*-

# Voltronic/Axpert monitor — QPIGS2 forzado a split-cr-padded, SN saneado para topics.
# - CRC XMODEM binario (2 bytes big-endian)
# - one-shot / split-cr-padded / blocks8 (verificación) para TODOS menos QPIGS2
# - QPIGS2: split-cr-padded únicamente
# - Publica PV2 (I,V,P) en {sn}_pv2
# - Sin recursión; un intento por comando (el bucle sigue vivo)

import os, time, errno, struct, re
from datetime import datetime
import crcmod.predefined
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
    # Deja solo A-Z a-z 0-9 _ -
    return re.sub(r'[^A-Za-z0-9_-]+', '', s or '')

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
    crc_b = struct.pack('>H', crc)      # 2 bytes big-endian
    return cb + crc_b + b'\x0d'         # cmd + CRC + CR

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
    # Enviar cmd+CRC y luego un paquete de 8B cuyo primer byte es '\r' (resto padding)
    cmd_crc, cr = frame[:-1], frame[-1:]
    os.write(fd, cmd_crc)
    os.write(fd, cr + b'\x00' * 7)

def _write_blocks8(fd: int, frame: bytes):
    # Dividir en bloques de 8 bytes; último bloque rellenado con 0x00 tras el CR
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
    """
    Para TODOS los comandos: probar one-shot → split-cr-padded → blocks8 (en ese orden),
    y devolver en cuanto haya respuesta.
    EXCEPTO para QPIGS2: usar SIEMPRE split-cr-padded directamente.
    """
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

def get_healtcheck(value):
    try:
        print('  ')
        print('  ')
        print(f'[{now()}] - [monitor.py] - [ get_healtcheck ]: INIT Serial Comand: get_healtcheck')
        data = '{'        
        if value == 'true':
         data += '"Gridmode": 1'
        else:           
         data += '"Gridmode": 0'
        data += '}'
    except Exception as e:
        print(f'[{now()}] - [monitor.py] - [ get_healtcheck ] - Error parsing inverter data...: {e}')
        return ''
    print(f'[{now()}] - [monitor.py] - [ get_healtcheck ]: END')
    
    return data

# ---------- Lecturas ----------
def get_parallel_data():
    try:     
        print('  ')
        print('  ')
        print(f'[{now()}] - [monitor.py] - [ get_parallel_dat ]: INIT Serial Comand: QPGS0')
        data = '{'
        response = serial_command('QPGS0')
        nums = response.split(' ')
        if len(nums) < 27: return ''
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
        data += ',"Solarmode":' + ('1' if nums[2]=='B' else '0') 
        + '}'
    except Exception as e:
        print(f'[{now()}] - [monitor.py] - [ get_parallel_data ] - Error parsing inverter data...: {e}')
        return ''
    print(f'[{now()}] - [monitor.py] - [ get_parallel_dat ]: END')
    return data

def get_data():
    try:       
        print('  ')
        print('  ')
        print(f'[{now()}] - [monitor.py] - [get_data]: INIT Serial Command: QPIGS')
        response = serial_command('QPIGS')
        nums = response.split(' ')
        if len(nums) < 21: return ''
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
        print(f'[{now()}] - [monitor.py] - [ get_data ]: END')
        return data
    except Exception as e:
        print(f'[{now()}] - [monitor.py] - [ get_data ] - Error parsing inverter data...: {e}')
        return ''

def get_qpigs2_json():
    """QPIGS2 (split-cr-padded). Devuelve JSON con PV2 (I,V,P) o '' si no parsea."""
    try:        
        print('  ')
        print('  ')
        print(f'[{now()}] - [monitor.py] - [get_qpigs2_json]: INIT Serial Command: QPIGS2 (split-cr-padded only)')
        r = serial_command('QPIGS2')  # típicamente "I V P", p.ej. "16.7 222.3 03732"
        parts = r.split()
        if len(parts) >= 3:
            try:
                pv2_i = float(parts[0]); pv2_v = float(parts[1]); pv2_p = float(parts[2])
                if pv2_p <= 0: pv2_p = round(pv2_v * pv2_i, 1)
                print(f"[QPIGS2] PV2 Current(A)={pv2_i}, PV2 Voltage(V)={pv2_v}, PV2 Power(W)={pv2_p}")
                return '{' + f'"Pv2InputCurrent": {pv2_i}, "Pv2InputVoltage": {pv2_v}, "Pv2InputPower": {pv2_p}' + '}'
            except Exception as pe:
                print(f"[get_qpigs2] parse error: {pe} raw='{r}'")
                return ''
        else:
            print(f"[get_qpigs2] respuesta corta: '{r}'")
            return ''
    except Exception as e:
        print(f'[{now()}] - [monitor.py] - [ get_qpigs2 ] - Error...: {e}')
        return ''

def get_settings():
    try:
        print('  ')
        print('  ')
        print(f'\n\n\n[{now()}] - [monitor.py] - [get_settings]: INIT Serial Command: QPIRI')
        response = serial_command('QPIRI')
        nums = response.split(' ')
        if len(nums) < 21: return ''
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
        print(f'[{now()}] - [monitor.py] - [ get_settings ]: END')
        return data
    except Exception as e:
        print(f'[{now()}] - [monitor.py] - [ get_settings ] - Error parsing inverter data...: ' + str(e))
        return ''

# ---------- MAIN ----------
def main():
    time.sleep(randint(0, 5))
    connect()

    try:
        raw_sn = serial_command('QID')
    except Exception:
        raw_sn = 'unknown'
    sn = sanitize_id(raw_sn.strip())
    print(f'Reading from inverter {sn} (raw="{raw_sn}")')

    while True:
        try:
            #HealtCheck
            d = get_healtcheck('true')
            if d: send_data(d, os.environ['MQTT_HEALTCHECK'])
            time.sleep(2)

            # QPGS0
            d = get_parallel_data()
            if d: send_data(d, os.environ['MQTT_TOPIC_PARALLEL'])
            time.sleep(2)

            # QPIGS
            d = get_data()
            if d: send_data(d, os.environ['MQTT_TOPIC'].replace('{sn}', sn))
            time.sleep(2)

            # QPIGS2 (split-cr-padded ONLY)
            pv2 = get_qpigs2_json()
            if pv2: send_data(pv2, os.environ['MQTT_TOPIC'].replace('{sn}', sn + '_pv2'))
            time.sleep(2)

            # QPIRI
            d = get_settings()
            if d: send_data(d, os.environ['MQTT_TOPIC_SETTINGS'])
            time.sleep(5)



        except Exception as e:
            #HealtCheck
            d = get_healtcheck('false')
            if d: send_data(d, os.environ['mqtt_healtCheck'])
            time.sleep(2)
            print("Error occurred:", e)
            time.sleep(10)

if __name__ == '__main__':
    main()
