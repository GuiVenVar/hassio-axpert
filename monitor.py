#! /usr/bin/python

# Axpert Inverter control script

# Read values from inverter, sends values to mqtt,
# calculation of CRC is done by XMODEM

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

def serial_command(command):
    print(command)

    try:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n\n\n['+date+'] - [monitor.py] - [ serial_command ]: INIT')
        xmodem_crc_func = crcmod.predefined.mkCrcFun('xmodem')
        command_bytes = command.encode('utf-8')
        command_crc_hex = hex(xmodem_crc_func(command_bytes)).replace('0x', '')
        command_crc = command_bytes + unhexlify(command_crc_hex.encode('utf-8')) + b'\x0d'

        try:
            file = open(os.environ['DEVICE'], 'r+')
            fd = file.fileno()
            fl = fcntl.fcntl(fd, fcntl.F_GETFL)
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        except Exception as e:
            print('error open file descriptor: ' + str(e))
            exit()

        os.write(fd, command_crc)

        response = b''
        timeout_counter = 0
        while b'\r' not in response:
            if timeout_counter > 500:
                raise Exception('Read operation timed out')
            timeout_counter += 1
            try:
                response += os.read(fd, 100)
            except Exception as e:
                time.sleep(0.01)
            if len(response) > 0 and (response[0] != ord('(') or b'NAKss' in response):
                raise Exception('NAKss')

        try:
            response = response.decode('utf-8')
        except UnicodeDecodeError:
            response = response.decode('iso-8859-1')

        print(response)
        print('\n\n\n['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [monitor.py] - [ serial_command ]: END \n\n')

        response = response.rstrip()
        lastI = response.find('\r')
        response = response[1:lastI-2]

        file.close()
        return response
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('['+date+'] - [monitor.py] - [ serial_command ] - Error reading inverter...: ' + str(e))
        file.close()
        time.sleep(0.1)
        connect()
        return serial_command(command)
    
def get_parallel_data():
    #collect data from axpert inverter
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
        # data += ',"InverterStatus": ' + nums[19]
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
        print(response)
        print('\n ** END Response **')
        return ''

    print('\n\n\n['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [monitor.py] - [ get_parallel_dat ]: END \n\n')
    return data

def get_data():
    #collect data from axpert inverter
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
    
def get_netDawta():
    #collect data from axpert inverter
    try:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n\n\n['+date+'] - [monitor.py] - [get_data]: INIT Serial Command: QPIGS2')
        response = serial_command('QPIGS2')
        nums = response.split(' ')
        if len(nums) < 21:
            return ''

        data = '\n\n\n{'

        data += response

        data += '}\n\n\n'
        print('\n\n\n['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [monitor.py] - [ get_data ]: END \n\n')

        return data
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('['+date+'] - [monitor.py] - [ get_data ] - Error parsing inverter data...: ' + str(e))
        return ''
def get_netDawta1():
    #collect data from axpert inverter
    try:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n\n\n['+date+'] - [monitor.py] - [get_data]: INIT Serial Command: QPIGS2')
        response = serial_command('QPGS2')
        nums = response.split(' ')
        if len(nums) < 21:
            return ''

        data = '\n\n\n{'

        data += response

        data += '}\n\n\n'
        print('\n\n\n['+datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'] - [monitor.py] - [ get_data ]: END \n\n')

        return data
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('['+date+'] - [monitor.py] - [ get_data ] - Error parsing inverter data...: ' + str(e))
        return ''


def get_settings():
    #collect data from axpert inverter
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
        # Aquí logeas el fallo con claridad
        print(f"[get_settings] Valor inesperado en {label}: {value} (claves válidas: {list(table.keys())})")
        return f"{label}_invalid({value})"

def send_data(data, topic):
    try:
        client.publish(topic, data, 0, True)
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n\n\n['+date+'] - [monitor.py] - [ send_data ] - Error sending to emoncms...: ' + str(e))
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
    time.sleep(randint(0, 5))  # so parallel streams might start at different times
    connect()
    
    serial_number = serial_command('QID')
    print('Reading from inverter ' + serial_number)

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
            
            data = get_netDawta()
            if data != '':
                send_data(data, os.environ['MQTT_TOPIC'].replace('{sn}', serial_number+'_100'))
            time.sleep(1)

            data = get_netDawta1()
            if data != '':
                send_data(data, os.environ['MQTT_TOPIC'].replace('{sn}', serial_number+'_101'))
            time.sleep(1)  

            data = get_settings()
            if data != '':
                send_data(data, os.environ['MQTT_TOPIC_SETTINGS'])
            time.sleep(4)
        except Exception as e:
            print("Error occurred:", e)
            # Consider handling specific errors or performing a reconnect here
            time.sleep(10)  # Delay before retrying to avoid continuous strain

if __name__ == '__main__':
    main()