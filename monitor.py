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

battery_types = {'0': 'AGM', '1': 'Flooded', '2': 'User'}
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
    print('\n['+date+'] - [monitor.py] - [ MQTT Connect ]: INIT')
    global client
    client = mqtt.Client(client_id=os.environ['MQTT_CLIENT_ID'])
    client.username_pw_set(os.environ['MQTT_USER'], os.environ['MQTT_PASS'])
    client.connect(os.environ['MQTT_SERVER'])
    print(os.environ['DEVICE'])

def serial_command(command):
    print(command)

    try:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n['+date+'] - [monitor.py] - [ serial_command ]: INIT')
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
        print('\n['+date+'] - [monitor.py] - [ get_parallel_dat ]: INIT Serial Comand: QPGS0')
        data = '{'
        response = serial_command('QPGS0')
        nums = response.split(' ')
        if len(nums) < 27:
            return ''

        if nums[2] == 'L':
            data += '"Gridmode":1'
        else:
            data += '"Gridmode":0'
        data += ',"SerialNumber": ' + nums[1] + '"'
        data += ',"BatteryChargingCurrent": ' + nums[12] + '"'
        data += ',"BatteryDischargeCurrent": ' + nums[26] + '"'
        data += ',"TotalChargingCurrent": ' +  nums[15] + '"'
        data += ',"GridVoltage": ' +  nums[4] + '"'
        data += ',"GridFrequency": ' +  nums[5] + '"'
        data += ',"OutputVoltage": ' +  nums[6] + '"'
        data += ',"OutputFrequency": ' +  nums[7] + '"'
        data += ',"OutputAparentPower": ' +  nums[8] + '"'
        data += ',"OutputActivePower": ' +  nums[9] + '"'
        data += ',"LoadPercentage": ' +  nums[10] + '"'
        data += ',"BatteryVoltage": ' +  nums[11] + '"'
        data += ',"BatteryCapacity": ' +  nums[13] + '"'
        data += ',"PvInputVoltage": ' +  nums[14] + '"'
        data += ',"TotalAcOutputApparentPower": ' + nums[16] + '"'
        data += ',"TotalAcOutputActivePower": ' + nums[17] + '"'
        data += ',"TotalAcOutputPercentage": ' + nums[18] + '"'
        # data += ',"InverterStatus": ' + nums[19]
        data += ',"OutputMode": ' +  nums[20]  + '"'
        data += ',"ChargerSourcePriority": ' +  nums[21]  + '"'
        data += ',"MaxChargeCurrent": ' +  nums[22]  + '"'
        data += ',"MaxChargerRange": ' +  nums[23]  + '"'
        data += ',"MaxAcChargerCurrent": ' +  nums[24]  + '"'
        data += ',"PvInputCurrentForBattery": ' +  nums[25]  + '"'
        if nums[2] == 'B':
            data += ',"Solarmode":1'
        else:
            data += ',"Solarmode":0'

        data += '}'
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('['+date+'] - [monitor.py] - [ get_parallel_data ] - Error parsing inverter data...: ' + str(e))
        return ''
    return data

def get_data():
    #collect data from axpert inverter
    try:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n['+date+'] - [monitor.py] - [get_data]: INIT Serial Command: QPIGS')
        response = serial_command('QPIGS')
        nums = response.split(' ')
        if len(nums) < 21:
            return ''

        data = '{'

        data += '"BusVoltage":' +  nums[7] + '"'
        data += ',"InverterHeatsinkTemperature":' +  nums[11] + '"'
        data += ',"BatteryVoltageFromScc":' +  nums[14] + '"'
        data += ',"PvInputCurrent":' +  nums[12] + '"'
        data += ',"PvInputVoltage":' +  nums[13] + '"'
        data += ',"PvInputPower":' +  nums[19] + '"'
        data += ',"BatteryChargingCurrent": ' +  nums[9] + '"'
        data += ',"BatteryDischargeCurrent":' +  nums[15] + '"'
        data += ',"DeviceStatus":"' + nums[16] + '"'

        data += '}'
        return data
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('['+date+'] - [monitor.py] - [ get_data ] - Error parsing inverter data...: ' + str(e))
        return ''

def get_settings():
    #collect data from axpert inverter
    try:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n['+date+'] - [monitor.py] - [get_settings]: INIT Serial Command: QPIGS')
        response = serial_command('QPIRI')
        nums = response.split(' ')
        if len(nums) < 21:
            return ''

        data = '{'

        data += '"AcInputVoltage":' +  nums[0] + '"'
        data += ',"AcInputCurrent":' +  nums[1] + '"'
        data += ',"AcOutputVoltage":' +  nums[2] + '"'
        data += ',"AcOutputFrequency":' +  nums[3] + '"'
        data += ',"AcOutputCurrent":' +  nums[4] + '"'
        data += ',"AcOutputApparentPower":' +  nums[5] + '"'
        data += ',"AcOutputActivePower":' +  nums[6] + '"'
        data += ',"BatteryVoltage":' +  nums[7] + '"'
        data += ',"BatteryRechargeVoltage":' +  nums[8] + '"'
        data += ',"BatteryUnderVoltage":' +  nums[9] + '"'
        data += ',"BatteryBulkVoltage":' +  nums[10] + '"'
        data += ',"BatteryFloatVoltage":' +  nums[11] + '"'
        data += ',"BatteryType":"' + battery_types [safe_number(nums[12])] + '"'
        data += ',"MaxAcChargingCurrent":' +  nums[13] + '"'
        data += ',"MaxChargingCurrent":' +  nums[14] + '"'
        data += ',"InputVoltageRange":"' + voltage_ranges [safe_number(nums[15])] + '"'
        data += ',"OutputSourcePriority":"' + output_sources [safe_number(nums[16])] + '"'
        data += ',"ChargerSourcePriority":"' + charger_sources[safe_number(nums[17])] + '"'
        data += ',"MaxParallelUnits":' +  nums[18] + '"'
        data += ',"MachineType":"' + machine_types [safe_number(nums[19])] + '"'
        data += ',"Topology":"' + topologies [safe_number(nums[20])] + '"'
        data += ',"OutputMode":"' + output_modes [safe_number(nums[21])] + '"'
        data += ',"BatteryRedischargeVoltage":' +  nums[22] + '"'
        data += ',"PvOkCondition":"' + pv_ok_conditions [safe_number(nums[23])] + '"'
        data += ',"PvPowerBalance":"' + pv_power_balance [safe_number(nums[24])] + '"'
        data += ',"MaxBatteryCvChargingTime":' +  nums[25] + '"'
        
        data += '}'
        return data
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('['+date+'] - [monitor.py] - [ get_settings ] - Error parsing inverter data...: ' + str(e))
        return ''

def send_data(data, topic):
    try:
        client.publish(topic, data, 0, True)
    except Exception as e:
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print('\n['+date+'] - [monitor.py] - [ send_data ] - Error sending to emoncms...: ' + str(e))
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