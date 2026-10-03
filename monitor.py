#!/usr/bin/python
# -*- coding: utf-8 -*-

from datetime import datetime
import errno
import os
from random import randint
import re
import struct
import time
import crcmod.predefined
import paho.mqtt.client as mqtt

battery_types = {'0': 'AGM', '1': 'Flooded', '2': 'User', '3': 'Lithium'}
voltage_ranges = {'0': 'Appliance', '1': 'UPS'}
output_sources = {'0': 'utility', '1': 'solar', '2': 'battery'}
charger_sources = {
    '0': 'utility first',
    '1': 'solar first',
    '2': 'solar + utility',
    '3': 'solar only',
}
machine_types = {'00': 'Grid tie', '01': 'Off Grid', '10': 'Hybrid'}
topologies = {'0': 'transformerless', '1': 'transformer'}
output_modes = {
    '0': 'single machine output',
    '1': 'parallel output',
    '2': 'Phase 1 of 3 Phase output',
    '3': 'Phase 2 of 3 Phase output',
    '4': 'Phase 3 of 3 Phase output',
}
pv_ok_conditions = {
    '0': (
        'As long as one unit of inverters has connect PV, parallel system will'
        ' consider PV OK'
    ),
    '1': (
        'Only All of inverters have connect PV, parallel system will consider'
        ' PV OK'
    ),
}
pv_power_balance = {
    '0': 'PV input max current will be the max charged current',
    '1': (
        'PV input max power will be the sum of the max charged power and loads'
        ' power'
    ),
}

client = None

# Variables de control para evitar inundación de logs (tope de 5 avisos seguidos)
consecutive_serial_errors = 0
MAX_LOG_ERRORS = 5
error_logged_silenced = False


def now():
  return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def connect():
  print(f'\n\n\n[{now()}] - [monitor.py] - [ MQTT Connect ]: INIT')
  global client
  client = mqtt.Client(client_id=os.environ.get('MQTT_CLIENT_ID', 'axpert_mon'))
  client.username_pw_set(
      os.environ.get('MQTT_USER', ''), os.environ.get('MQTT_PASS', '')
  )
  client.connect(os.environ.get('MQTT_SERVER', 'localhost'))
  print(f"Dispositivo configurado: {os.environ.get('DEVICE')}")


# ---------- Helpers ----------
def sanitize_id(s: str) -> str:
  return re.sub(r'[^A-Za-z0-9_-]+', '', s or '')


def safe_number(value):
  try:
    return int(value)
  except ValueError:
    try:
      return float(value)
    except ValueError:
      return value


def map_with_log(table: dict, value: str, label: str) -> str:
  if value in table:
    return table[value]
  print(
      f'[get_settings] Valor inesperado en {label}: {value} (claves válidas:'
      f' {list(table.keys())})'
  )
  return f'{label}_invalid({value})'


def send_data(data, topic):
  try:
    if client and data:
      client.publish(topic, data, 0, True)
      return 1
  except Exception as e:
    print(
        f'[{now()}] - [monitor.py] - [ send_data ] - Error enviando a MQTT:'
        f' {e}'
    )
    return 0
  return 0


# ---------- HID / Comunicación Serie ----------
def _build_frame(cmd: str) -> bytes:
  xmodem_crc_func = crcmod.predefined.mkCrcFun('xmodem')
  cb = cmd.encode('ascii')
  crc = xmodem_crc_func(cb)
  crc_b = struct.pack('>H', crc)
  return cb + crc_b + b'\x0d'


def _read_until_cr(fd: int, timeout_s: float = 4.0) -> bytes:
  deadline = time.time() + timeout_s
  r = b''
  while b'\r' not in r:
    if time.time() > deadline:
      raise TimeoutError('Read operation timed out')
    try:
      c = os.read(fd, 128)
      if c:
        r += c
      else:
        time.sleep(0.01)
    except OSError as e:
      if e.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
        time.sleep(0.01)
        continue
      raise
  return r


def _flush_input(fd: int):
  try:
    while True:
      if not os.read(fd, 512):
        break
  except OSError:
    pass


def _write_oneshot(fd: int, frame: bytes):
  os.write(fd, frame)


def _write_split_cr_padded(fd: int, frame: bytes):
  cmd_crc, cr = frame[:-1], frame[-1:]
  os.write(fd, cmd_crc)
  time.sleep(0.04)  # Pausa obligatoria para procesar reporte HID 1
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
    if off < n:
      time.sleep(0.04)  # Pausa obligatoria entre bloques HID


def serial_command(command: str) -> str:
  global consecutive_serial_errors, error_logged_silenced
  DEVICE = os.environ.get('DEVICE', '/dev/ttyUSB0')
  frame = _build_frame(command)
  fd = None
  try:
    fd = os.open(DEVICE, os.O_RDWR | os.O_NONBLOCK)
    time.sleep(0.02)
    _flush_input(fd)

    writers = (
        ('blocks8', _write_blocks8),
        ('split-cr-padded', _write_split_cr_padded),
        ('one-shot', _write_oneshot),
    )

    last_err = None
    for writer_name, writer in writers:
      try:
        _flush_input(fd)
        writer(fd, frame)
        break
      except Exception as e:
        last_err = e
        continue
    else:
      raise last_err if last_err else OSError('Fallaron todas las estrategias')

    resp = _read_until_cr(fd, timeout_s=4.0)

    try:
      s = resp.decode('utf-8')
    except UnicodeDecodeError:
      s = resp.decode('iso-8859-1')

    b = s.find('(')
    e = s.find('\r')
    payload = s[b + 1 : e] if (b != -1 and e != -1 and e > b) else s.strip()
    os.close(fd)

    # Éxito: reseteamos contadores de error si veníamos de fallo
    if consecutive_serial_errors > 0:
      if error_logged_silenced:
        print(f'[{now()}] - [INFO] Conexión serie restablecida con éxito.')
      consecutive_serial_errors = 0
      error_logged_silenced = False

    return payload

  except Exception as e:
    consecutive_serial_errors += 1
    if consecutive_serial_errors <= MAX_LOG_ERRORS:
      print(
          f"[{now()}] - [serial_command] ({consecutive_serial_errors}/{MAX_LOG_ERRORS}) Error ejecutando '{command}' en {DEVICE}: {e}"
      )
      if consecutive_serial_errors == MAX_LOG_ERRORS:
        print(
            f'[{now()}] - [AVISO] Límite de logs de error alcanzado. Silenciando'
            ' trazas hasta recuperación.'
        )
        error_logged_silenced = True

    if fd is not None:
      try:
        os.close(fd)
      except:
        pass
    return ''


def get_healthcheck(value):
  try:
    return '{"Health": "OK"}' if value == 'true' else '{"Health": "NO OK"}'
  except Exception as e:
    print(f'[{now()}] - [get_healthcheck] - Error: {e}')
    return ''


# ---------- Lecturas y Parsers ----------
def get_parallel_data():
  try:
    response = serial_command('QPGS0')
    if not response or 'NAK' in response:
      return ''
    nums = response.split(' ')
    if len(nums) < 26:
      return ''

    data = '{'
    data += '"Gridmode":' + ('1' if nums[2] == 'L' else '0')
    data += ',"SerialNumber": ' + str(safe_number(nums[1]))
    data += ',"BatteryChargingCurrent": ' + str(safe_number(nums[12]))
    data += (
        ',"BatteryDischargeCurrent": '
        + str(safe_number(nums[26]))
        if len(nums) > 26
        else '0'
    )
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
    data += ',"OutputMode": ' + (
        str(safe_number(nums[20])) if len(nums) > 20 else '0'
    )
    data += ',"Solarmode":' + ('1' if nums[2] == 'B' else '0') + '}'
    return data
  except Exception as e:
    print(f'[{now()}] - [get_parallel_data] - Error: {e}')
    return ''


def get_data():
  try:
    response = serial_command('QPIGS')
    if not response or 'NAK' in response:
      return ''
    nums = response.split(' ')
    if len(nums) < 17:
      return ''

    pv_power = (
        safe_number(nums[19])
        if len(nums) > 19
        else round(safe_number(nums[12]) * safe_number(nums[13]), 1)
    )

    data = '{'
    data += '"BusVoltage":' + str(safe_number(nums[7]))
    data += ',"InverterHeatsinkTemperature":' + str(safe_number(nums[11]))
    data += ',"BatteryVoltageFromScc":' + str(safe_number(nums[14]))
    data += ',"PvInputCurrent":' + str(safe_number(nums[12]))
    data += ',"PvInputVoltage":' + str(safe_number(nums[13]))
    data += ',"PvInputPower":' + str(pv_power)
    data += ',"BatteryChargingCurrent": ' + str(safe_number(nums[9]))
    data += ',"BatteryDischargeCurrent":' + str(safe_number(nums[15]))
    data += ',"DeviceStatus":"' + (nums[16] if len(nums) > 16 else '') + '"}'
    return data
  except Exception as e:
    print(f'[{now()}] - [get_data] - Error: {e}')
    return ''


def get_qpigs2_json():
  try:
    r = serial_command('QPIGS2')
    if not r or 'NAK' in r:
      return ''
    parts = r.split()
    if len(parts) >= 3:
      val0 = float(safe_number(parts[0]))
      val1 = float(safe_number(parts[1]))
      val2 = float(safe_number(parts[2]))

      if val0 > 50 and val1 < 50:
        pv2_v, pv2_i = val0, val1
      else:
        pv2_i, pv2_v = val0, val1

      pv2_p = val2
      if pv2_p <= 0 and (pv2_i > 0 and pv2_v > 0):
        pv2_p = round(pv2_v * pv2_i, 1)

      return (
          f'{{"Pv2InputCurrent": {pv2_i}, "Pv2InputVoltage": {pv2_v},'
          f' "Pv2InputPower": {pv2_p}}}'
      )
    return ''
  except Exception as e:
    print(f'[{now()}] - [get_qpigs2] - Error: {e}')
    return ''


def get_settings():
  try:
    response = serial_command('QPIRI')
    if not response or 'NAK' in response:
      return ''
    nums = response.split(' ')
    if len(nums) < 15:
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
    data += (
        ',"BatteryType":"'
        + map_with_log(battery_types, nums[12], 'BatteryType')
        + '"'
    )
    data += ',"MaxAcChargingCurrent":' + str(safe_number(nums[13]))
    data += ',"MaxChargingCurrent":' + str(safe_number(nums[14])) + '}'
    return data
  except Exception as e:
    print(f'[{now()}] - [get_settings] - Error: {e}')
    return ''


# ---------- Bucle Principal ----------
def main():
  time.sleep(randint(0, 3))
  connect()

  sn = '96342210104295'

  while True:
    try:
      # 1. HealthCheck
      d = get_healthcheck('true')
      if d:
        send_data(d, os.environ.get('MQTT_HEALTHCHECK', 'power/healthcheck'))
      time.sleep(0.3)

      # 2. QPGS0 (Paralelo y Batería)
      d = get_parallel_data()
      if d:
        send_data(
            d, os.environ.get('MQTT_TOPIC_PARALLEL', 'power/axpert_parallel')
        )
      time.sleep(0.3)

      # 3. QPIGS (MPPT 1 y Métricas Generales)
      d = get_data()
      if d:
        topic_sn = os.environ.get('MQTT_TOPIC', 'power/axpert{sn}').replace(
            '{sn}', sn
        )
        send_data(d, topic_sn)
      time.sleep(0.3)

      # 4. QPIGS2 (MPPT 2 - SEGUNDO STRING)
      pv2 = get_qpigs2_json()
      if pv2:
        topic_pv2 = os.environ.get('MQTT_TOPIC', 'power/axpert{sn}').replace(
            '{sn}', sn + '_pv2'
        )
        send_data(pv2, topic_pv2)
      time.sleep(0.3)

      # 5. QPIRI (Configuración)
      d = get_settings()
      if d:
        send_data(
            d, os.environ.get('MQTT_TOPIC_SETTINGS', 'power/axpert_settings')
        )

      update_time = 2
      try:
        update_time = int(os.environ.get('UPDATE_TIME', 2))
      except ValueError:
        update_time = 2

      time.sleep(update_time)

    except Exception as e:
      d = get_healthcheck('false')
      if d:
        send_data(d, os.environ.get('MQTT_HEALTHCHECK', 'power/healthcheck'))
      print(f'[{now()}] - Excepción en bucle principal: {e}')
      time.sleep(5)


if __name__ == '__main__':
  main()