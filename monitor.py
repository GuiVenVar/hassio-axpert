#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import random
import json
import struct
import logging
import serial
import crcmod.predefined
import paho.mqtt.client as mqtt

# ---------------- LOGGER ----------------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("inverter-monitor")

# Tablas de mapeo
battery_types = {'0': 'AGM', '1': 'Flooded', '2': 'User', '3': 'Lithium'}
voltage_ranges = {'0': 'Appliance', '1': 'UPS'}
output_sources = {'0': 'utility', '1': 'solar', '2': 'battery'}
charger_sources = {'0': 'utility first', '1': 'solar first', '2': 'solar + utility', '3': 'solar only'}
machine_types = {'00': 'Grid tie', '01': 'Off Grid', '10': 'Hybrid'}
topologies = {'0': 'transformerless', '1': 'transformer'}
output_modes = {
    '0': 'single machine output', '1': 'parallel output',
    '2': 'Phase 1 of 3 Phase output', '3': 'Phase 2 of 3 Phase output', '4': 'Phase 3 of 3 Phase output'
}
pv_ok_conditions = {'0': 'PV OK if one inverter connected', '1': 'PV OK only if all connected'}
pv_power_balance = {'0': 'PV max current = max charged current', '1': 'PV max power = charge + loads'}

client = None
crc_xmodem = crcmod.predefined.mkCrcFun('xmodem')

def map_with_log(table: dict, value: str, label: str) -> str:
    if value in table:
        return table[value]
    return f"{label}_unknown({value})"

def safe_number(value):
    try:
        val = float(value)
        return int(val) if val.is_integer() else val
    except Exception:
        return 0.0

def build_crc_frame(cmd: str) -> bytes:
    cb = cmd.encode('ascii')
    crc = crc_xmodem(cb)
    return cb + struct.pack('>H', crc) + b'\r'

# ---------------- MQTT ----------------
def connect_mqtt():
    global client
    mqtt_server = os.environ.get("MQTT_SERVER", "core-mosquitto")
    mqtt_client_id = os.environ.get("MQTT_CLIENT_ID", f"inverter-{random.randint(1000, 9999)}")
    mqtt_user = os.environ.get("MQTT_USER")
    mqtt_pass = os.environ.get("MQTT_PASS")

    logger.info("Conectando a MQTT en %s...", mqtt_server)
    client = mqtt.Client(client_id=mqtt_client_id)
    if mqtt_user and mqtt_pass:
        client.username_pw_set(mqtt_user, mqtt_pass)

    try:
        client.connect(mqtt_server, port=1883, keepalive=60)
        client.loop_start()
        logger.info("MQTT conectado.")
    except Exception as e:
        logger.exception("Error de conexión MQTT: %s", e)
        raise

def send_data(data, topic):
    if client is None or not data:
        return 0
    try:
        payload = json.dumps(data, ensure_ascii=False) if isinstance(data, dict) else str(data)
        client.publish(topic, payload, qos=0, retain=True)
        return 1
    except Exception as e:
        logger.error("Error publicando en %s: %s", topic, e)
        return 0

# ---------------- GESTOR SERIE CON BACKOFF ----------------
class Inversor:
    BACKOFF_STEPS = [1, 10, 60, 120, 300]  # Segundos de espera progresiva

    def __init__(self, port="/dev/ttyUSB0", baudrate=2400, timeout=1.5):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.ser = None
        self.backoff_idx = 0
        self.next_retry_time = 0.0
        self._connect()

    def _connect(self) -> bool:
        now = time.time()
        if now < self.next_retry_time:
            return False

        try:
            if self.ser and self.ser.is_open:
                try:
                    self.ser.close()
                except Exception:
                    pass

            self.ser = serial.Serial(self.port, baudrate=self.baudrate, timeout=self.timeout)
            logger.info("Puerto serie abierto con éxito en %s a %d baudios.", self.port, self.baudrate)
            self.backoff_idx = 0
            self.next_retry_time = 0.0
            return True

        except Exception as e:
            delay = self.BACKOFF_STEPS[min(self.backoff_idx, len(self.BACKOFF_STEPS) - 1)]
            self.next_retry_time = now + delay
            logger.error("Fallo al abrir %s: %s. Próximo reintento en %ds.", self.port, e, delay)
            self.backoff_idx = min(self.backoff_idx + 1, len(self.BACKOFF_STEPS) - 1)
            self.ser = None
            return False

    def _raw_query(self, frame: bytes) -> str:
        if self.ser is None or not self.ser.is_open:
            if not self._connect():
                return ""
        try:
            self.ser.reset_input_buffer()
            self.ser.write(frame)
            raw = self.ser.read_until(b'\r')
            return raw.decode('latin1', errors='ignore').strip()
        except Exception as e:
            logger.error("Error de comunicación serie: %s. Iniciando enfriamiento...", e)
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None
            now = time.time()
            delay = self.BACKOFF_STEPS[0]
            self.next_retry_time = now + delay
            self.backoff_idx = 1
            return ""

    def send_cmd(self, cmd: str) -> str:
        # Intento 1: ASCII Plano
        resp = self._raw_query(cmd.encode('ascii') + b'\r')

        # Intento 2: Con CRC si no responde o devuelve NAK
        if not resp or resp.startswith('(NAK'):
            resp_crc = self._raw_query(build_crc_frame(cmd))
            if resp_crc and not resp_crc.startswith('(NAK'):
                resp = resp_crc

        if not resp or resp.startswith('(NAK') or not resp.startswith('('):
            return ""

        content = resp[1:]
        if len(content) > 3 and content[-3] in (' ', '\r', '\n'):
            return content[:-3].strip()
        return content.strip()

    def parse_qpigs(self, resp: str) -> dict:
        if not resp:
            return {}
        nums = resp.split()
        if len(nums) < 16:
            return {}

        return {
            "BusVoltage": safe_number(nums[7]),
            "InverterHeatsinkTemperature": safe_number(nums[11]),
            "BatteryVoltageFromScc": safe_number(nums[14]),
            "PvInputCurrent": safe_number(nums[12]),
            "PvInputVoltage": safe_number(nums[13]),
            "PvInputPower": safe_number(nums[19]) if len(nums) > 19 else safe_number(nums[12]) * safe_number(nums[13]),
            "BatteryChargingCurrent": safe_number(nums[9]),
            "BatteryDischargeCurrent": safe_number(nums[15]),
            "DeviceStatus": nums[16] if len(nums) > 16 else ""
        }

    def parse_qpigs2(self, resp: str) -> dict:
        if not resp:
            return {}
        parts = resp.split()
        if len(parts) < 3:
            return {}

        pv2_i = safe_number(parts[0])
        pv2_v = safe_number(parts[1])
        pv2_p = safe_number(parts[2])
        if pv2_p <= 0 and (pv2_i > 0 and pv2_v > 0):
            pv2_p = round(pv2_v * pv2_i, 1)

        return {
            "Pv2InputCurrent": pv2_i,
            "Pv2InputVoltage": pv2_v,
            "Pv2InputPower": pv2_p
        }

    def parse_qpgs0(self, resp: str) -> dict:
        if not resp:
            return {}
        nums = resp.split()
        if len(nums) < 15:
            return {}

        return {
            "Gridmode": 1 if (len(nums) > 2 and nums[2] == 'L') else 0,
            "SerialNumber": safe_number(nums[1]),
            "BatteryChargingCurrent": safe_number(nums[12]),
            "BatteryDischargeCurrent": safe_number(nums[26]) if len(nums) > 26 else 0,
            "TotalChargingCurrent": safe_number(nums[15]) if len(nums) > 15 else 0,
            "GridVoltage": safe_number(nums[4]),
            "GridFrequency": safe_number(nums[5]),
            "OutputVoltage": safe_number(nums[6]),
            "OutputFrequency": safe_number(nums[7]),
            "OutputAparentPower": safe_number(nums[8]),
            "OutputActivePower": safe_number(nums[9]),
            "LoadPercentage": safe_number(nums[10]),
            "BatteryVoltage": safe_number(nums[11]),
            "BatteryCapacity": safe_number(nums[13]),
            "PvInputVoltage": safe_number(nums[14]),
            "OutputMode": safe_number(nums[20]) if len(nums) > 20 else 0,
            "Solarmode": 1 if (len(nums) > 2 and nums[2] == 'B') else 0
        }

    def parse_qpiri(self, resp: str) -> dict:
        if not resp:
            return {}
        nums = resp.split()
        if len(nums) < 15:
            return {}

        return {
            "AcInputVoltage": safe_number(nums[0]),
            "AcInputCurrent": safe_number(nums[1]),
            "AcOutputVoltage": safe_number(nums[2]),
            "AcOutputFrequency": safe_number(nums[3]),
            "AcOutputCurrent": safe_number(nums[4]),
            "AcOutputApparentPower": safe_number(nums[5]),
            "AcOutputActivePower": safe_number(nums[6]),
            "BatteryVoltage": safe_number(nums[7]),
            "BatteryRechargeVoltage": safe_number(nums[8]),
            "BatteryUnderVoltage": safe_number(nums[9]),
            "BatteryBulkVoltage": safe_number(nums[10]),
            "BatteryFloatVoltage": safe_number(nums[11]),
            "BatteryType": map_with_log(battery_types, nums[12], "BatteryType"),
            "MaxAcChargingCurrent": safe_number(nums[13]),
            "MaxChargingCurrent": safe_number(nums[14])
        }

# ---------------- EJECUCIÓN PRINCIPAL ----------------
def main():
    time.sleep(1)
    connect_mqtt()

    serial_port = os.environ.get("DEVICE", os.environ.get("SERIAL_PORT", "/dev/ttyUSB0"))
    inv = Inversor(port=serial_port, baudrate=2400)

    sn = "96342210104295"
    topic_base = os.environ.get("MQTT_TOPIC", "power/axpert{sn}")
    topic_parallel = os.environ.get("MQTT_TOPIC_PARALLEL", "power/axpert")
    topic_settings = os.environ.get("MQTT_TOPIC_SETTINGS", "power/axpert_settings")
    topic_health = os.environ.get("MQTT_HEALTHCHECK", "axpert/healthCheck")

    try:
        poll_interval = int(os.environ.get("POLL_INTERVAL", os.environ.get("UPDATE_TIME", 2)))
    except ValueError:
        poll_interval = 2

    logger.info("Monitor iniciado en %s (Polling: %ds).", serial_port, poll_interval)
    loop_count = 0

    while True:
        try:
            # 1. Telemetría Principal (QPIGS)
            resp = inv.send_cmd("QPIGS")
            data_qpigs = inv.parse_qpigs(resp)
            if data_qpigs:
                send_data(data_qpigs, topic_base.replace("{sn}", sn))

            # 2. Telemetría MPPT2 (QPIGS2)
            resp2 = inv.send_cmd("QPIGS2")
            data_qpigs2 = inv.parse_qpigs2(resp2)
            if data_qpigs2:
                send_data(data_qpigs2, topic_base.replace("{sn}", sn + "_pv2"))

            # 3. Telemetría Paralelo/Salida (QPGS0)
            resp_p = inv.send_cmd("QPGS0")
            data_qpgs0 = inv.parse_qpgs0(resp_p)
            if data_qpgs0:
                send_data(data_qpgs0, topic_parallel)

            # 4. Settings (QPIRI cada ~60s)
            if loop_count % 30 == 0:
                resp_s = inv.send_cmd("QPIRI")
                data_qpiri = inv.parse_qpiri(resp_s)
                if data_qpiri:
                    send_data(data_qpiri, topic_settings)

            # 5. Healthcheck Real
            health_ok = bool(data_qpigs or data_qpgs0)
            send_data({"Health": "OK" if health_ok else "NO OK"}, topic_health)

        except Exception as e:
            logger.exception("Error en ciclo principal: %s", e)
            send_data({"Health": "NO OK"}, topic_health)

        loop_count += 1
        time.sleep(poll_interval)

if __name__ == "__main__":
    main()