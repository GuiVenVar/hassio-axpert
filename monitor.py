#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import os
import random
import threading
import time

import crcmod.predefined
import paho.mqtt.client as mqtt
import serial

# ---------------- CONFIGURACIÓN ----------------
DEFAULT_SERIAL_PORT = "/dev/ttyUSB0"
DEFAULT_BAUDRATE = 2400
DEFAULT_POLL_INTERVAL = 2
DEFAULT_SETTINGS_INTERVAL = 600
DEFAULT_HEALTH_TOPIC = "axpert/healthCheck"
SERIAL_TIMEOUT = 1.5
MAX_SERIAL_RETRIES = 2

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("inverter-monitor")

battery_types = {
    "0": "AGM",
    "1": "Flooded",
    "2": "User",
    "3": "Lithium",
}
voltage_ranges = {"0": "Appliance", "1": "UPS"}
output_sources = {"0": "utility", "1": "solar", "2": "battery"}
charger_sources = {
    "0": "utility first",
    "1": "solar first",
    "2": "solar + utility",
    "3": "solar only",
}
machine_types = {"00": "Grid tie", "01": "Off Grid", "10": "Hybrid"}
topologies = {"0": "transformerless", "1": "transformer"}
output_modes = {
    "0": "single machine output",
    "1": "parallel output",
    "2": "Phase 1 of 3 Phase output",
    "3": "Phase 2 of 3 Phase output",
    "4": "Phase 3 of 3 Phase output",
}

CRC_XMODEM = crcmod.predefined.mkCrcFun("xmodem")


def env_int(name, default, minimum=1):
    try:
        value = int(os.environ.get(name, default))
        return value if value >= minimum else default
    except (TypeError, ValueError):
        return default


def safe_number(value):
    try:
        number = float(value)
        return int(number) if number.is_integer() else number
    except (TypeError, ValueError):
        return 0


def map_with_log(table, value, label):
    if value in table:
        return table[value]
    logger.warning("Valor desconocido para %s: %r", label, value)
    return f"{label}_unknown({value})"


def build_crc_frame(command):
    command_bytes = command.encode("ascii")
    checksum = CRC_XMODEM(command_bytes)
    return command_bytes + checksum.to_bytes(2, "big") + b"\r"


def parse_response(raw):
    """Extrae el payload de una respuesta Voltronic sin eliminar datos a ciegas."""
    if not raw:
        return ""
    start = raw.find(b"(")
    end = raw.find(b"\r", start + 1) if start >= 0 else -1
    if start < 0 or end < 0:
        return ""
    body = raw[start + 1:end]
    if len(body) >= 2:
        body = body[:-2]
    try:
        return body.decode("ascii", errors="strict").strip()
    except UnicodeDecodeError:
        return body.decode("latin1", errors="replace").strip()


# ---------------- MQTT ----------------
client = None
mqtt_publish_lock = threading.Lock()


def connect_mqtt():
    global client
    server = os.environ.get("MQTT_SERVER", "core-mosquitto")
    port = env_int("MQTT_PORT", 1883)
    client_id = os.environ.get(
        "MQTT_CLIENT_ID", f"axpert-ha-{random.randint(1000, 9999)}"
    )
    username = os.environ.get("MQTT_USER", "")
    password = os.environ.get("MQTT_PASS", "")

    logger.info("Conectando a MQTT en %s:%d", server, port)
    client = mqtt.Client(client_id=client_id)
    if username:
        client.username_pw_set(username, password)
    client.connect(server, port=port, keepalive=60)
    client.loop_start()
    logger.info("Cliente MQTT conectado; publicador en marcha.")


def send_data(data, topic):
    if client is None or not data or not topic:
        return False
    try:
        payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        result = client.publish(topic, payload, qos=0, retain=True)
        return result.rc == mqtt.MQTT_ERR_SUCCESS
    except Exception:
        logger.exception("Error publicando en MQTT, tópico %s", topic)
        return False


# ---------------- COMUNICACIÓN SERIE ----------------
class Inversor:
    """
    Consulta serie para protocolo Axpert/Voltronic.
    Estrategia por defecto: CRC primero; para QPIGS se admite respuesta válida
    tanto a la consulta con CRC como a la consulta ASCII simple.
    """

    def __init__(self, port, baudrate=2400, timeout=SERIAL_TIMEOUT):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.ser = None
        self.lock = threading.Lock()
        self._connect()

    def _connect(self):
        if self.ser and self.ser.is_open:
            return True
        try:
            self.ser = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=self.timeout,
                write_timeout=self.timeout,
                xonxoff=False,
                rtscts=False,
                dsrdtr=False,
            )
            logger.info(
                "Puerto serie abierto: %s (%d baudios, 8N1)",
                self.port,
                self.baudrate,
            )
            return True
        except Exception:
            logger.exception("No se pudo abrir el puerto serie %s", self.port)
            self.ser = None
            return False

    def close(self):
        if self.ser:
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None

    def _read_frame(self, timeout):
        deadline = time.monotonic() + timeout
        raw = bytearray()
        while time.monotonic() < deadline:
            if self.ser.in_waiting:
                chunk = self.ser.read(self.ser.in_waiting)
                raw.extend(chunk)
                if b"\r" in raw:
                    return bytes(raw[: raw.index(b"\r") + 1])
            else:
                time.sleep(0.01)
        return bytes(raw)

    def _query_once(self, command, with_crc):
        if not self._connect():
            return b""
        frame = build_crc_frame(command) if with_crc else command.encode("ascii") + b"\r"
        try:
            # Descartar cualquier respuesta vieja antes de enviar la nueva orden.
            self.ser.reset_input_buffer()
            self.ser.write(frame)
            self.ser.flush()
            return self._read_frame(self.timeout + 1.0)
        except (serial.SerialException, OSError):
            logger.exception("Error serie enviando %s", command)
            self.close()
            return b""

    def query(self, command):
        """
        Política específica basada en las pruebas:
        - QPIGS: CRC primero; si es NAK, prueba ASCII simple.
        - QPGS0 y QPIRI: CRC, que es el formato que se ha visto funcionar.
        - QPIGS2: CRC primero, porque el ensayo ASCII devolvió datos que podrían
          ser una respuesta atrasada; no se confía en respuestas sin validar.
        - Otros comandos: CRC primero.
        """
        with self.lock:
            modes = [True, False] if command in ("QPIGS", "QPIGS2") else [True]
            last_raw = b""
            for with_crc in modes:
                raw = self._query_once(command, with_crc)
                last_raw = raw
                payload = parse_response(raw)
                if not payload:
                    logger.warning(
                        "%s sin respuesta completa (%s CRC); RX=%r",
                        command,
                        "con" if with_crc else "sin",
                        raw,
                    )
                    continue
                if payload.startswith("NAK"):
                    logger.warning(
                        "%s rechazado (%s CRC): %r",
                        command,
                        "con" if with_crc else "sin",
                        payload,
                    )
                    continue
                logger.debug(
                    "%s respondió (%s CRC): %s",
                    command,
                    "con" if with_crc else "sin",
                    payload,
                )
                return payload
            logger.warning("%s falló tras los intentos; última RX=%r", command, last_raw)
            return ""


# ---------------- PARSERS ----------------
def parse_qpigs(response):
    nums = response.split()
    if len(nums) < 17:
        return {}
    data = {
        "BusVoltage": safe_number(nums[7]),
        "InverterHeatsinkTemperature": safe_number(nums[11]),
        "BatteryVoltageFromScc": safe_number(nums[14]),
        "PvInputCurrent": safe_number(nums[12]),
        "PvInputVoltage": safe_number(nums[13]),
        "PvInputPower": (
            safe_number(nums[19])
            if len(nums) > 19
            else round(safe_number(nums[12]) * safe_number(nums[13]), 1)
        ),
        "BatteryChargingCurrent": safe_number(nums[9]),
        "BatteryDischargeCurrent": safe_number(nums[15]),
        "DeviceStatus": nums[16],
    }
    return data


def parse_qpigs2(response):
    parts = response.split()
    if len(parts) < 3:
        return {}
    first = safe_number(parts[0])
    second = safe_number(parts[1])
    pv2_p = safe_number(parts[2])

    # Algunos firmwares/consultas exponen corriente y tensión en distinto orden.
    if first > 50 and second < 50:
        pv2_v, pv2_i = first, second
    else:
        pv2_i, pv2_v = first, second

    if pv2_p <= 0 and pv2_i > 0 and pv2_v > 0:
        pv2_p = round(pv2_v * pv2_i, 1)
    return {
        "Pv2InputCurrent": pv2_i,
        "Pv2InputVoltage": pv2_v,
        "Pv2InputPower": pv2_p,
    }


def parse_qpgs0(response):
    nums = response.split()
    if len(nums) < 26:
        return {}
    return {
        "Gridmode": 1 if nums[2] == "L" else 0,
        "SerialNumber": safe_number(nums[1]),
        "BatteryChargingCurrent": safe_number(nums[12]),
        "BatteryDischargeCurrent": safe_number(nums[26]) if len(nums) > 26 else 0,
        "TotalChargingCurrent": safe_number(nums[15]),
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
        "OutputMode": safe_number(nums[20]),
        "Solarmode": 1 if nums[2] == "B" else 0,
    }


def parse_qpiri(response):
    nums = response.split()
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
        "MaxChargingCurrent": safe_number(nums[14]),
    }


def parse_qpi(response):
    return response if response else ""


def parse_qid(response):
    return response if response else ""


# ---------------- BUCLE PRINCIPAL ----------------
def main():
    serial_port = os.environ.get("DEVICE", os.environ.get("SERIAL_PORT", DEFAULT_SERIAL_PORT))
    baudrate = env_int("BAUDRATE", DEFAULT_BAUDRATE)
    poll_interval = env_int("POLL_INTERVAL", env_int("UPDATE_TIME", DEFAULT_POLL_INTERVAL))
    settings_interval = env_int("SETTINGS_INTERVAL", DEFAULT_SETTINGS_INTERVAL)

    topic_base = os.environ.get("MQTT_TOPIC", "power/axpert{sn}")
    topic_parallel = os.environ.get("MQTT_TOPIC_PARALLEL", "power/axpert")
    topic_settings = os.environ.get("MQTT_TOPIC_SETTINGS", "power/axpert_settings")
    topic_health = os.environ.get("MQTT_HEALTHCHECK", "axpert/healthCheck")

    sn = os.environ.get("INVERTER_SERIAL", "96342210104295")

    connect_mqtt()
    inverter = Inversor(serial_port, baudrate=baudrate)

    last_settings = 0.0
    last_health_log = 0.0

    logger.info(
        "Monitor iniciado. Puerto=%s, baudios=%d, polling=%ds, settings=%ds",
        serial_port, baudrate, poll_interval, settings_interval
    )

    try:
        while True:
            cycle_started = time.monotonic()
            valid_telemetry = False

            try:
                # QPIGS: métricas principales
                response = inverter.query("QPIGS")
                data = parse_qpigs(response)
                if data:
                    send_data(data, topic_base.replace("{sn}", sn))
                    valid_telemetry = True

                # QPIGS2: segundo MPPT. Mantener la consulta separada.
                response2 = inverter.query("QPIGS2")
                data2 = parse_qpigs2(response2)
                if data2:
                    send_data(data2, topic_base.replace("{sn}", sn + "_pv2"))
                    valid_telemetry = True

                # QPGS0: datos paralelos y salida.
                response_parallel = inverter.query("QPGS0")
                data_parallel = parse_qpgs0(response_parallel)
                if data_parallel:
                    send_data(data_parallel, topic_parallel)
                    valid_telemetry = True

                # Configuración menos frecuente.
                now_mono = time.monotonic()
                if now_mono - last_settings >= settings_interval:
                    response_settings = inverter.query("QPIRI")
                    settings = parse_qpiri(response_settings)
                    if settings:
                        send_data(settings, topic_settings)
                    last_settings = now_mono

                send_data(
                    {"Health": "OK" if valid_telemetry else "NO OK"},
                    topic_health,
                )
                if not valid_telemetry and time.monotonic() - last_health_log > 30:
                    logger.error("No se ha recibido telemetría válida en este ciclo.")
                    last_health_log = time.monotonic()
                
            except Exception:
                logger.exception("Error en el ciclo de monitorización")
                send_data({"Health": "NO OK"}, topic_health)

            elapsed = time.monotonic() - cycle_started
            time.sleep(max(0.1, poll_interval - elapsed))
    finally:
        inverter.close()
        if client is not None:
            client.loop_stop()
            client.disconnect()


if __name__ == "__main__":
    main()
