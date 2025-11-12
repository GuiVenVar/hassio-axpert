#!/usr/bin/env python3
import os
import time
import random
import json
import logging
import serial
import paho.mqtt.client as mqtt

battery_types = {'0': 'AGM', '1': 'Flooded', '2': 'User', '3': 'Lithium' }
voltage_ranges = {'0': 'Appliance', '1': 'UPS'}
output_sources = {'0': 'utility', '1': 'solar', '2': 'battery'}
charger_sources = {'0': 'utility first', '1': 'solar first', '2': 'solar + utility', '3': 'solar only'}
machine_types = {'00': 'Grid tie', '01': 'Off Grid', '10': 'Hybrid'}
topologies = {'0': 'transformerless', '1': 'transformer'}
output_modes = {'0': 'single machine output', '1': 'parallel output', '2': 'Phase 1 of 3 Phase output', '3': 'Phase 2 of 3 Phase output', '4': 'Phase 3 of 3 Phase output'}
pv_ok_conditions = {'0': 'As long as one unit of inverters has connect PV, parallel system will consider PV OK', '1': 'Only All of inverters have connect PV, parallel system will consider PV OK'}
pv_power_balance = {'0': 'PV input max current will be the max charged current', '1': 'PV input max power will be the sum of the max charged power and loads power'}

def map_with_log(table: dict, value: str, label: str) -> str:
    if value in table: return table[value]
    print(f"[get_settings] Valor inesperado en {label}: {value} (claves válidas: {list(table.keys())})")
    return f"{label}_invalid({value})"


# ---------------- Logger ----------------
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "DEBUG"))
logger = logging.getLogger("inverter-monitor")

# ---------------- MQTT client ----------------
client = None

def connect():
    global client
    mqtt_server = os.environ.get("MQTT_SERVER")
    mqtt_client_id = os.environ.get("MQTT_CLIENT_ID", f"inverter-{random.randint(0,9999)}")
    mqtt_user = os.environ.get("MQTT_USER")
    mqtt_pass = os.environ.get("MQTT_PASS")

    if not mqtt_server:
        logger.error("MQTT_SERVER no definido en variables de entorno")
        raise RuntimeError("MQTT_SERVER no definido")

    logger.info("[MQTT] Connecting…")
    client = mqtt.Client(client_id=mqtt_client_id)
    if mqtt_user:
        client.username_pw_set(mqtt_user, mqtt_pass)
    try:
        client.connect(mqtt_server)
        client.loop_start()
        logger.info("[MQTT] Connected to %s", mqtt_server)
    except Exception:
        logger.exception("No se pudo conectar al broker MQTT")
        raise

def send_data(data, topic):
    """
    Publica en MQTT. Acepta dict o string; si es dict hace json.dumps.
    Devuelve 1 en caso de OK, 0 en caso de fallo (compatibilidad con tu código antiguo).
    """
    if client is None:
        logger.error("MQTT client no inicializado")
        return 0

    try:
        payload = data
        if isinstance(data, dict):
            payload = json.dumps(data, ensure_ascii=False)
        elif not isinstance(data, str):
            payload = str(data)

        client.publish(topic, payload, qos=0, retain=True)
        logger.debug("[MQTT] topic=%s bytes=%d payload=%s", topic, len(payload), payload[:200])
        return 1
    except Exception as e:
        logger.exception("[MQTT] publish failed: %s", e)
        return 0

# ---------------- Utilities ----------------
def safe_number(value):
    try:
        return float(value)
    except Exception:
        return 0.0

def sanitize_id(raw):
    """
    Limpia el raw SN y devuelve algo legible. Ajusta según el formato real que devuelva QID.
    """
    if not raw:
        return "unknown"
    # intentar extraer dígitos
    import re
    m = re.search(r'(\d+)', raw)
    if m:
        return m.group(1)
    # si no hay dígitos, devolver raw saneado
    return raw.strip()

# ---------------- Inversor (serial) ----------------
class Inversor:


    def __init__(self, port="/dev/ttyUSB0", baudrate=2400, timeout=1):
        self.port = port
        try:
            self.ser = serial.Serial(port, baudrate=baudrate, timeout=timeout)
            logger.info("Puerto serie abierto en %s", port)
        except Exception:
            logger.exception("No se pudo abrir el puerto serie %s", port)
            raise

    def send_cmd(self, cmd):
        frame = cmd.encode('ascii') + b'\r'
        self.ser.write(frame)
        resp = self.ser.readline()
        return resp.decode('ascii', errors='ignore').strip()


    # Parsers devuelven dict (robustos)
    def parse_qpigs(self, resp):
        """
        Devuelve exactamente los mismos campos que el parseo HID original.
        """
        if not resp:
            return {}

        s = resp.strip()
        if s.startswith('(') and s.endswith(')'):
            s = s[1:-1]

        nums = s.split()
        if len(nums) < 20:
            nums += [''] * (20 - len(nums))

        return {
            "BusVoltage": safe_number(nums[7]),
            "InverterHeatsinkTemperature": safe_number(nums[11]),
            "BatteryVoltageFromScc": safe_number(nums[14]),
            "PvInputCurrent": safe_number(nums[12]),
            "PvInputVoltage": safe_number(nums[13]),
            "PvInputPower": safe_number(nums[19]),
            "BatteryChargingCurrent": safe_number(nums[9]),
            "BatteryDischargeCurrent": safe_number(nums[15]),
            "DeviceStatus": nums[16] if len(nums) > 16 else "",
        }


    def parse_qpigs2(self, resp):
        """
        Devuelve exactamente los mismos campos que get_qpigs2_json:
        Pv2InputCurrent, Pv2InputVoltage y Pv2InputPower.
        """
        if not resp:
            return {}

        # limpiar y dividir la respuesta
        s = resp.strip().strip('()')
        parts = s.split()
        if len(parts) < 3:
            return {}

        # convertir con safe_number (como en tu otro parser)
        pv2_i = safe_number(parts[0])
        pv2_v = safe_number(parts[1])
        pv2_p = safe_number(parts[2])

        # si el inversor devuelve 0 en potencia, la calculamos
        if pv2_p is None or pv2_p <= 0:
            if pv2_i is not None and pv2_v is not None:
                pv2_p = round(pv2_v * pv2_i, 1)
            else:
                pv2_p = None

        return {
            "Pv2InputCurrent": pv2_i,
            "Pv2InputVoltage": pv2_v,
            "Pv2InputPower": pv2_p,
        }

    def parse_qpgs0(self, resp):
        """
        Devuelve un dict con los mismos campos que antes.
        """
        if not resp:
            return {}

        s = resp.strip()
        if s.startswith('(') and s.endswith(')'):
            s = s[1:-1]

        nums = s.split()
        # pad hasta 30 para cubrir índices usados
        if len(nums) < 30:
            nums += [''] * (30 - len(nums))

        return {
            "Gridmode": 1 if (len(nums) > 2 and nums[2] == 'L') else 0,
            "SerialNumber": safe_number(nums[1]),
            "BatteryChargingCurrent": safe_number(nums[12]),
            "BatteryDischargeCurrent": safe_number(nums[26]),
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
            "TotalAcOutputApparentPower": safe_number(nums[16]),
            "TotalAcOutputActivePower": safe_number(nums[17]),
            "TotalAcOutputPercentage": safe_number(nums[18]),
            "OutputMode": safe_number(nums[20]),
            "ChargerSourcePriority": safe_number(nums[21]),
            "MaxChargeCurrent": safe_number(nums[22]),
            "MaxChargerRange": safe_number(nums[23]),
            "MaxAcChargerCurrent": safe_number(nums[24]),
            "PvInputCurrentForBattery": safe_number(nums[25]),
            "Solarmode": 1 if (len(nums) > 2 and nums[2] == 'B') else 0,
        }

    def parse_qpiri(self, resp):
        nums = resp.strip('()').split()
        nums = nums + [''] * 30  # relleno por seguridad
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
            "InputVoltageRange": map_with_log(voltage_ranges, nums[15], "InputVoltageRange"),
            "OutputSourcePriority": map_with_log(output_sources, nums[16], "OutputSourcePriority"),
            "ChargerSourcePriority": map_with_log(charger_sources, nums[17], "ChargerSourcePriority"),
            "MaxParallelUnits": safe_number(nums[18]),
            "MachineType": map_with_log(machine_types, nums[19], "MachineType"),
            "Topology": map_with_log(topologies, nums[20], "Topology"),
            "OutputMode": map_with_log(output_modes, nums[21], "OutputMode"),
            "BatteryRedischargeVoltage": safe_number(nums[22]),
            "PvOkCondition": map_with_log(pv_ok_conditions, nums[23], "PvOkCondition"),
            "PvPowerBalance": map_with_log(pv_power_balance, nums[24], "PvPowerBalance"),
            "MaxBatteryCvChargingTime": safe_number(nums[25]),
        }

# ---------------- Funciones que replican tu código anterior ----------------

def get_data(inv):
    """
    Equivalente a tu antigua función get_data().
    Devuelve dict con QPIGS (u otros campos que quieras agregar).
    """
    try:
        resp = inv.send_cmd("QPIGS")
        parsed = inv.parse_qpigs(resp)
        # si quieres conservar nombre antiguo, encapsulamos en un key
        return {"QPIGS": parsed}
    except Exception:
        logger.exception("get_data() falló")
        return None

def get_qpigs2_json(inv):
    """
    Equivalente a tu antigua get_qpigs2_json()
    """
    try:
        resp = inv.send_cmd("QPIGS2")
        parsed = inv.parse_qpigs2(resp)
        return parsed
    except Exception:
        logger.exception("get_qpigs2_json() falló")
        return None

def get_parallel_data(inv):
    """
    Equivalente a tu antigua get_parallel_data() (QPGS0)
    """
    try:
        resp = inv.send_cmd("QPGS0")
        parsed = inv.parse_qpgs0(resp)
        return parsed
    except Exception:
        logger.exception("get_parallel_data() falló")
        return None

def get_healthcheck(inv, prefer_true='true'):
    """
    Si prefer_true == 'true' intentará lecturas que sólo funcionan con privilegios/paralelo.
    Para mantener compatibilidad con tu flow anterior, soporta 'true'/'false' strings.
    Devuelve dict o None.
    """
    try:
        ts = int(time.time())
        payload = {"status": "ok", "ts": ts}
        # ejemplo de campos: SOC, battery voltage, load
        try:
            resp = inv.send_cmd("QPIGS2")
            payload.update({"qpigs2": inv.parse_qpigs2(resp)})
        except Exception:
            logger.debug("No se pudo leer QPIGS2 para healthcheck")

        try:
            resp = inv.send_cmd("QPIGS")
            parsed = inv.parse_qpigs(resp)
            payload.update({
                "battery_voltage": parsed.get("BatteryVoltage"),
                "load_percentage": parsed.get("LoadPercentage"),
                "ac_output_active_power": parsed.get("ACOutputActivePower"),
            })
        except Exception:
            logger.debug("No se pudo leer QPIGS para healthcheck")

        # si prefer_true == 'true' puedes intentar lecturas más 'completas' (marca/privilegios)
        # aquí no hacemos nada extra por defecto; la lógica de fallback la gestiona el main
        return payload
    except Exception:
        logger.exception("get_healthcheck() falló")
        return None

def get_settings(inv):
    """
    Lee QPIRI y devuelve dict con settings (o None).
    """
    try:
        resp = inv.send_cmd("QPIRI")
        return inv.parse_qpiri(resp)
    except Exception:
        logger.exception("get_settings() falló")
        return None

# ---------------- Main (réplica de tu main original) ----------------
def main():
    # Arranque aleatorio (evitar tormenta)
    time.sleep(random.randint(0, 5))

    # Conectar MQTT
    try:
        connect()
    except Exception:
        logger.error("Fallo al conectar MQTT, saliendo")
        raise SystemExit(1)

    # Abrir puerto serie / inversor
    serial_port = os.environ.get("SERIAL_PORT", "/dev/ttyUSB0")
    try:
        inv = Inversor(port=serial_port)
    except Exception:
        logger.error("No se pudo abrir inversor en %s", serial_port)
        raise SystemExit(1)

    # obtener raw_sn como hacías antes
    try:
        raw_sn = inv.send_cmd('QID')
    except Exception:
        raw_sn = 'unknown'
    sn = sanitize_id(raw_sn.strip())
    logger.info("Reading from inverter %s (raw='%s')", sn, raw_sn)

    # Intervalos (segundos) con env y fallback
    try:
        FAST_INTERVAL = int(os.environ.get("DATA_INTERVAL", 2))
    except ValueError:
        FAST_INTERVAL = 2
    try:
        HEALTH_INTERVAL = int(os.environ.get("HEALTH_INTERVAL", 20))
    except ValueError:
        HEALTH_INTERVAL = 20
    try:
        SLOW_INTERVAL = int(os.environ.get("SETTINGS_INTERVAL", 600))
    except ValueError:
        SLOW_INTERVAL = 600

    logger.info("Intervals → FAST=%ss, HEALTH=%ss, SETTINGS=%ss", FAST_INTERVAL, HEALTH_INTERVAL, SLOW_INTERVAL)

    last_fast = last_health = last_slow = 0.0

    # topics base desde env
    TOPIC_TEMPLATE = os.environ.get('MQTT_TOPIC', 'inverter/{sn}')
    TOPIC_PARALLEL = os.environ.get('MQTT_TOPIC_PARALLEL', 'inverter/parallel')
    TOPIC_SETTINGS = os.environ.get('MQTT_TOPIC_SETTINGS', 'inverter/settings')
    TOPIC_HEALTH = os.environ.get('MQTT_HEALTHCHECK', 'inverter/health')

    try:
        while True:
            nowm = time.monotonic()

            # FAST
            if nowm - last_fast >= FAST_INTERVAL:
                last_fast = nowm
                try:
                    d = get_data(inv)
                    if d:
                        topic = TOPIC_TEMPLATE.replace('{sn}', sn)
                        send_data(d, topic)
                except Exception:
                    logger.exception("Error en ciclo FAST -> get_data")

                try:
                    pv2 = get_qpigs2_json(inv)
                    if pv2:
                        topic2 = TOPIC_TEMPLATE.replace('{sn}', sn + '_pv2')
                        send_data(pv2, topic2)
                except Exception:
                    logger.exception("Error en ciclo FAST -> qpigs2")

                try:
                    d = get_parallel_data(inv)
                    if d:
                        send_data(d, TOPIC_PARALLEL)
                except Exception:
                    logger.exception("Error en ciclo FAST -> parallel (qpgs0)")

            # HEALTH
            if nowm - last_health >= HEALTH_INTERVAL:
                last_health = nowm
                try:
                    # primero intenta 'true'
                    d = None
                    try:
                        d = get_healthcheck(inv, 'true')
                        if d:
                            send_data(d, TOPIC_HEALTH)
                        else:
                            raise Exception("healthcheck true devolvió None")
                    except Exception as e_true:
                        logger.debug("HealthCheck 'true' falló: %s. Intentando 'false'...", e_true)
                        try:
                            d = get_healthcheck(inv, 'false')
                            if d:
                                send_data(d, TOPIC_HEALTH)
                        except Exception:
                            logger.exception("Fallback healthcheck 'false' también falló")
                except Exception:
                    logger.exception("HealthCheck cycle error")

            # SETTINGS (QPIRI)
            if nowm - last_slow >= SLOW_INTERVAL:
                last_slow = nowm
                try:
                    d = get_settings(inv)
                    if d:
                        send_data(d, TOPIC_SETTINGS)
                except Exception:
                    logger.exception("Error en ciclo SETTINGS (QPIRI)")

            time.sleep(0.1)

    except KeyboardInterrupt:
        logger.info("Terminando por teclado")
    except Exception:
        logger.exception("Fallo en el bucle principal")
    finally:
        try:
            if client:
                client.loop_stop()
                client.disconnect()
        except Exception:
            pass
        try:
            inv.ser.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
