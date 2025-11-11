import serial
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def safe_number(value):
    """Convierte a número si puede, si no devuelve 0"""
    try:
        return float(value)
    except:
        return 0

class Inversor:
    def __init__(self, port="/dev/ttyUSB0", baudrate=2400, timeout=1):
        self.ser = serial.Serial(port, baudrate=baudrate, timeout=timeout)

    def send_cmd(self, cmd):
        frame = cmd.encode('ascii') + b'\r'
        self.ser.write(frame)
        resp = self.ser.readline()
        return resp.decode('ascii', errors='ignore').strip()

    def parse_qid(self, resp):
        nums = resp.strip('()').split()
        data = '{"SerialNumber":' + str(safe_number(nums[0])) + '}'
        return data

    def parse_qpigs(self, resp):
        nums = resp.strip('()').split()
        data = '{'
        data += '"ACInputVoltage":' + str(safe_number(nums[0]))
        data += ',"ACInputFrequency":' + str(safe_number(nums[1]))
        data += ',"ACOutputVoltage":' + str(safe_number(nums[2]))
        data += ',"ACOutputFrequency":' + str(safe_number(nums[3]))
        data += ',"ACOutputApparentPower":' + str(safe_number(nums[4]))
        data += ',"ACOutputActivePower":' + str(safe_number(nums[5]))
        data += ',"LoadPercentage":' + str(safe_number(nums[6]))
        data += ',"BusVoltage":' + str(safe_number(nums[7]))
        data += ',"BatteryVoltage":' + str(safe_number(nums[8]))
        data += ',"BatteryChargeCurrent":' + str(safe_number(nums[9]))
        data += ',"BatteryCapacity":' + str(safe_number(nums[10]))
        data += ',"PVInputCurrent":' + str(safe_number(nums[11]))
        data += '}'
        return data

    def parse_qpiri(self, resp):
        nums = resp.strip('()').split()
        data = '{'
        data += '"ACNominalVoltage":' + str(safe_number(nums[0]))
        data += ',"ACNominalFrequency":' + str(safe_number(nums[1]))
        data += ',"MaxChargeCurrent":' + str(safe_number(nums[5]))
        data += ',"MaxDischargeCurrent":' + str(safe_number(nums[6]))
        data += ',"BatteryNominalVoltage":' + str(safe_number(nums[7]))
        data += ',"PVInputVoltageMin":' + str(safe_number(nums[8]))
        data += ',"PVInputVoltageMax":' + str(safe_number(nums[9]))
        data += '}'
        return data

    def parse_qpigs2(self, resp):
        nums = resp.strip('()').split()
        data = '{'
        data += '"PVVoltage":' + str(safe_number(nums[0]))
        data += ',"BatterySOC":' + str(safe_number(nums[1]))
        data += ',"BatteryTemperature":' + str(safe_number(nums[2]))
        data += '}'
        return data

    def parse_qpgs0(self, resp):
        nums = resp.strip('()').split()
        data = '{'
        data += '"Gridmode":' + ('1' if nums[2] == 'L' else '0')
        data += ',"SerialNumber":' + str(safe_number(nums[1]))
        data += ',"BatteryChargingCurrent":' + str(safe_number(nums[12]))
        data += ',"BatteryDischargeCurrent":' + str(safe_number(nums[26]))
        data += ',"TotalChargingCurrent":' + str(safe_number(nums[15]))
        data += ',"GridVoltage":' + str(safe_number(nums[4]))
        data += ',"GridFrequency":' + str(safe_number(nums[5]))
        data += ',"OutputVoltage":' + str(safe_number(nums[6]))
        data += ',"OutputFrequency":' + str(safe_number(nums[7]))
        data += ',"OutputAparentPower":' + str(safe_number(nums[8]))
        data += ',"OutputActivePower":' + str(safe_number(nums[9]))
        data += ',"LoadPercentage":' + str(safe_number(nums[10]))
        data += ',"BatteryVoltage":' + str(safe_number(nums[11]))
        data += ',"BatteryCapacity":' + str(safe_number(nums[13]))
        data += ',"PvInputVoltage":' + str(safe_number(nums[14]))
        data += ',"TotalAcOutputApparentPower":' + str(safe_number(nums[16]))
        data += ',"TotalAcOutputActivePower":' + str(safe_number(nums[17]))
        data += ',"TotalAcOutputPercentage":' + str(safe_number(nums[18]))
        data += ',"OutputMode":' + str(safe_number(nums[20]))
        data += ',"ChargerSourcePriority":' + str(safe_number(nums[21]))
        data += ',"MaxChargeCurrent":' + str(safe_number(nums[22]))
        data += ',"MaxChargerRange":' + str(safe_number(nums[23]))
        data += ',"MaxAcChargerCurrent":' + str(safe_number(nums[24]))
        data += ',"PvInputCurrentForBattery":' + str(safe_number(nums[25]))
        data += ',"Solarmode":' + ('1' if nums[2] == 'B' else '0')
        data += '}'
        logger.debug("[QPGS0] payload built")
        return data

    def get_status(self):
        status = {}
        status["QID"] = self.parse_qid(self.send_cmd("QID"))
        status["QPIGS"] = self.parse_qpigs(self.send_cmd("QPIGS"))
        status["QPIRI"] = self.parse_qpiri(self.send_cmd("QPIRI"))
        status["QPIGS2"] = self.parse_qpigs2(self.send_cmd("QPIGS2"))
        status["QPGS0"] = self.parse_qpgs0(self.send_cmd("QPGS0"))
        return status

# --- Uso ---
if __name__ == "__main__":
    inv = Inversor(port="/dev/ttyUSB0")
    status = inv.get_status()
    import json
    print(json.dumps(status, indent=4))
