# onenet_data 2023/1/9 19:14

from sensor_device import *
import cmcc_onenet_api
import device_config
import time
import csv

f = open('DeviceData.csv', 'a+',newline='')
writer = csv.writer(f)
writer.writerow(['timestamp']+cmcc_onenet_api.DataNames)
lastTime = {}
while True:
    devices: list[SensorDevice] = device_config.get_devices_from_xml('device-config.xml')
    for d in devices:
        cmcc_onenet_api.get_device_payload(d)
        if d.id not in lastTime:
            lastTime[d.id] = d.timestamp
        elif lastTime[d.id] == d.timestamp:
            continue
        lastTime[d.id] = d.timestamp
        l = [d.timestamp, d.temperature, d.humidity, d.light, d.TDS, d.waterTemperature]
        writer.writerow(l)
        print(d.name, l)

    # 3/30
    # 待解决的问题：
    # 1. 可拓展性太差，加数据需要直接改代码，显然不合理，所以实际将onenet数据推到kafka的代码还要重构
    # 2. 数据格式可能不对
    # 3. 设备之间的数据关系如何处理没有确定
    # 4. 这一层具体的需求不确定，数据清洗那边都要做什么还没分好锅
    f.flush()
    time.sleep(5)
