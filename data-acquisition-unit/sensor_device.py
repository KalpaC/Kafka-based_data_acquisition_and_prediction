# 不用!/usr/bin/env python
# -*- coding: utf-8 -*-
# time: 2022/10/11 16:52
# file: sensor_device.py
# author: Yusheng Wang
# email: yasenwang@bupt.edu.cn
import json


class SensorDevice:
    def __init__(self):
        self.id = None
        self.key = None
        self.name = 'Default'
        self.timestamp = ""
        self.humidity = 0.0
        self.temperature = 0.0
        self.light = 0.0
        self.TDS = 0.0
        self.waterTemperature = 0.0

    def set_light(self, light):
        self.light = light
        return self

    def set_temperature(self, temperature):
        self.temperature = temperature
        return self

    def set_humidity(self, humidity):
        self.humidity = humidity
        return self

    def set_TDS(self, TDS):
        self.TDS = TDS

    def set_waterTemperature(self,waterTemperature):
        self.waterTemperature = waterTemperature
        return self

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return json.dumps({"name": self.name,
                           "id": self.id,
                           "timestamp": self.timestamp,
                           "light": self.light,
                           "temperature": self.temperature,
                           "humidity": self.humidity,
                           "TDS": self.TDS,
                           "waterTemperature":self.waterTemperature
                           }, ensure_ascii=False)

