#!/usr/bin/env python
# -*- coding: utf-8 -*-
# time: 2022/10/11 20:29
# file: device_config.py.py
# author: Yusheng Wang
# email: yasenwang@bupt.edu.cn
import xml.sax

from sensor_device import SensorDevice


class DeviceConfigHandler(xml.sax.handler.ContentHandler):
    def __init__(self):
        super().__init__()
        self.device: SensorDevice = None
        self.current_name = None
        self.current_attrs = None
        self.devices = []

    def startElement(self, name, attrs):
        self.current_name = name
        self.current_attrs = attrs
        if name == 'device':
            self.device = SensorDevice()

    def characters(self, content):
        if self.current_name == 'device-name':
            self.device.name = content
        if self.current_name == 'device-id':
            self.device.id = content
        if self.current_name == 'device-key':
            self.device.key = content

    def endElement(self, name):
        if name == 'device':
            self.devices.append(self.device)
        self.current_name = None
        self.current_attrs = None


def get_devices_from_xml(filename):
    parser = xml.sax.make_parser()
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)
    handler = DeviceConfigHandler()
    parser.setContentHandler(handler)
    parser.parse(filename)
    return handler.devices


if __name__ == '__main__':
    devices = get_devices_from_xml("device-config.xml")
    for device in devices:
        print(device)
