#!/usr/bin/env python
# -*- coding: utf-8 -*-
# time: 2022/10/11 16:40
# file: cmcc_onenet_api.py.py
# author: Yusheng Wang
# email: yasenwang@bupt.edu.cn
import json
import requests

from sensor_device import SensorDevice

DataNames = ['Temperature','Humidity','Light','TDS','Watertemperature']

def get_name_string():
    return ','.join(DataNames)


def get_device_data(device_id, api_key):
    url = f'https://api.heclouds.com/devices/{device_id}'
    headers = {
        'api-key': f'{api_key}'
    }
    response = requests.get(url, headers=headers)
    return response.text


def get_device_payload(device: SensorDevice, timeout: int):

    url = f'https://api.heclouds.com/devices/{device.id}/datapoints?datastream_id=' + get_name_string() + '&limit=1'
    headers = {
        'api-key': f'{device.key}'
    }
    try:
        response = requests.get(url, headers=headers,timeout=timeout)
    except TimeoutError as e:
        raise e
    datastreams = json.loads(response.text)['data']['datastreams']  # It is type of list.
    device.timestamp = datastreams[0]['datapoints'][0]['at'].split('.')[0]
    for datastream in datastreams:
        if datastream['id'] == 'Temperature':
            device.temperature = datastream['datapoints'][0]['value']
        if datastream['id'] == 'Light':
            device.light = datastream['datapoints'][0]['value']
        if datastream['id'] == 'Humidity':
            device.humidity = datastream['datapoints'][0]['value']
        if datastream['id'] == 'TDS':
            device.TDS = datastream['datapoints'][0]['value']
        if datastream['id'] == 'Watertemperature':
            device.waterTemperature = datastream['datapoints'][0]['value']


if __name__ == '__main__':
    pass
