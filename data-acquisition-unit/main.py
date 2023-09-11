# 作者 Ajex
# 创建时间 2023/4/10 19:28
# 文件名 main.py
import json

from kafka import KafkaProducer, errors
from apscheduler.schedulers.blocking import BlockingScheduler
from sensor_device import *
import logging

import cmcc_onenet_api
import device_config


class Env:
    def __init__(self, fp):
        with open(fp, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        self.lastTime = {}
        self.logger = logging.getLogger('DAU')
        self.logger_init()

    def logger_init(self):
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        # file_handler = logging.FileHandler(self.log_file(),encoding='utf-8')
        # file_handler.setFormatter(formatter)
        # self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def is_duplicated(self, d: SensorDevice):
        if d.id not in self.lastTime:
            self.lastTime[d.id] = d.timestamp
        elif self.lastTime[d.id] == d.timestamp:
            return True
        self.lastTime[d.id] = d.timestamp
        return False


    def kafka_server(self):
        return self.config['kafka']['kafka_server']

    def kafka_topic(self):
        return self.config['kafka']['kafka_topic']

    def topic_key(self):
        return self.config['kafka']['topic_key']

    def log_file(self):
        return self.config['log']['log_file']

    def job_interval(self):
        return self.config['parameter']['job_interval']

    def future_timeout(self):
        return self.config['parameter']['future_timeout']

    def onenet_get_timeout(self):
        return self.config['parameter']['onenet_get_timeout']


env = Env('basic-config.json')


def main_job():
    # Initial Kafka producer.
    try:
        producer = KafkaProducer(bootstrap_servers=[env.kafka_server()])
    except errors.KafkaError as e:
        env.logger.critical('Fail to connect to Kafka Server.')
        return
    # Load one-net devices.
    devices: list[SensorDevice] = device_config.get_devices_from_xml('./device-config.xml')
    for d in devices:
        try:
            cmcc_onenet_api.get_device_payload(d, env.onenet_get_timeout())
        except TimeoutError as e:
            env.logger.warning('GET timeout.')
        if env.is_duplicated(d):
            continue
        env.logger.info('GET new record. Device:' + str(d))
        try:
            future = producer.send(env.kafka_topic(), str(d).encode('utf-8'), key=env.topic_key().encode('utf-8'))
            future.get(timeout=env.future_timeout())
            env.logger.info('Send successfully. Device:' + str(d))
        except errors.KafkaError as e:
            env.logger.warning('Send FAILED. %s' % str(e))
    producer.flush()
    producer.close()


def main():
    env.logger.info(
        'DAU START REUNNING. Parameters: {"job_interval":%ds, "future_timeout":%ds, onenet_get_timeout:%ds}' % (
            env.job_interval(), env.future_timeout(), env.onenet_get_timeout()))
    scheduler = BlockingScheduler()
    scheduler.add_job(main_job, 'interval', seconds=env.job_interval())
    scheduler.start()


if __name__ == '__main__':
    main()
