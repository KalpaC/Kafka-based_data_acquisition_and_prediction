# WashedDataSaver 2023/5/3 17:13
import json

import pandas as pd
from kafka import KafkaConsumer
import logging
import mysql.connector
import numpy as np
import os


class WashedDataSaver:
    def __init__(self):
        self.default_path = "config-for-wds.json"
        try:
            with open(self.default_path, 'r', encoding="utf-8") as f:
                self.config = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError("File '%s' is not found\n" % self.default_path)

    def main(self):
        logger = logging.getLogger('kafka_mysql')
        log_dir = './log/'
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        file_handler = logging.FileHandler(log_dir + 'wds.log', encoding='utf-8')
        console_handler = logging.StreamHandler()
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.info('====Kafka MySQL WDS consumer START====')
        logger.setLevel('INFO')
        print('====Kafka MySQL WDS consumer START====')
        consumer = KafkaConsumer(self.config['topic'],
                                 bootstrap_servers=[self.config['kafka-server']], group_id=self.config['group-id'],
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=True)
        for message in consumer:
            message = message.value.decode('utf-8')
            packet = json.loads(message)
            logger.info("Receive kafka record:%s" % str(packet))
            self.save_to_mysql(packet)
            consumer.commit()

    def save_to_mysql(self, packet):
        logger = logging.getLogger('kafka_mysql')
        mydb = mysql.connector.connect(
            host=self.config['db-host'],  # 数据库主机地址
            port=self.config['db-port'],  # 数据库端口
            user=self.config['db-username'],  # 数据库用户名
            passwd=self.config['db-password'],  # 数据库密码
            database=self.config['db-name'],  # DB name
            auth_plugin='mysql_native_password'
        )
        my_cursor = mydb.cursor()
        # 数据库名、数据库表名
        cols = str(tuple(self.config['fields'])).replace("\'", "")
        places = ["%s" for i in range(len(self.config['fields']))]
        places = str(tuple(places)).replace("\'", "")
        # 做时区转换，数据库使用的时区为UTC
        packet['timestamp'] = pd.Timestamp(packet['timestamp']) - pd.Timedelta('8h')
        sql = "INSERT INTO %s.%s %sVALUES %s" % (self.config['db-name'], self.config['db-table'], cols, places)
        values = tuple([packet[key] if key in packet else np.NaN for key in self.config['fields']])
        my_cursor.execute(sql, values)
        mydb.commit()
        mydb.close()
        logger.info("Execute SQL successfully:%s" % (sql % values))


if __name__ == '__main__':
    wds = WashedDataSaver()
    wds.main()
