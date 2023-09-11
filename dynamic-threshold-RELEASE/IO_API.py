# 作者 Ajex
# 创建时间 2023/4/20 19:01
# 文件名 KafkaReader.py
import json
import time
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer, errors
import mysql.connector


# 定时拉取数据，而非一有数据就拉取，如果没有拉到数据则插入NaN。
# 每次拉取所有可拉取的数据，然后对其所有数据做平均，从而屏蔽设备、时间间隔的干扰


class KafkaReader:
    def __init__(self, jconfig: str, valid_period: pd.Timedelta):
        self.config_path = jconfig
        try:
            with open(jconfig, 'r', encoding="utf-8") as f:
                config_dict = json.load(f)
                self.server = config_dict["server"]
                self.topic = config_dict["topic"]
                self.group_id = config_dict["group-id"]
                self.period = valid_period
        except FileNotFoundError:
            raise FileNotFoundError("File '%s' is not found\n" % jconfig)
        except KeyError:
            raise KeyError("Missing important key in config file\n")

    def get_begin_time(self):
        return pd.Timestamp.now() - self.period

    def check_format(self, packet):
        if 'timestamp' in packet and 'records' in packet:
            records = packet['records']
            for device in records:
                if 'valid' in records[device]:
                    if records[device]['valid']:
                        if not all(x in records[device].keys() for x in
                                   ("TDS", "light", "temperature", "humidity", "waterTemperature")):
                            return False
                else:
                    return False
        return True

    def timing_records(self, logger):
        consumer = KafkaConsumer(self.topic,
                                 bootstrap_servers=[self.server], group_id=self.group_id,
                                 auto_offset_reset='earliest')
        logger.info(
            "Begin to poll records from sever: %s, topic: %s, group-id: %s, config path: %s"
            % (self.server, self.topic, self.group_id, self.config_path))
        kafka_error_occur = False
        commit_cnt = 0
        while True:
            try:
                if kafka_error_occur:
                    consumer = KafkaConsumer(self.topic,
                                             bootstrap_servers=[self.server], group_id=self.group_id,
                                             auto_offset_reset='earliest')
                for message in consumer:
                    message = message.value.decode('utf-8')
                    packet = json.loads(message)
                    ok = self.check_format(packet)
                    if not ok:
                        print(packet)
                        logger.warning("Washing unit have a dangerous error: wrong format of record")
                        continue
                    timestamp = packet['timestamp']
                    if pd.Timestamp(timestamp) >= self.get_begin_time():
                        records = packet['records']
                        row_dict = {}
                        for device in records:
                            record = records[device]
                            try:
                                if (not record['valid']) or record['valid'] == 0:
                                    row_dict[device] = None
                                else:
                                    record.pop('valid')
                                    record['timestamp'] = timestamp
                                    row_dict[device] = record
                            except Exception:
                                logger.warning("Unknown error occur, try again")
                                continue
                        logger.info("Receive data: %s" % str(row_dict))
                        yield row_dict
                    commit_cnt = (commit_cnt + 1) % 5
                    if commit_cnt == 0:
                        consumer.commit()
            except errors.KafkaError:
                logger.warning("Kafka Server error, try again after 5 seconds")
                kafka_error_occur = True
                time.sleep(5)
            except Exception:
                logger.warning("Unknown error occur, try again")
                time.sleep(5)


class PredictDataSaver:
    def __init__(self, jconfig: str = None):
        self.default_path = "config-for-pds.json"
        if jconfig:
            self.default_path = jconfig
        try:
            with open(self.default_path, 'r', encoding="utf-8") as f:
                self.config = json.load(f)
                self.host = self.config['db-host']
                self.port = self.config['db-port']
                self.user = self.config['db-username']
                self.passwd = self.config['db-password']
                self.db_name = self.config['db-name']
                self.db_table = self.config['db-table']
        except FileNotFoundError:
            raise FileNotFoundError("File '%s' is not found\n" % self.default_path)
        except KeyError:
            raise KeyError("Missing important key in config file\n")

    def save_to_mysql(self, device, dataframe: pd.DataFrame):
        mydb = mysql.connector.connect(
            host=self.host,  # 数据库主机地址
            port=self.port,  # 数据库端口
            user=self.user,  # 数据库用户名
            passwd=self.passwd,  # 数据库密码
            database=self.db_name,  # DB name
            auth_plugin='mysql_native_password'
        )
        my_cursor = mydb.cursor()
        # 做时区转换，数据库使用的时区为UTC
        dataframe = dataframe.copy()
        dataframe['timestamp'] -= pd.Timedelta('8h')
        keys = tuple(dataframe.keys().tolist() + ['name'])
        cols = str(keys).replace('\'', '')
        places = ['%s' for i in range(len(keys))]
        places = str(tuple(places)).replace('\'', '')
        sql = 'INSERT %s.%s %s VALUES %s' % (self.db_name, self.db_table, cols, places)
        rows = []
        for i in range(len(dataframe)):
            row = dataframe.iloc[i].tolist()
            row.append(device)
            rows.append(row)
        my_cursor.executemany(sql, rows)
        mydb.commit()
        mydb.close()


class WashedDataSaver:
    def __init__(self, jconfig):
        self.path = jconfig
        try:
            with open(self.path, 'r', encoding="utf-8") as f:
                self.config = json.load(f)
                self.host = self.config['db-host']  # 数据库主机地址
                self.port = self.config['db-port']  # 数据库端口
                self.user = self.config['db-username']  # 数据库用户名
                self.passwd = self.config['db-password']  # 数据库密码
                self.database = self.config['db-name']  # DB name
                self.table = self.config['db-table']
        except FileNotFoundError:
            raise FileNotFoundError("File '%s' is not found\n" % self.path)
        except KeyError:
            raise KeyError("Missing important key in config file\n")

    def save_to_mysql(self, packet, device):
        mydb = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            database=self.database,
            auth_plugin='mysql_native_password'
        )
        my_cursor = mydb.cursor()
        # 数据库名、数据库表名
        packet = packet.copy()
        packet['name'] = device
        cols = str(tuple(packet.keys())).replace("\'", "")
        places = ["%s" for _ in range(len(packet.keys()))]
        places = str(tuple(places)).replace("\'", "")
        # 做时区转换，数据库使用的时区为UTC
        packet['timestamp'] = pd.Timestamp(packet['timestamp']) - pd.Timedelta('8h')
        sql = "INSERT INTO %s.%s %sVALUES %s" % (self.database, self.table, cols, places)
        values = tuple(packet.values())
        my_cursor.execute(sql, values)
        mydb.commit()
        mydb.close()


class ExceptionWriter:
    def __init__(self, jconfig):
        try:
            with open(jconfig, 'r', encoding="utf-8") as f:
                self.config = json.load(f)
                self.host = self.config['db-host']  # 数据库主机地址
                self.port = self.config['db-port']  # 数据库端口
                self.user = self.config['db-username']  # 数据库用户名
                self.passwd = self.config['db-password']  # 数据库密码
                self.database = self.config['db-name']  # DB name
                self.table = self.config['db-table']
        except FileNotFoundError:
            raise FileNotFoundError("File '%s' is not found\n" % jconfig)
        except KeyError:
            raise KeyError("Missing important key in config file\n")

    def save_to_sql(self, text_dict: dict, timestamp):
        mydb = mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            database=self.database,
            auth_plugin='mysql_native_password'
        )
        my_cursor = mydb.cursor()
        text_dict['timestamp'] = pd.Timestamp(timestamp) - pd.Timedelta('8h')
        cols = str(tuple(text_dict.keys())).replace("\'", "")
        places = ["%s" for _ in range(len(text_dict.keys()))]
        places = str(tuple(places)).replace("\'", "")
        sql = "INSERT INTO %s.%s %sVALUES %s" % (self.database, self.table, cols, places)
        values = tuple(text_dict.values())
        my_cursor.execute(sql, values)
        mydb.commit()
        mydb.close()
