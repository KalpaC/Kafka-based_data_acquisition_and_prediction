# 作者 Ajex
# 创建时间 2023/4/20 21:14
# 文件名 env.py
import json


try:
    with open("env-config.json",'r',encoding="utf-8") as f:
        config = json.load(f)
except FileNotFoundError:
    raise FileNotFoundError("File 'env-config.json' is not found\n")

def get_kafka_topic():
    return config['kafka']['topic']

def get_kafka_server():
    return config['kafka']['server']

def get_kafka_group_id():
    return config['kafka']['group-id']
