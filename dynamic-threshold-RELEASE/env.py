# 作者 Ajex
# 创建时间 2023/4/20 21:14
# 文件名 env.py
import json
import logging
import pandas as pd
import os
from apscheduler.schedulers.blocking import BlockingScheduler
import threading

# 读入环境配置文件
config_dir = 'configs'
log_dir = './log'
env_config_name = 'env-config.json'
env_config = os.path.join(config_dir, env_config_name)
try:
    with open(env_config, 'r', encoding="utf-8") as f:
        config = json.load(f)
except FileNotFoundError:
    raise FileNotFoundError("File 'env-config.json' is not found\n")

# 创建动态阈值日志
logger = logging.getLogger('dynamic-threshold')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)
now = str(pd.Timestamp.now().date())
if not os.path.exists(log_dir):
    os.mkdir(log_dir)
log_file = os.path.join(log_dir, now + '.log')
f = logging.FileHandler(log_file, encoding='utf-8')
f.setFormatter(formatter)
logger.addHandler(f)
c = logging.StreamHandler()
c.setFormatter(formatter)
logger.addHandler(c)

logger.info("====Fish-keeper dynamic-threshold unit and washed data saver unit start!====")
logger.info("config dir: %s, log dir: %s, env config path: %s" % (config_dir, log_dir, env_config))


def refresh_config():
    global config
    try:
        with open(env_config, 'r', encoding="utf-8") as f:
            new_config = json.load(f)
            for key in config:
                if key not in new_config:
                    logger.error("Removing key from config file might do damage to this program")
                    break
            else:
                config = new_config
    except FileNotFoundError:
        raise FileNotFoundError("File 'env-config.json' is not found\n")
    except Exception as e:
        return


def schedule():
    scheduler = BlockingScheduler()
    scheduler.add_job(refresh_config, 'interval', seconds=5)
    scheduler.start()


# 定时检测参数更新
schedule_thread = threading.Thread(name='env', target=schedule)
schedule_thread.start()


def get_predict_steps():
    return config['steps']


def get_basic_data_amount():
    return get_predict_steps() * 5


def get_buff_size():
    return config['buff-size']


def get_upper_bound(df):
    tolerate = config['tolerate']
    for col in tolerate:
        try:
            df[col] += tolerate[col]
        except Exception as e:
            logger.info("Calculation error: %s" % str(e))
    return df


def get_config_path(config_file_name):
    return os.path.join(config_dir, config_file_name)


def get_period():
    return pd.Timedelta(config['valid-period'])


def need_to_check():
    return config['calculation_error_detect']
