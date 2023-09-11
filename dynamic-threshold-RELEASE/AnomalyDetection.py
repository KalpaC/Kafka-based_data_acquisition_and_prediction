# AnomalyDetection 2023/5/17 20:26
import logging
import pandas as pd
import IO_API
import json
import os

# 基本配置
reader_config = './configs/detector-reader.json'
writer_config = './configs/detector-writer.json'

logger = logging.getLogger('detector')
log_dir = './detector-log'
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

logger.info("===================Anomaly Detector Start======================")
detector_config = './configs/detector.json'
with open(detector_config, 'r', encoding='utf-8') as f:
    config = json.load(f)
    max_diff = config['max_diff']
    period = pd.Timedelta(config['valid-period'])
    max_threshold = config['max_threshold']
load_config_time = pd.Timestamp.now()


def refresh_config():
    global config, max_diff, period, max_threshold
    with open(detector_config, 'r', encoding='utf-8') as f:
        new_config = json.load(f)
        try:
            max_diff = new_config['max_diff']
            period = pd.Timedelta(new_config['valid-period'])
            max_threshold = new_config['max_threshold']
        except KeyError as e:
            logger.error("Lack of importance key in new config file: %s" % str(e))
        else:
            config = new_config


kr = IO_API.KafkaReader(reader_config, period)
ew = IO_API.ExceptionWriter(writer_config)
devices = set()
logger.info(
    "Detector config: %s, reader config: %s, writer config: %s" % (detector_config, reader_config, writer_config))

for new_data_for_devices in kr.timing_records(logger):
    items = []
    text_dict = {}
    drop = []
    now = pd.Timestamp.now()
    # 刷新配置
    if now - load_config_time > pd.Timedelta('5s'):
        refresh_config()
        load_config_time = now
    for item in new_data_for_devices.items():
        if item[0] in devices and item[1] is None:
            # 即设备掉线，出现异常
            drop.append(item[0])
            devices.remove(item[0])
        if item[1] is not None:
            items.append(item)
    if len(items) == 0:
        continue

    # 检测是否超出静态阈值
    max_record = {}
    for device, data in items:
        for col in max_threshold:
            if data[col] > max_threshold[col]:
                if col not in max_record:
                    max_record[col] = []
                max_record[col].append(device)
    for col in max_record:
        if col not in text_dict:
            text_dict[col] = ""
        text_dict[col] = "Single exception occur in: " + str(
            [(device, new_data_for_devices[device][col]) for device in max_record[col]]) + "\n"

    diff_record = {}
    # 检测设备间是否异常
    for i in range(len(items)):
        d = items[i][0]
        attr = items[i][1]
        for j in range(i + 1, len(items)):
            o_d = items[j][0]
            o_attr = items[j][1]
            for col in max_diff:
                if abs(attr[col] - o_attr[col]) > max_diff[col]:
                    if col not in diff_record:
                        diff_record[col] = {}
                    if d not in diff_record[col]:
                        diff_record[col][d] = []
                    diff_record[col][d].append(o_d)

    if len(drop) != 0:
        text_dict['other'] = "Device(s) dropped: " + str(drop)

    timestamp = items[0][1]['timestamp']
    for col in diff_record:
        if col not in text_dict:
            text_dict[col] = ""
        info = "Group exception exist in device group(s): "
        for j in diff_record[col]:
            info += str([j] + diff_record[col][j]) + "  "
        text_dict[col] = info + "\n"
        print(info)
        logger.info("%s %s, info: %s" % (timestamp, col, info))
    if len(drop) + len(max_record) + len(diff_record) != 0:
        ew.save_to_sql(text_dict, timestamp)
