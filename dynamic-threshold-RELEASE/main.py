# final 2023/5/12 17:10
import logging
import pandas as pd
import IO_API
import Processor
import env


def main():
    data = {}  # 缓存存储设备数据
    need_to_predict = {}  # 设备是否需要预测的标记
    predict_error_cnt = {}  # 连续预测出现异常的计数器
    steps = env.get_predict_steps()  # 读取单次预测步数
    basic_data_amount = env.get_basic_data_amount()  # 多少数据才能开始预测
    kr = IO_API.KafkaReader(env.get_config_path('reader-config.json'), env.get_period())  # kafkaReader初始化
    pds = IO_API.PredictDataSaver(env.get_config_path("config-for-pds.json"))  # 存储预测数据的mysql writer
    wds = IO_API.WashedDataSaver(env.get_config_path("config-for-wds.json"))  # 存储清洗后数据的mysql writer
    buff_size = env.get_buff_size()  # 设备数据的缓冲区上限，避免内存溢出，减少预测所需时间
    logger = logging.getLogger('dynamic-threshold')
    for new_data_for_devices in kr.timing_records(logger):
        # 理论上可以使用该生成函数不断获取数据，而不会被异常中断，除非最初就发现kafka服务器断线，否则会不断尝试重连
        for device in new_data_for_devices:
            # None意味着设备断联超过一段时间，过往的数据作废（如果有的话）
            if new_data_for_devices[device] is None:
                if device in data:
                    # 如果是无效数据，说明已经很久没有有效数据了，之前的数据作废
                    data.pop(device)
                    need_to_predict.pop(device)
                    predict_error_cnt.pop(device)
                continue
            # 新设备
            if device not in data:
                data[device] = [new_data_for_devices[device]]
                wds.save_to_mysql(new_data_for_devices[device], device)
                need_to_predict[device] = True
                predict_error_cnt[device] = 0
            # 已有设备
            else:
                # 如果时间戳小于等于上次，说明重复
                this = pd.Timestamp(new_data_for_devices[device]['timestamp'])
                last = pd.Timestamp(data[device][-1]['timestamp'])
                if this <= last:
                    continue
                # 检测与上次的间隔，避免由于中途数据清洗组件重启造成的问题。
                if this - last >= pd.Timedelta('60s'):
                    # 抛弃旧数据
                    data[device] = [new_data_for_devices[device]]
                    wds.save_to_mysql(new_data_for_devices[device], device)
                    need_to_predict[device] = True
                    predict_error_cnt[device] = 0
                    continue
                # 正常存入数据
                wds.save_to_mysql(new_data_for_devices[device], device)
                data[device].append(new_data_for_devices[device])
                need_to_predict[device] = True
                l = len(data[device])
                # 满足预测条件，进行预测
                if l >= basic_data_amount and l % steps == 0 and need_to_predict[device]:
                    df = pd.DataFrame(data[device])  # 将list[dict]数据转化为dataframe方便处理
                    df.index = pd.DatetimeIndex(df['timestamp'])  # 将时间戳列转化为索引
                    df.drop('timestamp', axis=1, inplace=True)  # 删除时间戳列以为预测做准备
                    psr = Processor.Processor(df)  # 放入processor
                    # 预测，注意如果没有正确使用ceemdan+arima预测，则会通过第二个返回值返回错误
                    predict, have_error = psr.predict_with_CEEMDAN(steps)
                    # 如果运算出现错误，则累计。如果错误不连续则清零
                    if have_error:
                        predict_error_cnt[device] += 1
                    else:
                        predict_error_cnt[device] = 0
                    env.get_upper_bound(predict)  # 计算得到阈值上界
                    predict['timestamp'] = predict.index  # 将时间戳放回df中
                    pds.save_to_mysql(device, predict)  # 写入mysql
                    logger.info(
                        "Write predict device:%s data from %s to %s" % (
                            device, str(predict['timestamp'][0]), str(predict['timestamp'][-1])
                        )
                    )
                    need_to_predict[device] = False
                if l > buff_size:
                    # 为了不影响预测step计算
                    data[device] = data[device][-(basic_data_amount + l % steps):]
                    logger.info("Clean buffer")
                # 连续发生4次错误则清理数据
                if env.need_to_check():
                    if predict_error_cnt[device] >= 4:
                        data[device] = data[device][-(basic_data_amount + l % steps):]
                        logger.warning("Continuous errors calculation exceptions")
                    # 连续发生10次则全部清除
                    if predict_error_cnt[device] >= 10:
                        data.pop(device)
                        need_to_predict.pop(device)
                        predict_error_cnt.pop(device)
                        logger.error("To much calculation exceptions, clear data to fix it")


if __name__ == '__main__':
    main()
