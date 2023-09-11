# CIMFS 2023/3/20 19:29
import numpy as np
import pandas as pd
from PyEMD import CEEMDAN
import pmdarima as pm


class Processor:
    def __init__(self, data: pd.DataFrame):
        """
        接受具有时间戳的等间隔数据
        :param data:
        """
        self.data = data.copy()

    def predict_only_ARIMA(self, steps):
        # 无CEEMDAN，纯ARIMA，可作为对照组
        def forward_index(begin):
            interval = self.data.index[1] - self.data.index[0]
            datetime = self.data.index[begin] + interval
            for i in range(steps):
                yield datetime
                datetime += interval

        df = pd.DataFrame()
        # 整体上是对每一列进行操作的
        for col in self.data:
            series = self.data[col]
            model = pm.auto_arima(series,
                                  stepwise=False,
                                  n_jobs=-1)
            predict = model.predict(steps)
            ans = pd.Series(predict, index=forward_index(-1))
            df[col] = ans
        return df

    def predict_with_CEEMDAN(self, steps: int) -> (pd.DataFrame, bool):
        """
        :param steps: 向后预测的步长个数
        :return: 返回具有时间戳索引的预测数据
        """

        def forward_index(begin):
            interval = self.data.index[1] - self.data.index[0]
            datetime = self.data.index[begin] + interval
            for i in range(steps):
                yield datetime
                datetime += interval

        df = pd.DataFrame()
        # 整体上是对每一列进行操作的
        have_error = False
        for col in self.data:
            # 第一步，进行ceemdan分解
            series = self.data[col]
            cIMFs, res = get_cIMFs(series)
            if np.isnan(cIMFs).any():
                df[col] = pd.Series(np.full(steps, series[-steps:].mean()), index=forward_index(-1))
                have_error = True
                continue
            total = np.zeros(steps)
            for cimf in cIMFs:
                try:
                    model = pm.auto_arima(cimf,
                                          stepwise=False,
                                          n_jobs=-1)
                    predict = model.predict(steps)
                except Exception:
                    predict = np.full(steps, np.mean(cimf[-steps:]))
                    have_error = True
                total += predict
            ans = pd.Series(total, index=forward_index(-1))
            df[col] = ans
        return df, have_error


def get_cIMFs(series: pd.Series):
    """
    本函数是原始的版本，返回的数据是np.ndarray
    :param series: pandas的Series类型数据，具有时间戳索引
    :return: cIMFs: , res: pd.Series
    """
    seq = series.values
    ceemdan = CEEMDAN()
    ceemdan.ceemdan(seq)
    cIMFs, res = ceemdan.get_imfs_and_residue()
    return cIMFs, res


def get_indexed_cIMFs(series: pd.Series):
    """
    本函数对ceemdan的函数进行了包装，保证输入和输出都是基于pd.Series的数据，并维护了索引不变
    :param series: pandas的Series类型数据，具有时间戳索引
    :return: cIMFs: list[pd.Series], res: pd.Series
    """
    if type(series) != pd.Series:
        raise TypeError('Expected pd.Series,', type(series), 'however.')
    seq = series.values
    ceemdan = CEEMDAN()
    ceemdan.ceemdan(seq)
    cIMFs, res = ceemdan.get_imfs_and_residue()
    cIMFs = [pd.Series(cimf, index=series.index) for cimf in cIMFs]
    res = pd.Series(res, index=series.index)
    return cIMFs, res
