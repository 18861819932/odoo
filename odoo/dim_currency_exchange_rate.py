#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2022/11/30 6:32 PM
作者    : wushuang
功能    : 每日凌晨获取 币的汇率
参数    : 
"""

import os
import sys
from datetime import datetime

filepath = os.path.abspath(__file__)
filename = os.path.basename(filepath)
curr_dir = os.path.dirname(filepath)
bi_path = os.path.dirname(curr_dir)
if sys.path[sys.path.__len__() - 1] != bi_path:
    sys.path.append(bi_path)  # 引入新的模块路径

from common.util_redis import RedisDb
from common.util import Util
from common.util_clickhouse import ClickHouseDb
import requests
import json
import urllib.parse


class DataProcess:
    def __init__(self, stime):
        self.stime = datetime.strptime(stime, '%Y-%m-%d')
        self.pid = "%d" % (os.getpid())
        self.log = Util.get_logger(self.pid)
        self.__ch_conn = ClickHouseDb("hw307")
        self.exchange_rates = []

    def get_exchange(self):
        _redis_conn = RedisDb('wallet')
        _redis_conn.connect(15)
        coin_to_usdt = _redis_conn.h_get_all("DC:TO:USDT:LIST")
        money_to_usdt = _redis_conn.h_get_all("usdt2fiat_rate")
        create_time = datetime.now()
        if coin_to_usdt and money_to_usdt:
            print(money_to_usdt)
            dollar_to_usdt = money_to_usdt['USD']
            if not dollar_to_usdt:
                self.log.info("usd exchange rate not exist")
                return False, None
            usdt_to_usd = 1/float(dollar_to_usdt)
            print(dollar_to_usdt, usdt_to_usd)
            for key, value in coin_to_usdt.items():
                to_usd_rate = float(value) * usdt_to_usd
                rate_obj = {
                    'stime': self.stime.date(),
                    'currency_mark': key,
                    'currency_type': 1,
                    'to_usdt_rate': value,
                    'to_usd_rate': to_usd_rate,
                    'create_time': create_time,
                    'update_time': create_time
                }
                self.exchange_rates.append(rate_obj)

            for mark, rate in money_to_usdt.items():
                if mark == 'USD':
                    money_to_usd = 1
                elif mark == 'CNY':
                    continue
                else:
                    money_to_usd = float(rate) * usdt_to_usd
                rate_obj = {
                    'stime': self.stime.date(),
                    'currency_mark': mark,
                    'currency_type': 2,
                    'to_usdt_rate': rate,
                    'to_usd_rate': money_to_usd,
                    'create_time': create_time,
                    'update_time': create_time
                }
                self.exchange_rates.append(rate_obj)
            self.get_cny_rate(create_time, usdt_to_usd)
            return True, None
        else:
            return False, None

    def get_cny_rate(self, create_time, usdt_to_usd):
        r_url = "https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=CNY"
        try:
            rst = requests.get(r_url)
            if rst.status_code == 200:
                rst_json = json.loads(rst.text)
                cny_rate = rst_json['tether']['cny']
                self.log.info("cny rate: {}".format(cny_rate))
                if cny_rate > 0:
                    rate_obj = {
                        'stime': self.stime.date(),
                        'currency_mark': 'CNY',
                        'currency_type': 2,
                        'to_usdt_rate': cny_rate,
                        'to_usd_rate': cny_rate*usdt_to_usd,
                        'create_time': create_time,
                        'update_time': create_time
                    }
                    self.exchange_rates.append(rate_obj)
        except Exception as ex:
            self.log.info("cny rate request error: {}".format(ex))

    # 清理数据
    def clean_data(self):
        clean_sql = "alter table df02.`dim_currency_exchange_rate` delete where stime=%(stime)s"
        param = {"stime": self.stime.date()}
        self.__ch_conn.connect()
        clean_rst = self.__ch_conn.execute(clean_sql, param, 0)
        if not clean_rst['code']:
            sys.exit(1)
        return True, None

    # 写入数据
    def load_data(self):
        data_len = len(self.exchange_rates)
        if data_len > 0:
            write_sql = "insert into df02.dim_currency_exchange_rate VALUES"
            write_rst = self.__ch_conn.batch_insert(write_sql, self.exchange_rates)
            self.log.info("币种汇率数据写入结果code={},rows={},errmsg={}".format(write_rst['code'], data_len,
                                                                       write_rst['errmsg']))
            if not write_rst['code']:
                return False, None
        else:
            return False, None
        return True, None


def main():
    stime = sys.argv[1]  # 传入的目标服务器连接串
    dp = DataProcess(stime)
    func_list = ['get_exchange', 'clean_data', 'load_data']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()
