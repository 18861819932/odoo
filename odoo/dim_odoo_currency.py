#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2022/12/1 6:05 PM
作者    : wushuang
功能    : 同步odoo中的 数字币信息
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

from common.common_config import CommonConf
import xmlrpc.client
from common.util import Util
from common.util_clickhouse import ClickHouseDb


class DataProcess:
    def __init__(self, stime):
        self.stime = datetime.strptime(stime, '%Y-%m-%d')
        self.pid = "%d" % (os.getpid())
        self.log = Util.get_logger(self.pid)
        self.__ch_conn = ClickHouseDb("hw307")
        self.odoo_currency = []

    def get_odoo_currency(self):
        odoo_activate = CommonConf.odoo15_conf['activate']
        odoo_url = CommonConf.odoo15_conf[odoo_activate]['odoo_url']
        db_name = CommonConf.odoo15_conf[odoo_activate]['db_name']
        user_name = CommonConf.odoo15_conf[odoo_activate]['user_name']
        pwd = CommonConf.odoo15_conf[odoo_activate]['pwd']
        try:
            od_common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(odoo_url))
            uid = od_common.authenticate(db_name, user_name, pwd, {})
            od_models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(odoo_url))
            models = od_models.execute_kw(db_name, uid, pwd, 'res.currency', 'search_read', [],
                                          {'fields': ['id', 'name', 'symbol', 'full_name']})
            now_time = datetime.now()
            for model in models:
                model['stime'] = self.stime.date()
                model['full_name'] = str(model['full_name'])
                model['create_time'] = now_time
                model['update_time'] = now_time
                self.odoo_currency.append(model)
        except Exception as ex:
            self.log.error("odoo request Exception:%s", ex)
            return False, None
        return True, None

    # 清理数据
    def clean_data(self):
        clean_sql = "alter table odoo.dim_odoo_currency delete where stime=%(stime)s"
        param = {"stime": self.stime.date()}
        self.__ch_conn.connect()
        clean_rst = self.__ch_conn.execute(clean_sql, param, 0)
        if not clean_rst['code']:
            sys.exit(1)
        return True, None

    # 写入数据
    def load_data(self):
        data_len = len(self.odoo_currency)
        if data_len > 0:
            write_sql = "insert into odoo.dim_odoo_currency VALUES"
            write_rst = self.__ch_conn.batch_insert(write_sql, self.odoo_currency)
            self.log.info("odoo币种数据写入结果code={},rows={},errmsg={}".format(write_rst['code'], data_len,
                                                                         write_rst['errmsg']))
            if not write_rst['code']:
                return False, None
        else:
            return False, None
        return True, None


def main():
    stime = sys.argv[1]
    dp = DataProcess(stime)
    func_list = ['get_odoo_currency', 'clean_data', 'load_data']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()
