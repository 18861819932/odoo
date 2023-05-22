#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2022/12/3 11:21 AM
作者    : wushuang
功能    : 每日同步 汇率
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

    def sync_currency_rate(self):
        odoo_activate = CommonConf.odoo15_conf['activate']
        odoo_url = CommonConf.odoo15_conf[odoo_activate]['odoo_url']
        db_name = CommonConf.odoo15_conf[odoo_activate]['db_name']
        user_name = CommonConf.odoo15_conf[odoo_activate]['user_name']
        pwd = CommonConf.odoo15_conf[odoo_activate]['pwd']
        try:
            # 查询每日汇率数据
            rate_sql = """
                SELECT
                    doc.id,
                    dcer.to_usd_rate
                from odoo.dim_odoo_currency doc join df02.dim_currency_exchange_rate dcer on doc.full_name = dcer.currency_mark
                where doc.stime = '{}' and position(doc.name, '-') > 0 and dcer.stime = '{}' and dcer.to_usd_rate > 0
            """.format(self.stime.date(), self.stime.date())
            print(rate_sql)
            self.__ch_conn.connect()
            result = self.__ch_conn.execute(rate_sql)
            # 生成currency rate
            rate_items = []
            if result['code']:
                for item in result['data']:
                    rate_item = {
                        'name': str(self.stime.date()),
                        'inverse_company_rate': str(item[1]),
                        'currency_id': item[0],
                        'company_id': 24}
                    rate_items.append(rate_item)
                od_common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(odoo_url))
                uid = od_common.authenticate(db_name, user_name, pwd, {})
                od_models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(odoo_url))
                od_models.execute_kw(db_name, uid, pwd, 'res.currency.rate', 'create', [rate_items])
            else:
                return False, None
        except Exception as ex:
            self.log.error("odoo request Exception:%s", ex)
            return False, None
        return True, None


def main():
    stime = sys.argv[1]
    dp = DataProcess(stime)
    func_list = ['sync_currency_rate']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()
