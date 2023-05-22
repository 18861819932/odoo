#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2023/2/21 11:02
作者    : wushuang
功能    : 每日同步新币种
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

    def sync_currency(self):
        odoo_activate = CommonConf.odoo15_conf['activate']
        odoo_url = CommonConf.odoo15_conf[odoo_activate]['odoo_url']
        db_name = CommonConf.odoo15_conf[odoo_activate]['db_name']
        user_name = CommonConf.odoo15_conf[odoo_activate]['user_name']
        pwd = CommonConf.odoo15_conf[odoo_activate]['pwd']
        try:
            check_sql = """select
                count()
            from odoo.dim_odoo_currency 
            where stime = '{}'
            """.format(self.stime.date())
            self.__ch_conn.connect()
            check_result = self.__ch_conn.execute(check_sql)
            if check_result['code'] and len(check_result['data']) > 0:
                row_num = check_result['data'][0][0]
                if row_num < 1500:
                    self.log.error("data error, the number of currency is {}!".format(row_num))
                    return False, None
            else:
                self.log.error("check data error!")
                return False, None
            self.log.info("check success!")
            # 查询每日汇率数据
            new_currency_sql = """
                select 
                    a.currency_id,
                    a.currency_mark,
                    b.id,
                    b.curr_id,
                    b.curr_mark
                from df01.ods_iwala_currency a 
                left join (select 
                    id,
                    toUInt32(splitByChar('-', name)[1]) as curr_id,
                    splitByChar('-', name)[2] as curr_mark
                from odoo.dim_odoo_currency
                where position(name, '-') > 0 and stime = '{}') b on a.currency_id = b.curr_id
                where b.id = 0 and a.currency_id not in (146, 312, 402, 1711)
            """.format(self.stime.date())
            print(new_currency_sql)
            result = self.__ch_conn.execute(new_currency_sql)
            # 生成new currency
            currency_items = []
            od_common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(odoo_url))
            uid = od_common.authenticate(db_name, user_name, pwd, {})
            od_models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(odoo_url))
            if result['code'] and len(result['data']) > 0:
                for item in result['data']:
                    tmp_name = "{}-{}".format(item[0], item[1])
                    currency_item = {
                        'name': tmp_name,
                        'full_name': item[1],
                        'symbol': item[1],
                        'display_name': tmp_name,
                        'active': True
                    }
                    currency_items.append(currency_item)
                od_models.execute_kw(db_name, uid, pwd, 'res.currency', 'create', [currency_items])
                self.log.info("the num of add currency: {}".format(len(currency_items)))
            else:
                self.log.info("no currency to add")
            # 更新currency
            self.update_currency(od_models, db_name, uid, pwd)
        except Exception as ex:
            self.log.error("odoo request Exception:%s", ex)
            return False, None
        return True, None

    def update_currency(self, odoo_model, db_name, uid, pwd):
        update_sql ="""
            select 
                a.currency_id,
                a.currency_mark,
                b.id,
                b.curr_id,
                b.curr_mark
            from df01.ods_iwala_currency a 
            join (select 
                stime,
                id,
                toUInt32(splitByChar('-', name)[1]) as curr_id,
                splitByChar('-', name)[2] as curr_mark
            from odoo.dim_odoo_currency
            where position(name, '-') > 0 and stime = '{}') b on a.currency_id = b.curr_id
            where a.currency_id not in (146, 312, 402, 1711) and a.currency_mark <> b.curr_mark
        """.format(self.stime.date())
        update_result = self.__ch_conn.execute(update_sql)
        # 获取需要更新的币种
        if update_result['code'] and len(update_result['data']) > 0:
            for obj in update_result['data']:
                display_name = "{}-{}".format(obj[0], obj[1])
                update_item = {
                    'name': display_name,
                    'full_name': obj[1],
                    'symbol': obj[1],
                    'display_name': display_name
                }
                self.log.info("update the {} currency name {} to {}".format(obj[2], obj[4], obj[1]))
                odoo_model.execute_kw(db_name, uid, pwd, 'res.currency', 'write', [[obj[2]], update_item])
            self.log.info("the num of updating currency: {}".format(len(update_result['data'])))
        else:
            self.log.info("no currency update")


def main():
    stime = sys.argv[1]
    dp = DataProcess(stime)
    func_list = ['sync_currency']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()

