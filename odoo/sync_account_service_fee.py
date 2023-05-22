#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2022/12/11 11:46 PM
作者    : wushuang
功能    : 客服扣手续费
参数    : 
"""

import os
import sys
import datetime

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
    def __init__(self, stime, etime):
        self.stime = datetime.datetime.strptime(stime, '%Y-%m-%d')
        self.etime = datetime.datetime.strptime(etime, '%Y-%m-%d')
        self.pid = "%d" % (os.getpid())
        self.log = Util.get_logger(self.pid)
        self.__ch_conn = ClickHouseDb("hw307")

    def sync_service_fee(self):
        odoo_activate = CommonConf.odoo15_conf['activate']
        odoo_url = CommonConf.odoo15_conf[odoo_activate]['odoo_url']
        db_name = CommonConf.odoo15_conf[odoo_activate]['db_name']
        user_name = CommonConf.odoo15_conf[odoo_activate]['user_name']
        pwd = CommonConf.odoo15_conf[odoo_activate]['pwd']
        try:
            # 查询充值明细 生成 journal item
            service_sql = """
                select 
                    c.id,
                    round(cast(a.service_fee as double),6) as service_fee_amount,
                    round(cast(a.service_fee*d.to_usd_rate as double),6) as  service_fee_usd
                from (select 
                    currency_id,
                    sum(money) as service_fee
                from df01.ods_iwala_finance 
                where add_time >= toUnixTimestamp('{}') 
                    and  add_time < toUnixTimestamp('{}')
                and trim(content) = '客服扣手续费'
                group by currency_id) a 
                    join df01.ods_iwala_currency b on a.currency_id = b.currency_id 
                    join odoo.dim_odoo_currency c on b.currency_mark = c.full_name
                    left join df02.dim_currency_exchange_rate d on b.currency_mark = d.currency_mark 
                where c.stime='{}' and d.stime = '{}' and position(c.name, '-') > 0
            """.format(self.stime, self.etime, self.etime.date(), self.etime.date())
            self.__ch_conn.connect()
            result = self.__ch_conn.execute(service_sql)
            journal_items = []
            if result['code']:
                if len(result['data']) == 0:
                    self.log.info("no data found!")
                    return True, None
                # 生成journal entry
                od_common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(odoo_url))
                uid = od_common.authenticate(db_name, user_name, pwd, {})
                od_models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(odoo_url))
                journal_entry = {"date": str(self.stime.date()),
                                 "ref": "user deposit manual handling fee",
                                 "journal_id": 50,
                                 "company_id": 24,
                                 "currency_id": 2}
                journal_entry_id = od_models.execute_kw(db_name, uid, pwd, 'account.move', 'create', [journal_entry])
                for item in result['data']:
                    debit_journal_item = {
                         'move_id': journal_entry_id,
                         'account_id': 1063,
                         'amount_currency': float(item[1]),
                         'currency_id': item[0],
                         # 'analytic_account_id': 89,
                         'debit': float(item[2])
                    }
                    journal_items.append(debit_journal_item)
                    credit_journal_item = {
                        'move_id': journal_entry_id,
                        'account_id': 1093,
                        'amount_currency': float(item[1]*-1),
                        'currency_id': item[0],
                        'analytic_account_id': 84,
                        'credit': float(item[2])
                    }
                    journal_items.append(credit_journal_item)
                od_models.execute_kw(db_name, uid, pwd, 'account.move.line', 'create', [journal_items])
                # 更新凭证状态
                # od_models.execute_kw(db_name, uid, pwd, 'account.move', 'write', [[journal_entry_id], {"state": "posted"}])
            else:
                return False, None
        except Exception as ex:
            self.log.error("odoo request Exception:%s", ex)
            return False, None
        return True, None


def main():
    stime = sys.argv[1]
    etime = sys.argv[2]
    dp = DataProcess(stime, etime)
    func_list = ['sync_service_fee']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()
