#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2023/3/8 17:48
作者    : wushuang
功能    : 同步热门币数据到odoo
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
    def __init__(self, stime):
        self.stime = datetime.datetime.strptime(stime, '%Y-%m-%d')
        self.pid = "%d" % (os.getpid())
        self.log = Util.get_logger(self.pid)
        self.__ch_conn = ClickHouseDb("hw307")

    def sync_hotmm_market(self):
        odoo_activate = CommonConf.odoo15_conf['activate']
        odoo_url = CommonConf.odoo15_conf[odoo_activate]['odoo_url']
        db_name = CommonConf.odoo15_conf[odoo_activate]['db_name']
        user_name = CommonConf.odoo15_conf[odoo_activate]['user_name']
        pwd = CommonConf.odoo15_conf[odoo_activate]['pwd']
        try:
            # 查询充值明细 生成 journal item
            hotmm_sql = """
                select 
                    doc.id,
                    round(cast(hot.day_pnl as double), 6) as hot_profit,
                    round(cast(hot.day_pnl * dr.to_usd_rate as double), 6) as usd_hot_profit
                from df03.ads_hotmm_liquidity hot join df02.dim_currency_exchange_rate dr on hot.base_mark = dr.currency_mark
                 join odoo.dim_odoo_currency doc on hot.base_mark = doc.full_name 
                where hot.stime = '{}' and dr.stime = '{}' and doc.stime = '{}' and hot.day_pnl <> 0
                """.format(self.stime.date(), self.stime.date(), self.stime.date())
            self.log.info(hotmm_sql)
            self.__ch_conn.connect()
            result = self.__ch_conn.execute(hotmm_sql)
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
                                 "ref": "Hot Currency MM outcome",
                                 "journal_id": 216,
                                 "company_id": 28,
                                 "currency_id": 2}
                journal_entry_id = od_models.execute_kw(db_name, uid, pwd, 'account.move', 'create', [journal_entry])
                for item in result['data']:
                    hot_profit = item[1]
                    if hot_profit > 0:
                        debit_id = 3264
                        credit_id = 5408
                        debit_analytic_id = 428
                        credit_analytic_id = 413
                        debit_amount = float(hot_profit)
                        credit_amount = float(hot_profit * -1)
                        currency_usd = float(item[2])
                    elif hot_profit < 0:
                        debit_id = 5408
                        credit_id = 3264
                        debit_analytic_id = 413
                        credit_analytic_id = 428
                        debit_amount = float(hot_profit * -1)
                        credit_amount = float(hot_profit)
                        currency_usd = float(item[2] * -1)
                    else:
                        continue
                    debit_journal_item = {
                         'move_id': journal_entry_id,
                         'account_id': debit_id,
                         'currency_id': item[0],
                         'analytic_account_id': debit_analytic_id,
                         'amount_currency': debit_amount,
                         'debit': currency_usd
                    }
                    journal_items.append(debit_journal_item)
                    credit_journal_item = {
                        'move_id': journal_entry_id,
                        'account_id': credit_id,
                        'currency_id': item[0],
                        'analytic_account_id': credit_analytic_id,
                        'amount_currency': credit_amount,
                        'credit': currency_usd
                    }
                    journal_items.append(credit_journal_item)
                od_models.execute_kw(db_name, uid, pwd, 'account.move.line', 'create', [journal_items])
                # 更新凭证状态
                # od_models.execute_kw(db_name, uid, pwd, 'account.move', 'write',
                #                     [[journal_entry_id], {"state": "posted"}])
            else:
                return False, None
        except Exception as ex:
            self.log.error("odoo request Exception:%s", ex)
            return False, None
        return True, None


def main():
    stime = sys.argv[1]
    dp = DataProcess(stime)
    func_list = ['sync_hotmm_market']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()
