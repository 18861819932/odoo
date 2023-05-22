#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2023/5/4 19:05
作者    : wushuang
功能    : 杠杆对冲数据同步odoo
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
    def __init__(self, stime, ptime):
        self.stime = datetime.datetime.strptime(stime, '%Y-%m-%d')
        self.ptime = datetime.datetime.strptime(ptime, '%Y-%m-%d')
        self.pid = "%d" % (os.getpid())
        self.log = Util.get_logger(self.pid)
        self.__ch_conn = ClickHouseDb("hw307")

    def sync_margin_hedge_profit(self):
        odoo_activate = CommonConf.odoo15_conf['activate']
        odoo_url = CommonConf.odoo15_conf[odoo_activate]['odoo_url']
        db_name = CommonConf.odoo15_conf[odoo_activate]['db_name']
        user_name = CommonConf.odoo15_conf[odoo_activate]['user_name']
        pwd = CommonConf.odoo15_conf[odoo_activate]['pwd']
        try:
            # 查询杠杆对冲 生成 journal item
            margin_sql = """
            with tmp as (select 
                b.id as cid,
                toDecimal256(a.margin_digifinex_cm_total, 6) as mm_profit,
                toDecimal256(a.margin_digifinex_cm_total*c.to_usd_rate, 6) as mm_usd_profit,
                toDecimal256(a.hedge_profit, 6) as hd_profit,
                toDecimal256(a.hedge_profit*c.to_usd_rate, 6) as hd_usd_profit
            from (
                select 'USDT' as currency_mark, margin_digifinex_cm_total, hedge_profit
                from iwala_financial.ads_hedge_margin_profit 
                where stime = '{}') a
            join odoo.dim_odoo_currency b on a.currency_mark = b.full_name
            left join df02.dim_currency_exchange_rate c on a.currency_mark = c.currency_mark 
            where  b.stime = '{}' and c.stime = '{}')
            select
                cid,
                mm_profit,
                mm_usd_profit,
                hd_profit,
                hd_usd_profit,
                hd_profit - mm_profit as t1,
                hd_usd_profit - mm_usd_profit as t2
            from tmp
            """.format(self.ptime.date(), self.stime.date(), self.stime.date())
            self.__ch_conn.connect()
            result = self.__ch_conn.execute(margin_sql)
            journal_items = []
            if result['code']:
                if len(result['data']) == 0:
                    self.log.info("no data found!")
                    return True, None
                # 生成journal entry
                od_common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(odoo_url))
                uid = od_common.authenticate(db_name, user_name, pwd, {})
                od_models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(odoo_url))
                journal_entry = {"date": str(self.ptime.date()),
                                 "ref": "Margin Hedge outcome",
                                 "journal_id": 216,
                                 "company_id": 28,
                                 "currency_id": 2}
                journal_entry_id = od_models.execute_kw(db_name, uid, pwd, 'account.move', 'create', [journal_entry])
                for item in result['data']:
                    currency_id = item[0]
                    mm_amount = item[1]
                    mm_usd = item[2]
                    mm_account_id = 3355
                    mm_analytic_id = 428
                    self.build_item(journal_items, journal_entry_id, mm_account_id, mm_analytic_id,
                                    currency_id, mm_amount, mm_usd)
                    hd_amount = item[3] * -1
                    hd_usd = item[4] * -1
                    hd_account_id = 3264
                    hd_analytic_id = 429
                    self.build_item(journal_items, journal_entry_id, hd_account_id, hd_analytic_id,
                                    currency_id, hd_amount, hd_usd)
                    t_amount = item[5]
                    t_usd = item[6]
                    t_account_id = 5408
                    t_analytic_id = 413
                    self.build_item(journal_items, journal_entry_id, t_account_id, t_analytic_id,
                                    currency_id, t_amount, t_usd)
                print(journal_items)
                od_models.execute_kw(db_name, uid, pwd, 'account.move.line', 'create', [journal_items])
            else:
                return False, None
        except Exception as ex:
            self.log.error("odoo request Exception:%s", ex)
            return False, None
        return True, None

    def build_item(self, journal_items, journal_id, account_id, analytic_id, currency_id, amount, usd_money):
        if amount > 0:
            credit_journal_item = {
                'move_id': journal_id,
                'account_id': account_id,
                'amount_currency': float(amount * -1),
                'currency_id': currency_id,
                'analytic_account_id': analytic_id,
                'credit': float(usd_money)
            }
            journal_items.append(credit_journal_item)
        elif amount < 0:
            debit_journal_item = {
                'move_id': journal_id,
                'account_id': account_id,
                'amount_currency': float(amount * -1),
                'currency_id': currency_id,
                'analytic_account_id': analytic_id,
                'debit': float(usd_money * -1)
            }
            journal_items.append(debit_journal_item)


def main():
    stime = sys.argv[1]
    ptime = sys.argv[2]
    dp = DataProcess(stime, ptime)
    func_list = ['sync_margin_hedge_profit']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()
