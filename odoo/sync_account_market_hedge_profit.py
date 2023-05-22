#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2023/3/1 17:48
作者    : wushuang
功能    : 同步对冲资产脚本, 净持币 资产差
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

    def sync_pool_gas_fee(self):
        odoo_activate = CommonConf.odoo15_conf['activate']
        odoo_url = CommonConf.odoo15_conf[odoo_activate]['odoo_url']
        db_name = CommonConf.odoo15_conf[odoo_activate]['db_name']
        user_name = CommonConf.odoo15_conf[odoo_activate]['user_name']
        pwd = CommonConf.odoo15_conf[odoo_activate]['pwd']
        try:
            # 查询充值明细 生成 journal item
            hedge_sql = """
                select 
                    b.id,
                    round(cast(a.num_third_profit as double),6) as amount_third_profit,
                    round(cast(a.num_third_profit*c.to_usd_rate as double),6) as num_third_profit_usd,
                    round(cast(a.num_contract_third_profit as double),6) as amount_contract_third_profit,
                    round(cast(a.num_contract_third_profit*c.to_usd_rate as double),6) as num_contract_third_profit_usd,
                    round(cast(a.num_leverage_third_profit as double),6) as amount_leverage_third_profit,
                    round(cast(a.num_leverage_third_profit*c.to_usd_rate as double),6) as num_leverage_third_profit_usd,
                    round(cast(a.num_etf_third_profit as double),6) as amount_etf_third_profit,
                    round(cast(a.num_etf_third_profit*c.to_usd_rate as double),6) as num_etf_third_profit_usd
                from iwala_financial.ads_iwala_hedge_third_assert_d a
                join odoo.dim_odoo_currency b on a.currency_mark = b.full_name
                left join df02.dim_currency_exchange_rate c on a.currency_mark = c.currency_mark 
                where a.stime = '{}' and b.stime = '{}' and c.stime = '{}'
                """.format(self.stime.date(), self.stime.date(), self.stime.date())
            self.log.info(hedge_sql)
            self.__ch_conn.connect()
            result = self.__ch_conn.execute(hedge_sql)
            if result['code']:
                if len(result['data']) == 0:
                    self.log.info("no data found!")
                    return True, None
                spot_items = []
                contract_items = []
                margin_items = []
                etf_items = []
                for item in result['data']:
                    if item[1] != 0:
                        spot_item = {
                            "id": item[0],
                            "amount": item[1],
                            "usd": item[2]
                        }
                        spot_items.append(spot_item)
                    if item[3] != 0:
                        contract_item = {
                            "id": item[0],
                            "amount": item[3],
                            "usd": item[4]
                        }
                        contract_items.append(contract_item)
                    if item[5] != 0:
                        margin_item = {
                            "id": item[0],
                            "amount": item[5],
                            "usd": item[6]
                        }
                        margin_items.append(margin_item)
                    if item[7] != 0:
                        etf_item = {
                            "id": item[0],
                            "amount": item[7],
                            "usd": item[8]
                        }
                        etf_items.append(etf_item)
                od_common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(odoo_url))
                uid = od_common.authenticate(db_name, user_name, pwd, {})
                od_models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(odoo_url))
                if len(spot_items) > 0:
                    self.log.info("spot hedge length {}".format(len(spot_items)))
                    self.create_journal(spot_items, od_models, db_name, uid, pwd, "SPOT Hedge outcome")
                if len(contract_items) > 0:
                    self.log.info("contract hedge length {}".format(len(contract_items)))
                    self.create_journal(contract_items, od_models, db_name, uid, pwd, "Contract Hedge outcome")
                if len(margin_items) > 0:
                    self.log.info("margin hedge length {}".format(len(margin_items)))
                    self.create_journal(margin_items, od_models, db_name, uid, pwd, "Margin Hedge outcome")
                if len(etf_items) > 0:
                    self.log.info("etf hedge length {}".format(len(etf_items)))
                    self.create_journal(etf_items, od_models, db_name, uid, pwd, "ETF Hedge outcome")
            else:
                return False, None
        except Exception as ex:
            self.log.error("odoo request Exception:%s", ex)
            return False, None
        return True, None

    def create_journal(self, items, od_models, db, uid, pwd, reference):
        journal_entry = {"date": str(self.ptime.date()),
                         "ref": reference,
                         "journal_id": 216,
                         "company_id": 28,
                         "currency_id": 2}
        journal_entry_id = od_models.execute_kw(db, uid, pwd, 'account.move', 'create', [journal_entry])
        journal_items = []
        for obj in items:
            if obj['amount'] > 0:
                debit_id = 3264
                credit_id = 5408
                debit_analytic_id = 428
                credit_analytic_id = 413
                debit_amount = float(obj['amount'])
                credit_amount = float(obj['amount'] * -1)
                currency_usd = float(obj['usd'])
            else:
                debit_id = 5408
                credit_id = 3264
                debit_analytic_id = 413
                credit_analytic_id = 428
                debit_amount = float(obj['amount'] * -1)
                credit_amount = float(obj['amount'])
                currency_usd = float(obj['usd'] * -1)
            debit_journal_item = {
                'move_id': journal_entry_id,
                'account_id': debit_id,
                'currency_id': obj['id'],
                'analytic_account_id': debit_analytic_id,
                'amount_currency': debit_amount,
                'debit': currency_usd
            }
            journal_items.append(debit_journal_item)
            credit_journal_item = {
                'move_id': journal_entry_id,
                'account_id': credit_id,
                'currency_id': obj['id'],
                'analytic_account_id': credit_analytic_id,
                'amount_currency': credit_amount,
                'credit': currency_usd
            }
            journal_items.append(credit_journal_item)
        od_models.execute_kw(db, uid, pwd, 'account.move.line', 'create', [journal_items])


def main():
    stime = sys.argv[1]
    ptime = sys.argv[2]
    dp = DataProcess(stime, ptime)
    func_list = ['sync_pool_gas_fee']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()
