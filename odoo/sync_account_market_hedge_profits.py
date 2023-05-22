#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2023/3/27 17:57
作者    : wushuang
功能    : 同步对冲 盈亏, 净持币 资产差
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
        self.mm_id = 217
        self.hedge_id = 216
        self.odoo_activate = CommonConf.odoo15_conf['activate']
        self.odoo_url = CommonConf.odoo15_conf[self.odoo_activate]['odoo_url']
        self.db_name = CommonConf.odoo15_conf[self.odoo_activate]['db_name']
        self.user_name = CommonConf.odoo15_conf[self.odoo_activate]['user_name']
        self.pwd = CommonConf.odoo15_conf[self.odoo_activate]['pwd']
        self.od_common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(self.odoo_url))
        self.uid = self.od_common.authenticate(self.db_name, self.user_name, self.pwd, {})
        self.od_models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(self.odoo_url))
        self.__ch_conn = ClickHouseDb("hw307")
        self.__ch_conn.connect()

    def sync_contract_profit(self):
        try:
            # 查询充值明细 生成 journal item
            contract_sql = """
                select 
                    b.id,
                    round(cast(a.d_profit as double),6) as mm_profit,
                    round(cast(a.d_profit*c.to_usd_rate as double),6) as mm_usd_profit,
                    round(cast(a.today_profit as double),6) as hd_profit,
                    round(cast(a.today_profit*c.to_usd_rate as double),6) as hd_usd_profit
                from iwala_financial.ads_futures_mm_profit_history a
                join odoo.dim_odoo_currency b on a.currency_mark = b.full_name
                left join df02.dim_currency_exchange_rate c on a.currency_mark = c.currency_mark 
                where a.stime = '{}' and b.stime = '{}' and c.stime = '{}'
                """.format(self.stime.date(), self.stime.date(), self.stime.date())
            self.log.info(contract_sql)
            result = self.__ch_conn.execute(contract_sql)
            if result['code']:
                if len(result['data']) == 0:
                    self.log.info("no data found!")
                    return True, None
                mm_items = []
                hd_items = []
                for item in result['data']:
                    if item[1] != 0:
                        mm_item = {
                            "id": item[0],
                            "amount": item[1],
                            "usd": item[2]
                        }
                        mm_items.append(mm_item)
                    if item[3] != 0:
                        hd_item = {
                            "id": item[0],
                            "amount": item[3],
                            "usd": item[4]
                        }
                        hd_items.append(hd_item)
                self.log.info("contract mm length {}".format(len(mm_items)))
                if len(mm_items) > 0:
                    self.create_journal(mm_items, self.od_models, self.db_name, self.uid, self.pwd,
                                        "Contract MM outcome", self.mm_id)
                self.log.info("contract hedge length {}".format(len(hd_items)))
                if len(hd_items) > 0:
                    self.create_journal(hd_items, self.od_models, self.db_name, self.uid, self.pwd,
                                        "Contract Hedge outcome", self.hedge_id)
            else:
                return False, None
        except Exception as ex:
            self.log.error("contract profit Exception:%s", ex)
            return False, None
        return True, None

    def sync_etf_profit(self):
        try:
            # 查询充值明细 生成 journal item
            etf_sql = """
                select 
                    b.id,
                    round(cast(a.spot_profit as double),6) as d_profit,
                    round(cast(a.spot_profit*c.to_usd_rate as double),6) as d_profit_usd,
                    round(cast(a.swap_profit as double),6) as hedge_profit,                   
                    round(cast(a.swap_profit*c.to_usd_rate as double),6) as hedge_profit_usd
                from iwala_financial.ads_etf_summary_profit a
                join odoo.dim_odoo_currency b on a.currency_mark = b.full_name
                left join df02.dim_currency_exchange_rate c on a.currency_mark = c.currency_mark 
                where a.stime = '{}' and b.stime = '{}' and c.stime = '{}'
                """.format(self.ptime.date(), self.stime.date(), self.stime.date())
            self.log.info(etf_sql)
            result = self.__ch_conn.execute(etf_sql)
            if result['code']:
                if len(result['data']) == 0:
                    self.log.info("etf no data found!")
                    return True, None
                etf_mm_items = []
                etf_hd_items = []
                for item in result['data']:
                    if item[1] != 0:
                        mm_item = {
                            "id": item[0],
                            "amount": item[1],
                            "usd": item[2]
                        }
                        etf_mm_items.append(mm_item)
                    if item[3] != 0:
                        hd_item = {
                            "id": item[0],
                            "amount": item[3],
                            "usd": item[4]
                        }
                        etf_hd_items.append(hd_item)
                self.log.info("etf mm length {}".format(len(etf_mm_items)))
                if len(etf_mm_items) > 0:
                    self.create_journal(etf_mm_items, self.od_models, self.db_name, self.uid, self.pwd,
                                        "ETF MM outcome", self.mm_id)
                self.log.info("etf hedge length {}".format(len(etf_hd_items)))
                if len(etf_hd_items) > 0:
                    self.create_journal(etf_hd_items, self.od_models, self.db_name, self.uid, self.pwd,
                                        "ETF Hedge outcome", self.hedge_id)
            else:
                return False, None
        except Exception as ex:
            self.log.error("etf profit Exception:%s", ex)
            return False, None
        return True, None

    def sync_margin_profit(self):
        try:
            # 查询充值明细 生成 journal item
            margin_sql = """
                select 
                    b.id,
                    round(cast(a.margin_digifinex_cm_total as double), 6) as mm_profit,
                    round(cast(a.margin_digifinex_cm_total*c.to_usd_rate as double), 6) as mm_usd_profit,
                    round(cast(a.hedge_profit as double), 6) as hd_profit,
                    round(cast(a.hedge_profit*c.to_usd_rate as double), 6) as hd_usd_profit,
                    round(cast(a.profit_total as double), 6) as total_profit,
                    round(cast(a.profit_total*c.to_usd_rate as double), 6) as total_usd_profit
                from (
                    select 'USDT' as currency_mark, margin_digifinex_cm_total, hedge_profit,
                        (hedge_profit - margin_digifinex_cm_total) as profit_total 
                    from iwala_financial.ads_hedge_margin_profit 
                    where stime = '{}') a
                join odoo.dim_odoo_currency b on a.currency_mark = b.full_name
                left join df02.dim_currency_exchange_rate c on a.currency_mark = c.currency_mark 
                where  b.stime = '{}' and c.stime = '{}'
                """.format(self.stime.date(), self.stime.date(), self.stime.date())
            self.log.info(margin_sql)
            result = self.__ch_conn.execute(margin_sql)
            if result['code']:
                if len(result['data']) == 0:
                    self.log.info("margin no data found!")
                    return True, None
                margin_items = []
                for item in result['data']:
                    if item[1] != 0:
                        spot_item = {
                            "id": item[0],
                            "amount": item[1],
                            "usd": item[2]
                        }
                        margin_items.append(spot_item)
                if len(margin_items) > 0:
                    self.log.info("margin hedge length {}".format(len(margin_items)))
                    self.create_journal(margin_items, self.od_models, self.db_name, self.uid, self.pwd,
                                        "Margin Hedge outcome")
            else:
                return False, None
        except Exception as ex:
            self.log.error("margin profit Exception:%s", ex)
            return False, None
        return True, None

    def sync_spot_profit(self):
        try:
            # 查询充值明细 生成 journal item
            spot_sql = """
                select 
                    b.id,
                    round(cast(a.hedge_profit as double),6) as spot_profit,
                    round(cast(a.hedge_profit*c.to_usd_rate as double),6) as spot_usd_profit
                from iwala_financial.ads_spot_hedge_profit a
                join odoo.dim_odoo_currency b on a.currency_mark = b.full_name
                left join df02.dim_currency_exchange_rate c on a.currency_mark = c.currency_mark 
                where a.stime = '{}' and  b.stime = '{}' and c.stime = '{}'
                """.format(self.ptime.date(), self.stime.date(), self.stime.date())
            self.log.info(spot_sql)
            result = self.__ch_conn.execute(spot_sql)
            if result['code']:
                if len(result['data']) == 0:
                    self.log.info("spot no data found!")
                    return True, None
                spot_items = []
                for item in result['data']:
                    if item[1] != 0:
                        spot_item = {
                            "id": item[0],
                            "amount": item[1],
                            "usd": item[2]
                        }
                        spot_items.append(spot_item)
                if len(spot_items) > 0:
                    self.log.info("spot hedge length {}".format(len(spot_items)))
                    self.create_journal(spot_items, self.od_models, self.db_name, self.uid, self.pwd,
                                        "SPOT Hedge outcome", self.hedge_id)
            else:
                return False, None
        except Exception as ex:
            self.log.error("spot profit Exception:%s", ex)
            return False, None
        return True, None

    def create_journal(self, items, od_models, db, uid, pwd, reference, journal_id):
        journal_entry = {"date": str(self.ptime.date()),
                         "ref": reference,
                         "journal_id": journal_id,
                         "company_id": 28,
                         "currency_id": 2}
        journal_entry_id = od_models.execute_kw(db, uid, pwd, 'account.move', 'create', [journal_entry])
        journal_items = []
        analytic_id = 428
        if journal_id == self.hedge_id:
            analytic_id = 429
        for obj in items:
            if obj['amount'] > 0:
                debit_id = 3264
                credit_id = 5408
                debit_analytic_id = analytic_id
                credit_analytic_id = 413
                debit_amount = float(obj['amount'])
                credit_amount = float(obj['amount'] * -1)
                currency_usd = float(obj['usd'])
            else:
                debit_id = 5408
                credit_id = 3264
                debit_analytic_id = 413
                credit_analytic_id = analytic_id
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
    func_list = ['sync_spot_profit', 'sync_etf_profit', 'sync_contract_profit']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()
