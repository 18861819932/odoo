#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2023/02/27 11:14 PM
作者    : revis
功能    : 基金运行
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

    def sync_fund_run(self):
        odoo_activate = CommonConf.odoo15_conf['activate']
        odoo_url = CommonConf.odoo15_conf[odoo_activate]['odoo_url']
        db_name = CommonConf.odoo15_conf[odoo_activate]['db_name']
        user_name = CommonConf.odoo15_conf[odoo_activate]['user_name']
        pwd = CommonConf.odoo15_conf[odoo_activate]['pwd']
        try:
            # 查询基金募集金额 生成 journal item
            fund_run_sql = """
            select  b.id,a.fund_currency_mark,
                        case when a.fund_exchange_type in('D网','未知') then 100
                             when a.fund_exchange_type = '币安' then 101
                             when a.fund_exchange_type = '火币' then 102
                             when a.fund_exchange_type = 'OKX' then 103
                             else 0
                        end as fund_finance_type,
                        round(a.initial_price, 6) as  coin_num_run,
                        round(a.initial_price*c.price_usdt, 6) as  usd_num_run
                from odoo.ods_odoo_fund_initial_price as a 
                left join df01.ods_iwala_currency  as d on a.collect_currency_id = d.currency_id 
                left join odoo.dim_odoo_currency as b on d.currency_mark = b.full_name 
                left join df01.ods_r_d_iwala_currency_zero_price c on a.collect_currency_id = c.currency_trade_id
                where  a.status >= 40 and a.status <= 70 and  toDate(b.stime) =if(toDate('{}')>='2022-12-01','{}','2022-12-01') and toDate(c.stime) ='{}' and a.run_begin_date ='{}'
            """.format(self.stime.date(), self.stime.date(),self.stime.date(),self.stime.date())
            self.__ch_conn.connect()
            result = self.__ch_conn.execute(fund_run_sql)
            print(fund_run_sql)
            print(result['data'])
            if result['code']:
                if len(result['data']) == 0:
                    self.log.info("no data found!")
                    return True, None
                # 生成journal entry
                od_common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(odoo_url))
                uid = od_common.authenticate(db_name, user_name, pwd, {})
                od_models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(odoo_url))
                for item in result['data']:
                    journal_items = []
                    journal_entry = {"date": str(self.stime.date()),
                                 "ref": "<%s> Fund operate"%(item[1]),
                                 "journal_id": 56,
                                 "company_id": 24,
                                 "currency_id": 2}
                    journal_entry_id = od_models.execute_kw(db_name, uid, pwd, 'account.move', 'create', [journal_entry])
                    debit_journal_item = {
                         'move_id': journal_entry_id,
                         'account_id': 975,
                         'amount_currency': float(item[3]),
                         'currency_id': item[0],
                         'analytic_account_id': item[2],
                         'debit': float(item[4])
                    }
                    journal_items.append(debit_journal_item)
                    credit_journal_item = {
                        'move_id': journal_entry_id,
                        'account_id': 974,
                        'amount_currency': float(item[3]*-1),
                        'currency_id': item[0],
                        'analytic_account_id': 100,
                        'credit': float(item[4])
                    }
                    journal_items.append(credit_journal_item)
                    print(journal_items)
                    od_models.execute_kw(db_name, uid, pwd, 'account.move.line', 'create', [journal_items])
                    # od_models.execute_kw(db_name, uid, pwd, 'account.move', 'write',
                    #                     [[journal_entry_id], {"state": "posted"}])
            else:
                return False, None
            # 更新凭证状态

        except Exception as ex:
            self.log.error("odoo request Exception:%s", ex)
            return False, None
        return True, None


def main():
    stime = sys.argv[1]
    dp = DataProcess(stime)
    func_list = ['sync_fund_run']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()
