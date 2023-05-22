#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2023/01/06 14:52 PM
作者    : revis
功能    : 现货手续费
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

    def sync_spot_fee_discount(self):
        odoo_activate = CommonConf.odoo15_conf['activate']
        odoo_url = CommonConf.odoo15_conf[odoo_activate]['odoo_url']
        db_name = CommonConf.odoo15_conf[odoo_activate]['db_name']
        user_name = CommonConf.odoo15_conf[odoo_activate]['user_name']
        pwd = CommonConf.odoo15_conf[odoo_activate]['pwd']
        try:
            # 查询dft抵扣明细 生成 journal item
            sopt_fee_sql = """
                select    b.id,
                    round(cast(a.total_spot_fee_num as double), 6) as coin_num,
                    round(cast(if(c.price_usdt = 0 ,a.total_spot_fee_num * e.price_usdt, a.total_spot_fee_num *c.price_usdt)
                    as double), 6) as usd_num
                from (select 
	                currency_trade_id,
	                sum(fee) total_spot_fee_num
                FROM df01.ods_iwala_trade_user
				WHERE toDate(add_time) = '{}' 
				  AND kind = 'spot'
				  AND currency_id not in
				    (SELECT currency_id
				     FROM df01.ods_iwala_currency
				     WHERE currency_mark LIKE '%%%%3L'
				       OR currency_mark LIKE '%%%%3S')
				group by currency_trade_id
                having total_spot_fee_num>0) a
                left join df01.ods_iwala_currency  as d on a.currency_trade_id = d.currency_id 
                left join odoo.dim_odoo_currency as b on d.currency_mark = b.full_name  
                left join df01.ods_r_d_iwala_currency_zero_price c on a.currency_trade_id = c.currency_trade_id 
                left join df02.dwd_r_d_iwala_currency_trade_initial_price AS e ON d.currency_mark =e.currency_mark
                where b.stime = '{}' and toDate(c.stime) = '{}' 
            """.format(self.stime.date(), self.stime.date(), self.stime.date())
            self.__ch_conn.connect()
            result = self.__ch_conn.execute(sopt_fee_sql)
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
                                 "ref": "tx fee charged on spot",
                                 "journal_id": 51,
                                 "company_id": 24,
                                 "currency_id": 2}
                journal_entry_id = od_models.execute_kw(db_name, uid, pwd, 'account.move', 'create', [journal_entry])
                for item in result['data']:
                    debit_journal_item = {
                         'move_id': journal_entry_id,
                         'account_id': 1063,
                         'amount_currency': float(item[1]),
                         'currency_id': item[0],
                         'analytic_account_id': 100,
                         'debit': float(item[2])
                    }
                    journal_items.append(debit_journal_item)
                    credit_journal_item = {
                        'move_id': journal_entry_id,
                        'account_id': 1086,
                        'amount_currency': float(item[1]*-1),
                        'currency_id': item[0],
                        'analytic_account_id': 81,
                        'credit': float(item[2])
                    }
                    journal_items.append(credit_journal_item)
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
    func_list = ['sync_spot_fee_discount']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()
