#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2023/02/28 10:15 PM
作者    : revis
功能    : 保本浮息基金月计提
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

    def sync_account_fund_caculate_safe_float_interest(self):
        odoo_activate = CommonConf.odoo15_conf['activate']
        odoo_url = CommonConf.odoo15_conf[odoo_activate]['odoo_url']
        db_name = CommonConf.odoo15_conf[odoo_activate]['db_name']
        user_name = CommonConf.odoo15_conf[odoo_activate]['user_name']
        pwd = CommonConf.odoo15_conf[odoo_activate]['pwd']
        try:
            # 查询基金募集金额 生成 journal item
            fund_caculate_safe_float_interest_sql = """
  select id,fund_currency_mark,fund_finance_type,coin_num_float_profit,usd_num_float_profit_dr,usd_num_float_profit_cr,coin_num_client_profit,usd_num_client_profit_dr,usd_num_client_profit_cr,coin_num_client_mange,usd_num_client_mange_dr,usd_num_client_mange_cr,coin_num_fair_value_change,
			                                         if(usd_num_fair_value_change_dr>=usd_num_fair_value_change_cr,usd_num_fair_value_change_dr-usd_num_fair_value_change_cr,toDecimal256(0,6)) as usd_num_fair_value_change_d,
			                                         if(usd_num_fair_value_change_cr>usd_num_fair_value_change_dr,usd_num_fair_value_change_cr-usd_num_fair_value_change_dr,toDecimal256(0,6)) as usd_num_fair_value_change_c
			                                    from ( 
		                                             select  b.id as id,e.fund_currency_mark as fund_currency_mark,
	                                                    case when a.fund_exchange_type in('D网','未知') then 100
	                                                    when a.fund_exchange_type = '币安' then 101
	                                                    when a.fund_exchange_type = '火币' then 102
	                                                    when a.fund_exchange_type = 'OKX' then 103
	                                                    else 0
	                                                    end as fund_finance_type,
					                                    toDecimal256(a.floating_profit_loss, 6) as  coin_num_float_profit,
					                                    toDecimal256(if(a.floating_profit_loss >= 0,a.floating_profit_loss*c.price_usdt,0), 6)    as  usd_num_float_profit_dr,
					                                    toDecimal256(if(a.floating_profit_loss <  0,-1*a.floating_profit_loss*c.price_usdt,0), 6) as  usd_num_float_profit_cr,
					                                    toDecimal256(a.client_profit, 6) as  coin_num_client_profit,
					                                    toDecimal256(if(a.client_profit >= 0,a.client_profit*c.price_usdt,0), 6)    as  usd_num_client_profit_dr,
					                                    toDecimal256(if(a.client_profit <  0,-1*a.client_profit*c.price_usdt,0), 6) as  usd_num_client_profit_cr,
	                                                    toDecimal256(a.manage_fee,6) as  coin_num_client_mange,
					                                    toDecimal256(if(a.manage_fee >= 0,a.manage_fee*c.price_usdt,0), 6)    as  usd_num_client_mange_dr,
					                                    toDecimal256(if(a.manage_fee <  0,-1*a.manage_fee*c.price_usdt,0), 6) as  usd_num_client_mange_cr,
	                                                    -1*(coin_num_float_profit+coin_num_client_profit+coin_num_client_mange) as  coin_num_fair_value_change,
	                                                    toDecimal256(if(a.fair_value_change >=  0,a.fair_value_change*c.price_usdt,0), 6)   as  usd_num_fair_value_change_dr,
	                                                    (usd_num_float_profit_dr+usd_num_client_profit_dr+usd_num_client_mange_dr+usd_num_fair_value_change_dr-usd_num_float_profit_cr-usd_num_client_profit_cr-usd_num_client_mange_cr) as  usd_num_fair_value_change_cr
                                            from odoo.dws_odoo_fund_month_caculate_to_odoo as a 
                                            left join df01.ods_iwala_currency  as d on a.collect_currency_id = d.currency_id 
                                            left join odoo.dwd_odoo_fund_finance_type as e on a.fund_id = e.fund_id
                                            left join odoo.dim_odoo_currency as b on d.currency_mark = b.full_name 
                                            left join df01.ods_r_d_iwala_currency_zero_price c on a.collect_currency_id = c.currency_trade_id
                                            where  a.fund_type = '保本浮息' and toDate(b.stime) =if(toDate('{}')>='2022-12-01','{}','2022-12-01') and toDate(c.stime) ='{}' and a.record_date ='{}'and a.record_date = e.record_date)
                                        """.format(self.stime.date(),self.stime.date(),self.stime.date(),self.stime.date())
            self.__ch_conn.connect()
            result = self.__ch_conn.execute(fund_caculate_safe_float_interest_sql)
            print(fund_caculate_safe_float_interest_sql)
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
                                 "ref": "<%s> Float Rate Guaranteed Fund monthly accrual"%(item[1]),
                                 "journal_id": 56,
                                 "company_id": 24,
                                 "currency_id": 2}
                    journal_entry_id = od_models.execute_kw(db_name, uid, pwd, 'account.move', 'create', [journal_entry])
                    dr_cr_journal_item_1 = {
                         'move_id': journal_entry_id,
                         'account_id': 975,
                         'amount_currency': str(item[3]),
                         'currency_id': item[0],
                         'analytic_account_id': item[2],
                         'debit': str(item[4]),
                         'credit': str(item[5])
                    }
                    journal_items.append(dr_cr_journal_item_1)
                    credit_journal_item_1 = {
                        'move_id': journal_entry_id,
                        'account_id': 4904,
                        'amount_currency': str(item[6]),
                        'currency_id': item[0],
                        'analytic_account_id': 100,
                        'debit': str(item[7]),
                        'credit': str(item[8])
                    }
                    journal_items.append(credit_journal_item_1)
                    credit_journal_item_2 = {
                        'move_id': journal_entry_id,
                        'account_id': 1087,
                        'amount_currency': str(item[9]),
                        'currency_id': item[0],
                        'analytic_account_id': 80,
                        'debit': str(item[10]),
                        'credit': str(item[11])
                    }
                    journal_items.append(credit_journal_item_2)
                    dr_cr_journal_item_2 = {
                         'move_id': journal_entry_id,
                         'account_id': 6962,
                         'amount_currency': str(item[12]),
                         'currency_id': item[0],
                         'analytic_account_id': 80,
                         'debit': str(item[13]),
                         'credit': str(item[14])
                    }
                    journal_items.append(dr_cr_journal_item_2)
                    od_models.execute_kw(db_name, uid, pwd, 'account.move.line', 'create', [journal_items])
                    # od_models.execute_kw(db_name, uid, pwd, 'account.move', 'write',
                    #                    [[journal_entry_id], {"state": "posted"}])
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
    func_list = ['sync_account_fund_caculate_safe_float_interest']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()
