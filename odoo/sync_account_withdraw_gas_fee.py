#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2022/12/14 8:14 PM
作者    : wushuang
功能    : "统计D网提币等被扣第三方平台账户手续费 gasfee for withdrawing"
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

    def sync_withdraw_gas_fee(self):
        odoo_activate = CommonConf.odoo15_conf['activate']
        odoo_url = CommonConf.odoo15_conf[odoo_activate]['odoo_url']
        db_name = CommonConf.odoo15_conf[odoo_activate]['db_name']
        user_name = CommonConf.odoo15_conf[odoo_activate]['user_name']
        pwd = CommonConf.odoo15_conf[odoo_activate]['pwd']
        try:
            # 查询充值明细 生成 journal item
            withdraw_gas_sql = """
                with gas_fee_detail as (select 
                    b.id,
                    b.name,
                    fee_mark,
                    total_gas_fee
                from (select
                    fee_mark,
                    sum(gas_fee) as total_gas_fee
                from
                    df01.ods_wallet_consumed_gasfee
                where
                    block_time >= '{}'
                    and block_time < '{}'
                    and transfer_type = 1
                group by
                    fee_mark) a inner join odoo.dim_odoo_currency b on a.fee_mark = b.full_name 
                where b.stime = '{}' and position(b.name, '-') > 0)
                select 
                    gd.id,
                    round(cast(gd.total_gas_fee as double), 6) as gas_fee_amount,
                    round(cast(gd.total_gas_fee * dr.to_usd_rate as double), 6) as usd_gas_fee
                from gas_fee_detail gd join df02.dim_currency_exchange_rate dr on gd.fee_mark = dr.currency_mark
                where dr.stime = '{}'
                """.format(self.stime, self.etime, self.etime.date(), self.etime.date())
            self.log.info(withdraw_gas_sql)
            self.__ch_conn.connect()
            result = self.__ch_conn.execute(withdraw_gas_sql)
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
                                 "ref": "withdraw gasfee",
                                 "journal_id": 50,
                                 "company_id": 24,
                                 "currency_id": 2}
                journal_entry_id = od_models.execute_kw(db_name, uid, pwd, 'account.move', 'create', [journal_entry])
                for item in result['data']:
                    debit_journal_item = {
                         'move_id': journal_entry_id,
                         'account_id': 1185,
                         'currency_id': item[0],
                         'analytic_account_id': 84,
                         'amount_currency': float(item[1]),
                         'debit': float(item[2])
                    }
                    journal_items.append(debit_journal_item)
                    credit_journal_item = {
                        'move_id': journal_entry_id,
                        'account_id': 974,
                        'currency_id': item[0],
                        'analytic_account_id': 100,
                        'amount_currency': float(item[1]*-1),
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
    etime = sys.argv[2]
    dp = DataProcess(stime, etime)
    func_list = ['sync_withdraw_gas_fee']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()

