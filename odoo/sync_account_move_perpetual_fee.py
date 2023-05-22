#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2023/01/14 11:15 PM
作者    : revis
功能    : move合约手续费
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

    def sync_move_perpetual_fee(self):
        odoo_activate = CommonConf.odoo15_conf['activate']
        odoo_url = CommonConf.odoo15_conf[odoo_activate]['odoo_url']
        db_name = CommonConf.odoo15_conf[odoo_activate]['db_name']
        user_name = CommonConf.odoo15_conf[odoo_activate]['user_name']
        pwd = CommonConf.odoo15_conf[odoo_activate]['pwd']
        try:
            # 查询dft抵扣明细 生成 journal item
            move_perpetual_fee_sql = """
            select  b.id,
            round(cast(fee_usdt as double), 6) as  coin_num,
            round(cast(fee_usdt as double), 6) as usd_num          
            from (SELECT 104 as currency_id,cast(sum(if(is_inverse = 0,toDecimal128(fee,16),toDecimal128(toDecimal256(fee,16) * toDecimal256(price,16),16))) AS float) as fee_usdt                                                                                                                                             
                from                                                                                                                                                                      
                (SELECT price,fee,bbb.is_inverse                                                                                                         
                    from df01.ods_perpetual_trade_user as a                                                                                                                               
                    left join df01.ods_perpetual_instrument as bbb                                                                                                                        
                        on a.instrument_id=bbb.instrument_id                                                                                                                              
                    where type=2 -- 真实合约                                                                                                                                                  
                    and contract_type = 1                                                                                                                                                 
                    and participant_id<>force_close_participant_id                                                                                                                        
                    and participant_id not in (select toUInt32(arrayJoin(splitByChar(',',vip_participant_ids))) from df01.ods_perpetual_instrument)                                       
                    and toDate(stime) = '{}')
                union ALL 
                SELECT   104 as currency_id,cast( abs(sum(money)) AS float)  AS fee                                                                                                                              
                FROM  df01.ods_perpetual_finance                                                                                                                                    
                WHERE finance_type IN (31,32) and toDate(insert_time) = '{}'
                having fee >0) as a
            left join df01.ods_iwala_currency  as d on a.currency_id = d.currency_id 
            left join odoo.dim_odoo_currency as b on d.currency_mark = b.full_name  
            where b.stime = '{}'
            """.format(self.stime.date(),self.stime.date(),self.stime.date())
            self.__ch_conn.connect()
            result = self.__ch_conn.execute(move_perpetual_fee_sql)
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
                                 "ref": "tx fee charged on move-contract",
                                 "journal_id": 53,
                                 "company_id": 24,
                                 "currency_id": 2}
                journal_entry_id = od_models.execute_kw(db_name, uid, pwd, 'account.move', 'create', [journal_entry])
                for item in result['data']:
                    debit_journal_item = {
                         'move_id': journal_entry_id,
                         'account_id': 4901,
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
                        'analytic_account_id': 79,
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
    func_list = ['sync_move_perpetual_fee']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


if __name__ == '__main__':
    main()
