#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日期    : 2022/7/11 4:03 PM
作者    : wushuang
功能    : 
参数    : 
"""

import os, sys

filepath = os.path.abspath(__file__)
filename = os.path.basename(filepath)
curr_dir = os.path.dirname(filepath)
bi_path = os.path.dirname(curr_dir)
if sys.path[sys.path.__len__() - 1] != bi_path:
    sys.path.append(bi_path)  # 引入新的模块路径
from common.util_clickhouse import ClickHouseDb
import xmlrpc.client

if __name__ == "__main__":
    # ch = ClickHouseDb('hw307')
    # ch.connect()
    # rst = ch.execute("select * from df01.ods_iwala_member limit 1")
    # print(rst['data'])
    # df = pd.DataFrame(columns=['stime', 'key', 'value'],
    #                  data=rst['data'])
    # df['stime'] = pd.to_datetime(df['stime']).dt.tz_localize('Asia/Shanghai')
    # df['stime'] = np.where(df['stime'] == pd.to_datetime('1970-01-01'), pd.NaT, df['stime'])
    # print(df.to_dict('records'))
    # ch.batch_insert("insert into test.t_test VALUES ", df.to_dict('records'))
    odoo_url = "https://od.digifinex.org"
    db_name = "odoo15backup"
    user_name = "york_xiong@digifinex.org"
    pwd = "f2b5ac33192d1acae52c5b42cbd12747f5ebe6dc"
    common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(odoo_url))
    # common.version()
    uid = common.authenticate(db_name, user_name, pwd, {})
    # print('uid:', uid)
    od_models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(odoo_url))
    models = od_models.execute_kw(db_name, uid, pwd, 'account.account', 'search_read',
                                  [[['company_id', '=', 24]]], {"fields": ["id", "code", "display_name"]})
    # models = od_models.execute_kw(db_name, uid, pwd, 'res.company', 'search_read',
    #                                 [], {"fields": ["id", "name"]})
    # models = od_models.execute_kw(db_name, uid, pwd, 'account.analytic.account', 'search_read',
    #                                                                 [[['company_id', '=', 24]]], {"fields": ["id", "display_name"]})
    # models = od_models.execute_kw(db_name, uid, pwd, 'account.journal', 'search_read',
    #                              [[['company_id', '=', 24]]], {"fields": ["id", "display_name"]})
    print(models)
    # 创建 currency rate
    # cu_rate = {'name': '2022-12-01',
    #           'inverse_company_rate': '1788.00012', 'currency_id': 1851, 'company_id': 24}
    # od_models.execute_kw(db_name, uid, pwd, 'res.currency.rate', 'create', [cu_rate])

    # 创建 entry
    # journal_entry = {"date": "2022-11-29", "ref": "User deposit 20221128-20221129",
    #                 "journal_id": 183, "company_id": 24, "currency_id": 2}
    # journal_entry_id = od_models.execute_kw(db_name, uid, pwd, 'account.move', 'create', [journal_entry])

    # l1 = od_models.execute_kw(db_name, uid, pwd, 'account.move.line', 'create',
    #                           [[{'move_id': journal_entry_id,
    #                              'account_id': 3095,
    #                              'amount_currency': 2,
    #                              'currency_id': 1851,
    #                              'analytic_account_id': 89,
    #                              'debit': 4000
    #                              }, {'move_id': journal_entry_id,
    #                                  'account_id': 3184,
    #                                  'amount_currency': -2,
    #                                  'currency_id': 1851,
    #                                  'analytic_account_id': 89,
    #                                  'credit': 4000
    #                                  }]])
    # 更新状态 为posted
    # od_models.execute_kw(db_name, uid, pwd, 'account.move', 'write', [[journal_entry_id], {"state": "posted"}])
    # print(mid)
