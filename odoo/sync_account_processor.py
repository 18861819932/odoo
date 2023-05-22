#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import sys
import datetime

filepath = os.path.abspath(__file__)
filename = os.path.basename(filepath)
curr_dir = os.path.dirname(filepath)
bi_path = os.path.dirname(curr_dir)
if sys.path[sys.path.__len__() - 1] != bi_path:
    sys.path.append(bi_path)  # 引入新的模块路径

# sys.path.append("..")

from common.common_config import CommonConf
import xmlrpc.client
from common.util import Util
from common.util_clickhouse import ClickHouseDb


# In[ ]:


class DataProcess:

    def __init__(self, s_time, e_time, config):

        self.s_time = datetime.datetime.strptime(s_time, '%Y-%m-%d')
        self.e_time = datetime.datetime.strptime(e_time, '%Y-%m-%d')
        self.config = config
        self.pid = '{:d}'.format(os.getpid())
        self.log = Util.get_logger(self.pid)
        self.__ch_conn = ClickHouseDb('hw307')

    def go(self):

        odoo_activate = CommonConf.odoo15_conf['activate']
        odoo_url = CommonConf.odoo15_conf[odoo_activate]['odoo_url']
        db_name = CommonConf.odoo15_conf[odoo_activate]['db_name']
        user_name = CommonConf.odoo15_conf[odoo_activate]['user_name']
        pwd = CommonConf.odoo15_conf[odoo_activate]['pwd']

        try:

            query = self.config['query'].format(s_time=self.s_time, e_time=self.e_time)
            print(query)
            self.__ch_conn.connect()
            result = self.__ch_conn.execute(query)

            journal_items = []

            if result['code']:
                if len(result['data']) == 0:
                    self.log.info("no data found!")

                    return True, None

                # journal entry create
                od_common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(odoo_url))
                uid = od_common.authenticate(db_name, user_name, pwd, {})
                od_models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(odoo_url))

                journal_entry = {
                    'date': str(self.s_time.date()),
                    'ref': self.config['ref'].format(s_time=self.s_time, e_time=self.e_time),
                    'journal_id': self.config['journal_id'],
                    'company_id': self.config['company_id'],
                    'currency_id': self.config['currency_id']
                }

                journal_entry_id = od_models.execute_kw(
                    db_name, uid, pwd, 'account.move', 'create', [journal_entry])

                for item in result['data']:

                    if len(item) == 4:  # normal

                        # Debit
                        debit_journal_item = {
                            'move_id': journal_entry_id,
                            'account_id': self.config['debit']['account_id'],
                            'amount_currency': float(item[1]),
                            'currency_id': item[0],
                            'analytic_account_id': self.config['debit']['analytic_account_id'],
                            'debit': float(item[2]),
                            'name': str(item[3])
                        }
                        debit_journal_item = {k: v for k, v in debit_journal_item.items() if v != None}
                        if 'account_id' in debit_journal_item.keys():
                            journal_items.append(debit_journal_item)

                        # Credit
                        credit_journal_item = {
                            'move_id': journal_entry_id,
                            'account_id': self.config['credit']['account_id'],
                            'amount_currency': float(item[1] * -1),
                            'currency_id': item[0],
                            'analytic_account_id': self.config['credit']['analytic_account_id'],
                            'credit': float(item[2]),
                            'name': str(item[3])
                        }
                        credit_journal_item = {k: v for k, v in credit_journal_item.items() if v != None}
                        if 'account_id' in credit_journal_item.keys():
                            journal_items.append(credit_journal_item)

                    if len(item) == 7:  # special

                        journal_item_temp = {
                            'move_id': journal_entry_id,
                            'account_id': item[5],
                            'amount_currency': float(item[1]),
                            'currency_id': item[0],
                            'analytic_account_id': item[6] if item[6] != 0 else None,
                            'debit' if item[4] == 'Debit' else 'credit': abs(float(item[2])),
                            'name': str(item[3])
                        }
                        journal_item_temp = {k: v for k, v in journal_item_temp.items() if v != None}
                        if 'account_id' in journal_item_temp.keys():
                            journal_items.append(journal_item_temp)

                rounding_diff = round(
                    (abs(sum(list(map(lambda x: round(x['debit'], 2) if 'debit' in x.keys() else 0, journal_items)))) -
                     abs(sum(
                         list(map(lambda x: round(x['credit'], 2) if 'credit' in x.keys() else 0, journal_items))))),
                    2
                )
                if rounding_diff != 0:
                    for i in journal_items:
                        if '[Rounding Diff]' in i['name']:
                            i['debit'] -= rounding_diff

                od_models.execute_kw(
                    db_name, uid, pwd, 'account.move.line', 'create', [journal_items])
            # od_models.execute_kw(
            #     db_name, uid, pwd, 'account.move', 'write',
            #     [[journal_entry_id], {"state": "posted"}])
            else:
                self.log.info("sql execute error: {}".format(result['errmsg']))
                return False, None

        except Exception as ex:
            self.log.error("odoo request Exception:%s", ex)
            return False, None

        return True, None


# In[ ]:


def main():
    s_time = sys.argv[1]
    e_time = sys.argv[2]
    config_path = sys.argv[3]

    #     s_time = '2022-12-01'
    #     e_time = '2022-12-02'
    #     config_path = 'ysl_contract_detail'

    with open('query_config/{}.txt'.format(config_path), 'r') as f:
        res = f.read()
        config = eval(res)

    dp = DataProcess(s_time, e_time, config)
    func_list = ['go']
    for func in func_list:
        rst_code, rst_data = getattr(dp, func)()
        if not rst_code:
            dp.log.info(rst_data)
            sys.exit(1)
    dp.log.info('数据处理完成')


# In[ ]:


if __name__ == '__main__':
    main()
