import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import urllib.request
import re

from datetime import datetime

import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint
import R50_general.dfm_to_table_common as df2db
import json
import R50_general.advanced_helper_funcs as ahf

global_module_name = 'fetch_stock_shareholder_from_eastmoney'


def fetch2DB(stockid:str = ''):
    # 1.init step
    # create DD tables for data store and add chars for stock structure.
    # 1.1 setup DB structure for core concept table
    dict_misc_pars_sh_num = gcf.set_dict_misc_pars('EASTMONEY','D','N',global_module_name,'SHAREHOLDER')   #股东人数
    dict_misc_pars_tensh_tradable = gcf.set_dict_misc_pars('EASTMONEY','D','Y',global_module_name,'SHAREHOLDER') #十大流通股东
    dict_misc_pars_tensh = dict_misc_pars_tensh_tradable.copy() #十大股东
    dict_misc_pars_tensh_changes = dict_misc_pars_tensh_tradable.copy() #十大股东持股变动
    dict_misc_pars_funds = dict_misc_pars_tensh_tradable.copy() #基金持股
    dict_misc_pars_nontradable_rls = dict_misc_pars_sh_num.copy()   #限售解禁

    # check whether db table is created.
    # 股东人数
    dfm_db_chars_sh_sum = df2db.get_chars('EASTMONEY', ['SHAREHOLDER10'])
    table_name_sh_num = R50_general.general_constants.dbtables['stock_shareholder_number_eastmony']
    df2db.create_table_by_template(table_name_sh_num,table_type='stock_date')
    dict_cols_sh_num = {'股东人数': 'int',
                         '较上期变化%': 'decimal(10,6)',
                         '筹码集中度': 'nvarchar(50)',
                         '股价':'decimal(10,2)',
                         '前十大股东持股合计%':'decimal(10,6)',
                         '前十大流通股东持股合计%':'decimal(10,6)',
                         }
    df2db.add_new_chars_and_cols(dict_cols_sh_num, list(dfm_db_chars_sh_sum['Char_ID']), table_name_sh_num, dict_misc_pars_sh_num)

    # 十大流通股东
    dfm_db_chars_tensh_tradable = df2db.get_chars('EASTMONEY', ['SHAREHOLDER20'])
    table_name_tensh_tradable = R50_general.general_constants.dbtables['stock_top_ten_tradable_shareholder_eastmoney']
    df2db.create_table_by_template(table_name_tensh_tradable, table_type='stock_date_multi_value')
    dict_cols_tensh_tradable = { '名次': 'int',
                                 '股东名称':'nvarchar(50)',
                                 '股东性质':'nvarchar(50)',
                                 '股份类型':'nvarchar(50)',
                                 '持股数':'int',
                                 '占总流通股本持股比例':'decimal(10,6)',
                                 '增减股数':'int',
                                 '变动比例':'decimal(10,6)',
                                 }
    df2db.add_new_chars_and_cols(dict_cols_tensh_tradable, list(dfm_db_chars_tensh_tradable['Char_ID']), table_name_tensh_tradable, dict_misc_pars_tensh_tradable)

    #十大股东
    dfm_db_chars_tensh = df2db.get_chars('EASTMONEY', ['SHAREHOLDER30'])
    table_name_tensh = R50_general.general_constants.dbtables['stock_top_ten_shareholder_eastmoney']
    df2db.create_table_by_template(table_name_tensh, table_type='stock_date_multi_value')
    dict_cols_tensh = { '名次': 'int',
                     '股东名称':'nvarchar(50)',
                     '股份类型':'nvarchar(50)',
                     '持股数':'int',
                     '占总流通股本持股比例':'decimal(10,6)',
                     '增减股数':'int',
                     '变动比例':'decimal(10,6)',
                     }
    df2db.add_new_chars_and_cols(dict_cols_tensh, list(dfm_db_chars_tensh['Char_ID']), table_name_tensh, dict_misc_pars_tensh)

    # 十大股东持股变动
    dfm_db_chars_tensh_changes = df2db.get_chars('EASTMONEY', ['SHAREHOLDER40'])
    table_name_tensh_changes = R50_general.general_constants.dbtables['stock_top_ten_shareholder_shares_changes_eastmoney']
    df2db.create_table_by_template(table_name_tensh_changes, table_type='stock_date_multi_value')
    dict_cols_tensh_changes = {'变动时间':'datetime',
                               '名次': 'int',
                                '股东名称':'nvarchar(50)',
                                '股份类型':'nvarchar(50)',
                                '持股数':'int',
                                '占总流通股本持股比例':'decimal(10,6)',
                                '增减股数':'int',
                                '增减股占原股东持股比例':'decimal(10,6)',
                                '变动原因':'nvarchar(50)',
                               }

    df2db.add_new_chars_and_cols(dict_cols_tensh_changes, list(dfm_db_chars_tensh_changes['Char_ID']),
                                 table_name_tensh_changes, dict_misc_pars_tensh_changes)

    # 基金持股
    dfm_db_chars_funds = df2db.get_chars('EASTMONEY', ['SHAREHOLDER50'])
    table_name_funds = R50_general.general_constants.dbtables['stock_fund_shareholder_eastmoney']
    df2db.create_table_by_template(table_name_funds, table_type='stock_date_multi_value')
    dict_cols_funds = {'名次': 'int',
                        '基金代码':'nvarchar(50)',
                        '基金名称':'nvarchar(50)',
                        '持股数':'int',
                        '持仓市值':'decimal(18,2)',
                        '占总股本比':'decimal(10,6)',
                        '占流通比':'decimal(10,6)',
                        '占净值比':'decimal(10,6)',
                       }

    df2db.add_new_chars_and_cols(dict_cols_funds, list(dfm_db_chars_funds['Char_ID']),
                                 table_name_funds, dict_misc_pars_funds)

    # 限售解禁
    dfm_db_chars_nontradable_rls = df2db.get_chars('EASTMONEY', ['SHAREHOLDER60'])
    table_name_nontradable_rls = R50_general.general_constants.dbtables['stock_nontradable_shares_release_eastmoney']
    df2db.create_table_by_template(table_name_nontradable_rls, table_type='stock_date')
    dict_cols_nontradable_rls = {'解禁股数': 'int',
                                 '股票类型':'nvarchar(50)',
                                 '解禁股占总股本比例':'decimal(10,6)',
                                 '解禁股占流通股本比例':'decimal(10,6)',
                                 }

    df2db.add_new_chars_and_cols(dict_cols_nontradable_rls, list(dfm_db_chars_nontradable_rls['Char_ID']),
                                 table_name_nontradable_rls, dict_misc_pars_nontradable_rls)


    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' % row['Stock_ID'])
        # step1:parse webpage and get raw data
        url_stock_cpt = R50_general.general_constants.weblinks['stock_core_concept_eastmoney'] % row['MktStk_ID']
        json_stock_cpt = gcf.get_webpage(url_stock_cpt,flg_return_json=True)
        dfm_stk_cpts = soup_parse_stock_cpt(json_stock_cpt)
        if len(dfm_stk_cpts) > 0:
            df2db.load_dfm_to_db_multi_value_by_mkt_stk_cur(row['Market_ID'],
                                                            row['Stock_ID'],
                                                            dfm_stk_cpts,
                                                            table_name,
                                                            dict_misc_pars,
                                                            process_mode='w_check')
        else:
            logprint("Stock %s doesn't have core concept assigned" % (row['MktStk_ID']))


        # TODO: error handling
        # assert len(dfm_stk_strc) > 0, 'No stock structure details can be found for stockid %s' % row['Stock_ID']
        #
        # # step2: format raw data into prop data type
        # dfm_stk_strc['股本单位'] = '万股'
        # gcf.dfmprint(dfm_stk_strc)
        #
        # # formating
        # dfm_fmt_stk_strc = format_dfm_stk_strc(dfm_stk_strc)
        # # only one entry allowed in one day, so need to combine multiple changes in one day into one entry
        # process_duplicated_entries(dfm_fmt_stk_strc, row['Stock_ID'])
        # gcf.dfmprint(dfm_fmt_stk_strc)
        # market_id = 'SH' if row['Stock_ID'].startswith('6') else 'SZ'
        # df2db.load_dfm_to_db_by_mkt_stk_w_hist(market_id, row['Stock_ID'], dfm_fmt_stk_strc, table_name,
        #                                        dict_misc_pars,
        #                                        processing_mode='w_update')
        # time.sleep(10)



def soup_parse_stock_cpt(json_stock_cpt:str):
    dt_json_stkcpt = json.loads(json_stock_cpt)
    # print(dt_soup_stkcpt)
    ls_cpts = []
    if dt_json_stkcpt:
        ls_hxtc = dt_json_stkcpt['hxtc']
        for item in ls_hxtc:
            dt_cpt = {}
            dt_cpt['关键词'] = item['gjc'].strip()
            dt_cpt['要点内容'] = item['ydnr'].strip()
            ls_cpts.append(dt_cpt)
            # print(len(dt_cpt['关键词']),',',len(dt_cpt['要点内容']))
        return DataFrame(ls_cpts)
    else:
        return DataFrame()

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call=fetch2DB, wait_seconds=60)

if __name__ == '__main__':
    fetch2DB('002050')
    # auto_reprocess()