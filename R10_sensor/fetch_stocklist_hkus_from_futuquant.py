"""
得到US&HK所有的股票信息
"""
from futuquant.open_context import *
from R50_general.general_constants import futu_api_ip as api_ip
from R50_general.general_constants import futu_api_port as api_port
import pandas as pd
from pandas import Series, DataFrame
import numpy as np

import re

from datetime import datetime

from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
from R50_general.general_helper_funcs import logprint
import R50_general.dfm_to_table_common as df2db
import R50_general.advanced_helper_funcs as ahf
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf

global_module_name = 'fetch_stocklist_hkus_from_futuquant'

def fetch2DB():
    dfm_db_chars = df2db.get_chars('FUTUQUANT', ['STOCKLIST'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'FUTUQUANT'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'STOCKLIST'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['stocklist_hkus_futuquant']
    df2db.create_table_by_template(table_name, table_type='stock_date')
    dict_cols_cur = {'Stock_Name': 'nvarchar(200)',
                     'lot_size': 'int',
                     'sec_type': 'nvarchar(2)',
                     'stockid_futuquant': 'nvarchar(40)',
                     }
    df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)


    # get all stock in CN,US,HK
    dfm_stocklist_stk = enum_all_stocks(api_ip, api_port,'STOCK')
    # get all index in CN,US,HK
    dfm_stocklist_idx = enum_all_stocks(api_ip, api_port,'IDX')

    dfm_stocklist_all = pd.concat([dfm_stocklist_stk,dfm_stocklist_idx])

    ls_dfm_stocklist = []
    for index, row in dfm_stocklist_all.iterrows():
        dt_stock ={}
        ls_code = row['code'].split('.')
        if len(ls_code)==2:
            dt_stock['Market_ID'],dt_stock['Stock_ID'] = tuple(ls_code)
        elif len(ls_code)>2 :
            dt_stock['Market_ID']= ls_code[0]
            dt_stock['Stock_ID'] = '.'.join(ls_code[1:])
        else:
            assert 0==1,'Wrong stock code: %s' %ls_code
        dt_stock['Stock_Name'] = row['name']
        if row['stock_type'] == 'STOCK':
            dt_stock['sec_type'] = '1'
        elif row['stock_type'] == 'IDX':
            dt_stock['sec_type'] = '3'
        dt_stock['lot_size'] = row['lot_size']
        dt_stock['Trans_Datetime'] = datetime.strptime(row['listing_date'],'%Y-%m-%d')
        dt_stock['stockid_futuquant']=row['stockid']
        ls_dfm_stocklist.append(dt_stock)

    dfm_stocklist = DataFrame(ls_dfm_stocklist)
    # step2: format data into prop data type
    gcf.dfm_col_type_conversion(dfm_stocklist, columns=dict_cols_cur, dateformat='%Y-%m-%d')
    dfm_stocklist_all.to_excel('stocklist.xls')

    key_cols = ['Market_ID','Stock_ID','Trans_Datetime']
    df2db.dfm_to_db_insert_or_update(dfm_stocklist, key_cols, table_name, global_module_name,
                                     process_mode='w_update')

def enum_all_stocks(ip, port,stock_type):
    quote_ctx = OpenQuoteContext(ip, port)

    ret, dfm_stocklist_sh = quote_ctx.get_stock_basicinfo(market='SH', stock_type=stock_type)
    if ret == RET_ERROR:
        assert 0==1, 'Error during fetch %s list of SH, error message: %s' %(stock_type,dfm_stocklist_sh)

    ret, dfm_stocklist_sz = quote_ctx.get_stock_basicinfo(market='SZ', stock_type=stock_type)
    if ret == RET_ERROR:
        assert 0==1, 'Error during fetch %s list of SZ, error message: %s' %(stock_type,dfm_stocklist_sz)

    ret, dfm_stocklist_hk = quote_ctx.get_stock_basicinfo(market='HK', stock_type=stock_type)
    if ret == RET_ERROR:
        assert 0==1, 'Error during fetch %s list of HK, error message: %s' %(stock_type,dfm_stocklist_hk)

    ret, dfm_stocklist_us = quote_ctx.get_stock_basicinfo(market='US', stock_type=stock_type)
    if ret == RET_ERROR:
        assert 0==1, 'Error during fetch %s list of US, error message: %s' %(stock_type,dfm_stocklist_us)

    quote_ctx.close()

    return pd.concat([dfm_stocklist_sh,dfm_stocklist_sz,dfm_stocklist_hk,dfm_stocklist_us])


if __name__ == "__main__":
    # enum_all_index(api_ip, api_port)

    # dfm_stocklist = enum_all_stocks(api_ip, api_port,'STOCK')
    # dfm_stocklist.to_excel('stocklist.xls')
    #
    # dfm_stocklist = enum_all_stocks(api_ip, api_port,'IDX')
    # dfm_stocklist.to_excel('idx.xls')

    fetch2DB( )





