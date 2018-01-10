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

    dfm_stocklist = enum_all_stocks(api_ip, api_port,'STOCK')
    dfm_stocklist.to_excel('stocklist.xls')

    dfm_stocklist = enum_all_stocks(api_ip, api_port,'IDX')
    dfm_stocklist.to_excel('idx.xls')





