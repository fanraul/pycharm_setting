"""
得到一个指数下面所有的股票信息
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


def fututry(ip, port):
    quote_ctx = OpenQuoteContext(ip, port)
    # ret, data_frame = quote_ctx.get_plate_stock(strcode)
    ret_code, ret_data = quote_ctx.get_plate_list('SZ', 'ALL')

    # ret_code, ret_data = quote_ctx.get_plate_stock('SH.BK0180')

    # ret_code, ret_data = quote_ctx.get_stock_basicinfo('US', stock_type='STOCK')

    gcf.dfmprint(ret_data)

    quote_ctx.close()

if __name__ == "__main__":
    # print(get_index_stocks(api_ip, api_port, 'HK.800000'))
    # print(gcf.get_last_trading_day('HK'))
    fututry(api_ip,api_port)
