import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from datetime import datetime

from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
from R50_general.general_constants_funcs import logprint
import R50_general.general_constants_funcs as gc
import R50_general.dfm_to_table_common as df2db

import tquant.getdata as gt

"""
fetch bellow infos from Tquant and group them into 2 categories and them updated into db:
1) time related
公司传真
公司全称
公司电话
公司简称
公司网址
公司董秘
法定代表人
注册地址
注册资本(万元)
英文名称
行业种类
邮政编码
2) fix value
上市推荐人
上市时间
主承销商
保荐机构
发行价格（元）
发行市盈率（倍）
发行数量（万股）
发行方式
招股时间

get_brief(symbol_list)
上市公司基础资料：symbol_list = ['600100','600000','600030','000002','300314'] 参数为LIST

"""

def fetch2DB():
    # init step
    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist()
    #only for test purpose,use one stock to test.
    # dfm_stocks = df2db.get_cn_stocklist('601288')
    ls_stockids = list(dfm_stocks['Stock_ID'])

    #just to test purpose:
#    ls_stockids = ['600100','600000','600030','000002','300314']

    # print(ls_stockids)
    logprint('Getting all stocks basic info')
    dfm_stock_infos = gt.get_brief(ls_stockids)
#    print(dfm_stock_infos)
    dfm_stock_fix_infos = dfm_stock_infos[['上市推荐人','上市时间','主承销商','保荐机构','发行价格（元）','发行市盈率（倍）',
                                          '发行数量（万股）','发行方式','招股时间']]
    print(dfm_stock_fix_infos)



if __name__ == '__main__':
    fetch2DB()