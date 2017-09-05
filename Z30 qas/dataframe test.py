import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from datetime import datetime

from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
from R50_general.generalconstants import logprint

import tquant.getdata as gt

dcm_sql = dcm(echo=False)
engine = dcm_sql.getengine()
conn = dcm_sql.getconn()

#    print(dfm_stocks)
item = '300274'
market_id = 'SH' if item.startswith('6') else 'SZ'
# dfm_cur_balance = pd.read_sql_query('''select * from stock_fin_balance
#                                     where Market ID = %(market) and Stock_ID = %(stock)
#                                         '''
#                                     , conn, params={'market': market_id, 'stock':item})
dfm_cur_balance = pd.read_sql_query('''select * from stock_fin_cashflow
                                    where Market_ID = ? and Stock_ID = ?
                                        '''
                                    , conn, params=(market_id,item))

dfm_cur_balance.to_excel('temp1.xls')

try:
    ls_finreports = gt.get_financial(item)
except:
    logprint("Stock %s can't get financial information" % item)


df_balance = ls_finreports[0]
print(dfm_cur_balance['Report_Date'][0],type(dfm_cur_balance['Report_Date'][0]))
ls_index = [id.date() for id in df_balance.index]

#print (ls_index)

for id in ls_index:
    s_balance = df_balance.loc[id]
#    for col in df_balance.columns:
#        print(col,':',s_balance[col])

