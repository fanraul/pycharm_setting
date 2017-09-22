from bs4 import BeautifulSoup
import urllib.request
import pandas as pd
from pandas import Series, DataFrame
import numpy as np
from datetime import datetime

log = True

weblinks = {
    'stock_list_easymoney': 'http://quote.eastmoney.com/stocklist.html',   # obselete
    'stock_change_record_qq': 'http://stock.finance.qq.com/corp1/profile.php?zqdm=%(stock_id)s',
    'stock_category_qq': 'http://stockapp.finance.qq.com/mstats/?mod=all'
}

#table created by program dfm2table has prefix DD!
dbtables = {
    'finpreports_Tquant' :['DD_stock_fin_balance_tquant', 'DD_stock_fin_profit_tquant','DD_stock_fin_cashflow_tquant'],
    'name_hist_qq': 'DD_stock_name_change_hist_qq',
    'fixed_basic_info_tquant': 'DD_stock_fixed_basic_info_tquant',
    'basic_info1_tquant': 'DD_stock_basic_info1_tquant'
}

dbtemplate_stock_date = """
CREATE TABLE [%(table)s](
	[Market_ID] [nvarchar](50) NOT NULL,
	[Stock_ID] [nvarchar](50) NOT NULL,
	[Trans_Datetime] [datetime] NOT NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY 
(
	[Market_ID] ASC,
	[Stock_ID] ASC,
	[Trans_Datetime] ASC
))
"""

dbtemplate_stock_wo_date = """
CREATE TABLE [%(table)s](
	[Market_ID] [nvarchar](50) NOT NULL,
	[Stock_ID] [nvarchar](50) NOT NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY 
(
	[Market_ID] ASC,
	[Stock_ID] ASC
))
"""

def logprint(*args, sep=' ',  end='\n',  file=None ):
    if log:
        print(*args,sep=' ', end='\n', file=None)
        print(*args, sep=' ', end='\n', file= open('templog.txt','a'))

def get_webpage(weblink_str :str):
    req = urllib.request.Request(weblink_str)
    user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36'
    req.add_header('User-Agent', user_agent)
    req.add_header('Referer',weblink_str)
    try:
        response = urllib.request.urlopen(req)
        html = response.read()
    #print (html.decode("gb2312"))
        soup = BeautifulSoup(html,"lxml")
    #        print (soup.prettify())
        return soup
    except urllib.error.HTTPError as e:
        logprint("Weblink %s open failed:" %weblink_str,e.code,e.read().decode("utf8"))
        return None


def dfm_col_type_conversion(dfm:DataFrame,index='',columns= {}, dateformat='%Y-%m-%d'):
    """
    this function is a common function to convert the columns in dataframe as the requested data type
    :param dfm:
    :param index: the data type of index column
    :param columns: a dict of the rquested data type of columns
    :param dateformat: for datetime type, what is the original date format for conversion
    :return:
    """

    #convert index
    if index == 'datetime':
        dfm['new_index_formated_999']= dfm.index.map(lambda x: datetime.strptime(x,dateformat))
        dfm.set_index('new_index_formated_999',inplace = True)

    for col in columns:
        if col in dfm.columns:
            new_type = columns[col]
            if new_type == 'datetime':
                dfm[col] = dfm[col].map(lambda x: datetime.strptime(x,dateformat) if x != '--' else pd.NaT)
            elif 'varchar' in columns[col] or 'str' == columns[col]:
                dfm[col] = dfm[col].astype('str')
            elif 'int' in columns[col]:
                dfm[col] = dfm[col].astype('int')
            elif 'float' in columns[col] or 'double' in columns[col]:
                dfm[col] = dfm[col].astype('float')

if __name__ == "__main__":
    # print(get_cn_stocklist())
    # print(get_chars('Tquant',['FIN10','FIN20','FIN30']))
    # create_table_by_stock_date_template('hello123')
    # dict_misc_pars = {}
    # dict_misc_pars['char_origin'] = 'Tquant'
    # dict_misc_pars['char_freq'] = "D"
    # dict_misc_pars['allow_multiple'] ='N'
    # dict_misc_pars['created_by'] = 'fetch_stock_3fin_report_from_tquant'
    # dict_misc_pars['char_usage'] = 'FIN10'
    # add_new_chars_and_cols({'test1':'decimal(18,2)','test2':'decimal(18,2)'},[],'stock_fin_balance_1',dict_misc_pars)
    dfm_temp = DataFrame([{'A':'1997-1-3','B':2,'C':'12.23'},{'A':'1997-1-4','B':3,'C':'12.35'}],index=['2017-1-1','2017-1-2'])
    print(dfm_temp)
    dfm_col_type_conversion(dfm_temp,index= 'datetime',columns={'A': 'datetime','B':'varchar(50)','C':'double(18,6)'})
    print(dfm_temp)
    print(list(dfm_temp.index))
    print(type(dfm_temp.iloc[0][0]), type(dfm_temp.iloc[0][1]),type(dfm_temp.iloc[0][2]))