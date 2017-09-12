import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import urllib.request
import re

from datetime import datetime

from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
import R50_general.general_constants_funcs as gcf
from R50_general.general_constants_funcs import logprint
import R50_general.dfm_to_table_common as df2db

def fetch2DB(mode:str = ''):
    """
    fetch stock name history and update into DB
    :param mode: if mode = 'update_log" then delete all entries in YY_stock_changes_qq,otherwise only update name changes
    :return:
    """
    # init step
    # step2.1: get current stock list
    #dfm_stocks = df2db.get_cn_stocklist()
    #only for test purpose,use one stock to test.
    dfm_stocks = df2db.get_cn_stocklist('300018')

    #get chars for name change hist
    dfm_db_chars = df2db.get_chars('QQ', ['NAME_HIST'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'QQ'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] ='N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] ='fetch_stock_change_record_from_qq'
    dict_misc_pars['char_usage'] = 'NAME_HIST'
    table_name = gcf.dbtables['name_hist_qq']
    df2db.create_table_by_stock_date_template(table_name)

    ls_changes =[]



    # step2:loop at stock list and fetch and save to DB
    for item in dfm_stocks['Stock_ID']:         # get column Stock_ID from dataframe
        # Step1: parsing webpage and produce stock list
        url_stock_profile = gcf.weblinks['stock_change_record_qq'] %{'stock_id':item}
        soup_profile = gcf.get_webpage(url_stock_profile)
        if soup_profile:
            dfm_item_changes = soup_parse(soup_profile)
            gcf.dfm_col_type_conversion(dfm_item_changes,columns={'变动日期':'datetime','公布日期':'datetime'})
            dfm_item_name_changes = dfm_item_changes[dfm_item_changes['变动项目']=='证券简称']
            # print(dfm_item_name_changes)
            dfm_item_name_changes=dfm_item_name_changes.set_index('变动日期')
            dfm_item_name_changes = dfm_item_name_changes.rename(columns={'公布前内容':'原证券简称',
                                                  '公布后内容':'证券简称',
                                                  '公布日期':'简称变动公布日期'})
            del(dfm_item_name_changes['变动项目'])
            dict_cols_cur = {'证券简称': 'varchar(10)',
                             '简称变动公布日期':'datetime',
                             '原证券简称':'varchar(10)'}
            if len(dfm_item_name_changes) > 0:
                df2db.add_new_chars_and_cols(dict_cols_cur,list(dfm_db_chars['Char_ID']),table_name,dict_misc_pars)
                # step4: insert transaction data into transaction table
                market_id = 'SH' if item.startswith('6') else 'SZ'
                df2db.load_dfm_to_db_by_mkt_stk_w_hist(market_id, item, dfm_item_name_changes, table_name, dict_misc_pars,
                                                       processing_mode='w_update')
            dfm_item_changes['Stock_ID'] = item
            dfm_item_changes['Market_ID'] = 'SH' if item.startswith('6') else 'SZ'
            ls_changes.append(dfm_item_changes)
            # print (dfm_item_name_changes )

    if ls_changes and mode == 'update_log':
        dfm_changes = pd.concat(ls_changes)
        df2db.load_snapshot_dfm_to_db(dfm_changes,'YY_stock_changes_qq')


            #fetch2DB_individual(item,dfm_db_chars)               # item is str type

def soup_parse(soup:BeautifulSoup):
    body_content = soup.find_all('table',class_= 'list')
    # print (body_content[0])
    stock_changes = body_content[1].find_all('tr')
    # print(stock_profile[0].td)
    # print(stock_profile[1].contents)
    stock_changes = stock_changes[2:] if len(stock_changes) > 2 else []
    ls_changes =[]
    for tr in stock_changes:
        change_record = {}
        ls_change_item = list(tr.stripped_strings)
        change_record['变动项目'] = ls_change_item[0].strip()
        change_record['变动日期'] = ls_change_item[1].strip()
        change_record['公布日期'] = ls_change_item[2].strip()
        change_record['公布前内容'] = ls_change_item[3].strip()
        change_record['公布后内容'] = ls_change_item[4].strip()
        ls_changes.append(change_record)

    return DataFrame(ls_changes)


if __name__ ==  '__main__':
    fetch2DB('update_log')