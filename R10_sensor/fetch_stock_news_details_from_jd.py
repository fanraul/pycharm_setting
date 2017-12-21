import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import re

from datetime import datetime

import R50_general.advanced_helper_funcs as ahf
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint
import R50_general.dfm_to_table_common as df2db


global_module_name = 'fetch_stock_news_details_from_jd'
flg_error_reprocess = ''

def fetch2FILE():
    table_name = R50_general.general_constants.dbtables['newslist_jd']
    dfm_cond = DataFrame([{'db_col':'News_downloaded','db_oper':'is','db_val':"NULL"},
                          {'db_col':'Ind_useless', 'db_oper': 'is', 'db_val': "NULL"},])

    dfm_newslist = df2db.get_data_from_DB(table_name,dfm_cond)

    if flg_error_reprocess == 'X':
        dfm_cond_error = DataFrame([{'db_col':'News_downloaded','db_oper':'=','db_val':"'E'"},
                          {'db_col':'Ind_useless', 'db_oper': 'is', 'db_val': "NULL"},])
        dfm_newslist_error = df2db.get_data_from_DB(table_name, dfm_cond_error)
        dfm_newslist = pd.concat([dfm_newslist,dfm_newslist_error])

    for index,row in dfm_newslist.iterrows():
        url_news = row['Weblink']
        html_news_item = gcf.get_webpage_with_retry(url_news,flg_return_rawhtml=True,time_wait=5)
        if html_news_item:
            soup_news_item=BeautifulSoup(html_news_item,"lxml")
            if len(soup_news_item.find_all('article')) > 0:
                # there is article session in the html
                filename_news = R50_general.general_constants.Global_path_news_details_jd + row['News_ID']+'.html'
                file_news = open(filename_news,'wb')
                file_news.write(html_news_item)
                file_news.close()
                dfm_newslist_upt = DataFrame([{'Region_ID':row['Region_ID'],'News_ID':row['News_ID'],
                                              'News_downloaded':'X'}])
                df2db.dfm_to_db_insert_or_update(dfm_newslist_upt, ['Region_ID', 'News_ID'], table_name, global_module_name,
                                                 process_mode='w_update')

                logprint('News %s is downloaded' %(row['News_ID']))
            else:
                dfm_newslist_upt = DataFrame([{'Region_ID':row['Region_ID'],'News_ID':row['News_ID'],
                                              'News_downloaded':'E'}])
                df2db.dfm_to_db_insert_or_update(dfm_newslist_upt, ['Region_ID', 'News_ID'], table_name, global_module_name,
                                                 process_mode='w_update')

                logprint('News %s can not downloaded!' %(row['News_ID']),add_log_files='I')


if __name__ == "__main__":
    # flg_error_reprocess = 'X'
    # fetch2FILE()
    table_name = R50_general.general_constants.dbtables['newslist_jd']

    dfm_newslist = df2db.get_data_from_DB(table_name,DataFrame())

    dfm_newslist['duplicated'] = dfm_newslist.duplicated(subset=['News_datetime','Title'])

    dfm_newslist.to_excel('newslistDB.xls')