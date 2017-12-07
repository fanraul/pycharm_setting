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
from sqlalchemy.exc import IntegrityError

global_module_name = 'fetch_stock_news_list_from_jd'
general_pages_to_fetch = 5500
general_start_page = 0
general_pages_to_split = 100

def fetch2DB():
    # create jd news table
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'JD'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'Y'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'NEWS'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['newslist_jd']
    df2db.create_table_by_template(table_name,table_type='jd_newslist')

    # update every general_pages_to_split pages
    ls_dfmnews = []
    webscrap_times = general_pages_to_fetch // general_pages_to_split
    try:
        for i in range(webscrap_times):
            start_page_cur = i * general_pages_to_split + general_start_page
            pages_to_fetch_cur = general_pages_to_split

            dfm_newslist = parse_newslist(pages_to_fetch_cur,start_page_cur)
            dfm_newslist.drop_duplicates(subset = ['News_ID'],inplace=True)

            if len(dfm_newslist) > 0:
                dfm_newslist['Region_ID'] = 'CN'
                df2db.dfm_to_db_insert_or_update(dfm_newslist, ['Region_ID','News_ID'], table_name, global_module_name, process_mode='wo_update')
            ls_dfmnews.append(dfm_newslist)
    # except IntegrityError:
    finally:
        pd.concat(ls_dfmnews).to_excel('newslist.xls')

def parse_newslist(pages,start_page)-> DataFrame  :
    logprint('Get JD stock news from pages %s to pages %s:' %(start_page,start_page+pages-1))
    ls_dfmnewslist = []
    for i in range(pages):
        url_newslist_page = R50_general.general_constants.weblinks['stock_newslist_jd'] %(i+start_page)
        url_prefix_newsdetail = R50_general.general_constants.weblinks['jd_stock_news_details_prefix']
        soup_newslist = gcf.get_webpage(url_newslist_page)
        if soup_newslist:
            body_newslist = soup_newslist.find_all('li',class_ = "clearfix")
            for newslink in body_newslist:
                str_news_link = newslink.div.a.get('href').strip()
                dt_newslink ={}
                dt_newslink['Title'] = newslink.div.a.string.strip()
                dt_newslink['Weblink'] = url_prefix_newsdetail + str_news_link
                dt_newslink['News_datetime'] = datetime.strptime('20'+ newslink.find_all('span',"date-time")[0].string.strip(),'%Y-%m-%d %H:%M')
                dt_newslink['News_ID'] = re.findall('detail-([0-9]+).html',str_news_link)[0]
                ls_dfmnewslist.append(dt_newslink)
    return DataFrame(ls_dfmnewslist)

    # for index,row in dfm_newslist.iterrows():


'''
    <li class="clearfix">
        <div class="c-no-le ">
            <a href="/info/detail-106208.html" clstag="jr|keycount|gupiao_zixun|rmzx_uszx1"  class="c-li-title" target="_blank">涨停板预测:今日22股有望涨停！ </a>
            <p class="c-ke-text"></p>
            <p class="c-date-com">
                <span class="date-time">&nbsp;17-12-01 00:19</span>
                <span class="come">来源: 京东股票</span>
                <span class="comment">1条评论</span>
            </p>
        </div>
    </li>
'''

if __name__ == '__main__':
    fetch2DB()