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

global_module_name = 'fetch_stock_news_from_jd'


def fetch_newslist(pages):
    logprint('Get JD stock news first %s pages:' %pages)
    ls_dfmnewslist = []
    for i in range(pages):
        url_newslist_page = R50_general.general_constants.weblinks['stock_newslist_jd'] %(i+1)
        url_prefix_newsdetail = R50_general.general_constants.weblinks['jd_stock_news_details_prefix']
        soup_newslist = gcf.get_webpage(url_newslist_page)
        if soup_newslist:
            body_newslist = soup_newslist.find_all('li',class_ = "clearfix")
            for newslink in body_newslist:
                dt_newslink ={}
                dt_newslink['title'] = newslink.div.a.string.strip()
                dt_newslink['weblink'] = url_prefix_newsdetail + newslink.div.a.get('href').strip()
                dt_newslink['news_datetime'] = newslink.find_all('span',"date-time")[0].string.strip()
                ls_dfmnewslist.append(dt_newslink)
    dfm_newslist = DataFrame(ls_dfmnewslist)
    dfm_newslist.to_excel('newslist.xls')

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
    fetch_newslist(50)