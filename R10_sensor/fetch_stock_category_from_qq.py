import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import urllib.request
import re

from datetime import datetime

# from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
import R50_general.general_constants_funcs as gcf
from R50_general.general_constants_funcs import logprint
# import R50_general.dfm_to_table_common as df2db
# TODO: current only handle China market category, will parse other market in future
def fetch2DB():
    url_category = gcf.weblinks['stock_category_qq']
    soup_category = gcf.get_webpage(url_category)
    # parse data like below:
    # <a class ="clk-mo-li" href="?id=bd021225" id="a-l-bd021225" title="广东自贸区" >  广东自贸区 < / a >
    # bd01代表腾讯行业
    # Bd02代表概念
    # Bd03代表地域
    # Bd04代表证监会行业

    dt_type = {'bd01':'腾讯行业',
               'bd02':'概念',
               'bd03':'地域',
               'bd04':'证监会行业'}
    ls_category = []
    for type in dt_type:
        ls_category.extend(parse_concept(soup_category,type))
#    print("\n".join(list(map(lambda x:str(x[0])+':'+str(x[1]),ls_category))))

def parse_concept(tmp_soup,cate_type:str):
    # cate_industry       = re.findall('href="\?id=bd01(.*?)">(.*?)<',str(soup_category))
    # cate_concept        = re.findall('href="\?id=bd02(.*?)">(.*?)<',str(soup_category))
    # cate_region         = re.findall('href="\?id=bd03(.*?)">(.*?)<',str(soup_category))
    # cate_zjh_industry   = re.findall('href="\?id=bd04(.*?)">(.*?)<',str(soup_category))
    ls_category = []
    ls_tmp_category = re.findall('href="\?id=%s(.*?)>(.*?)<' %cate_type,str(tmp_soup))
    tmp_category = ''
    for item in ls_tmp_category:
        if len(item) != 2:
            raise
        tmp_category = item[1]
        tmp_title = re.findall('title="(.*?)"',item[0])
        if len(tmp_title) == 1:
            tmp_category = tmp_title[0]
        ls_category.append((cate_type,tmp_category))
    return ls_category[:]



if __name__ == '__main__':
    fetch2DB()