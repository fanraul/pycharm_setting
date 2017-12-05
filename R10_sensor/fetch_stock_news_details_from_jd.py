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


def fetch2FILE():
    url_news = 'http://gupiao.jd.com/info/detail-106193.html'
    html_news_item = gcf.get_webpage(url_news,flg_return_rawhtml=True)
    filename_news = R50_general.general_constants.folder_news_details_jd + 'detail-106193.html'
    file_news = open(filename_news,'wb')
    file_news.write(html_news_item)
    file_news.close()

if __name__ == "__main__":
    fetch2FILE()