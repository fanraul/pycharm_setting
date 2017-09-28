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

timestamp = datetime.now()

# TODO: current only handle China market category, will parse other market in future
def fetch2DB():


    url_catglist = gcf.weblinks['stock_category_w_detail_qq'][0]

    # t参数:代表不同的category类型
    # 01-腾讯行业
    # 02-概念
    # 03-地域
    # 04-证监会行业

    # 1.1 get category code list
    ls_catg =[]
    for i in range(4):
        url_link = url_catglist %{'catg_type':'0'+str(i+1)}
        # print(url_link)
        soup_category = gcf.get_webpage(url_link)
        list_data = re.findall("data:'(.*)'",str(soup_category))
        if i==2 or i==3:
            # need to replace qt with hz, eg. bkqt034600->bkhz034600
            ls_catg.extend([x[:2]+'hz'+x[4:] for x in list_data[0].split(',')])
        else:
            ls_catg.extend(list_data[0].split(','))

    # 1.2 get category detail, such as name etc.
    ls_dfm_catgs = []
    ls_dfm_catgs_trans =[]
    ls_tmp =[]
    for i in range(len(ls_catg)):
        ls_tmp.append(ls_catg[i])
        # every 80 items,get web page in one batch
        # 在for循环中用每n个处理一次时,要注意要用(i+1) % n,因为i是从0开始的.
        if (i+1) % 50 == 0:
            dfm_catgs_tmp, dfm_catgs_trans_tmp = parse_concept(','.join(ls_tmp))
            ls_dfm_catgs.append(dfm_catgs_tmp)
            ls_dfm_catgs_trans.append(dfm_catgs_trans_tmp)
            ls_tmp=[]
    if ls_tmp:
        dfm_catgs_tmp, dfm_catgs_trans_tmp = parse_concept(','.join(ls_tmp))
        ls_dfm_catgs.append(dfm_catgs_tmp)
        ls_dfm_catgs_trans.append(dfm_catgs_trans_tmp)


    dfm_cur_catg = pd.concat(ls_dfm_catgs)
    dfm_catgs_daily_trans = pd.concat(ls_dfm_catgs_trans)

    # 2.1 insert new category into DB
    # get db category list
    dfm_db_catg = df2db.get_stock_catg('QQ')
    dfm_new_catg = gcf.dfm_A_minus_B(dfm_cur_catg,dfm_db_catg,['Catg_Origin','Catg_Type','Catg_Name'])
    # print(dfm_db_catg,dfm_cur_catg,dfm_new_catg,sep = '\n')
    if len(dfm_new_catg) > 0:
        # gcf.dfmprint(dfm_cur_catg)
        logprint('New Category added:\n' ,'\n'.join([dfm_new_catg.iloc[i]['Catg_Type'] + ':' +
                                                   dfm_new_catg.iloc[i]['Catg_Name']
                                                   for i in range(len(dfm_new_catg))]), sep = '\n')
        df2db.load_snapshot_dfm_to_db(dfm_new_catg,'ZCFG_category',w_timestamp=True)



    # 2.2 insert new stock category relationship into DB
    # create DB table and chars
    table_name_concept = gcf.dbtables['stock_category_relation_qq']
    df2db.create_table_by_template(table_name_concept,table_type='stock_date_multi_value')
    # get chars for stock category
    dfm_db_chars_catg = df2db.get_chars('QQ', ['CATG'])
    dict_misc_pars_catg = {}
    dict_misc_pars_catg['char_origin'] = 'QQ'
    dict_misc_pars_catg['char_freq'] = "D"
    dict_misc_pars_catg['allow_multiple'] ='Y'
    dict_misc_pars_catg['created_by'] = dict_misc_pars_catg['update_by'] ='fetch_stock_category_and_daily_status_from_qq'
    dict_misc_pars_catg['char_usage'] = 'CATG'
    dict_cols_cur_catg = {'所属板块': 'nvarchar(50)'}
    df2db.add_new_chars_and_cols(dict_cols_cur_catg, list(dfm_db_chars_catg['Char_ID']), table_name_concept,
                                 dict_misc_pars_catg)


    #func2: fetch stock category info
    dt_stk_catgs = parse_stock_under_catg(dfm_cur_catg)
    print(dt_stk_catgs)

    #get stock list:
    dfm_cn_stocks = df2db.get_cn_stocklist()
    dfm_cn_stocks['marstk_id'] = dfm_cn_stocks['Market_ID'] + dfm_cn_stocks['Stock_ID']

    for index,row in dfm_cn_stocks.iterrows():
        ls_catgs = dt_stk_catgs.get(row['marstk_id'],None)
        if ls_catgs:
            dfm_item_catgs = DataFrame(ls_catgs)
            df2db.load_dfm_to_db_multi_value_by_mkt_stk_cur(row['Market_ID'],
                                                            row['Stock_ID'],
                                                            dfm_item_catgs,
                                                            table_name_concept,
                                                            dict_misc_pars_catg,
                                                            process_mode = 'w_check')
        else:
            logprint("Stock %s doesn't have category assigned" %(row['Stock_ID']+':'+ row['Stock_Name']))

    # 2.3 insert category daily detail into DB


def parse_stock_under_catg(dfm_catgs:DataFrame) ->dict:
    """
    return a dictionary, key:value -> market code + stock code: list of category related to it
    :param dfm_catgs:
    :return:
    """
    dt_stkcatgs = {}
    for index,row in dfm_catgs.iterrows():
        catg_code = row['Catg_Reference']
        url_catgstklist = gcf.weblinks['stock_category_w_detail_qq'][2] %{'catg_code':catg_code}
        soup_stklst = gcf.get_webpage(url_catgstklist)
        list_data = re.findall("data:'(.*)'", str(soup_stklst))
        if list_data:
            ls_stk = list_data[0].split(',')
            ls_stk = [stk.strip().upper() for stk in ls_stk]
            print(ls_stk)
            for stkcode in ls_stk:
                ls_catgs = dt_stkcatgs.get(stkcode, [])
                # print(ls_catgs)
                if ls_catgs:
                    ls_catgs.append({'所属板块': row['Catg_Name']})
                else:
                    dt_stkcatgs[stkcode] = [{'所属板块': row['Catg_Name']}]
        else:
            logprint('Exception: Catg %s has no stock assigned' %row['Catg_Name'] )


    return dt_stkcatgs


def parse_concept(str_catgs):
    url_catgdetail = gcf.weblinks['stock_category_w_detail_qq'][1] % {'catg_list': str_catgs}
    soup_catg = gcf.get_webpage(url_catgdetail)
    # print(url_catgdetail)
    # print(soup_catg)

    dt_type = {'01':'腾讯行业',
               '02':'概念',
               '03':'地域',
               '04':'证监会行业'}

    ls_catg_detail = re.findall('="(.*)";',str(soup_catg))

    ls_dfm_catg = []
    ls_dfm_catg_daily_trans =[]

    for item in ls_catg_detail:
        ls_item = item.split('~')
        if len(ls_item) > 10:
            dt_line = {}
            dt_line['Catg_Origin'] = 'QQ'
            dt_line['Catg_Type'] = dt_type[ls_item[0][:2]]
            dt_line['Catg_Name'] = ls_item[1].strip()
            dt_line['Catg_Reference'] = ls_item[0].strip()
            dt_line['Created_datetime'] = timestamp
            dt_line['Created_by'] = 'fetch_stock_category_and_daily_status_from_qq'
            ls_dfm_catg.append(dt_line)

            dt_catg_daily_trans ={}
            dt_catg_daily_trans['Catg_Origin']  =  'QQ'
            dt_catg_daily_trans['Catg_Type']    = dt_type[ls_item[0][:2]]
            dt_catg_daily_trans['Catg_Name']    = ls_item[1].strip()
            dt_catg_daily_trans['上涨家数']      = ls_item[2]
            dt_catg_daily_trans['平盘家数']      = ls_item[3]
            dt_catg_daily_trans['下跌家数']      = ls_item[4]
            dt_catg_daily_trans['总家数']        = ls_item[5]
            dt_catg_daily_trans['平均价格']      = ls_item[6]
            dt_catg_daily_trans['涨跌幅']        = ls_item[7]
            dt_catg_daily_trans['总成交手数']    = ls_item[8]
            dt_catg_daily_trans['总成交额万元']  = ls_item[9]
            ls_dfm_catg_daily_trans.append(dt_catg_daily_trans)
        else:
            raise
    return (DataFrame(ls_dfm_catg),DataFrame(ls_dfm_catg_daily_trans))

if __name__ == '__main__':
    fetch2DB()
    # soup_tmp = gcf.get_webpage('http://qt.gtimg.cn/q=bkqt012067')
    #
    # print(soup_tmp.prettify())