import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from datetime import datetime

import R50_general.general_constants
from R50_general.general_helper_funcs import logprint
import R50_general.general_helper_funcs as gcf
import R50_general.dfm_to_table_common as df2db
import R50_general.advanced_helper_funcs as ahf


import tquant.myquant as mt

global_module_name = 'fetch_stock_dailybar_from_netease'

def fetch2DB(stockid:str):
    # init step
    # create DD tables for data store and add chars for stock structure.
    # get chars for name change hist
    dfm_db_chars = df2db.get_chars('NETEASE', ['DAILYBAR'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'NETEASE'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'DAILYBAR'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['stock_dailybar_netease']
    df2db.create_table_by_template(table_name,table_type='stock_date')
    dict_cols_cur = {'open':'decimal(12,4)',
                     'close':'decimal(12,4)',
                     'high':'decimal(12,4)',
                     'low':'decimal(12,4)',
                     'turnover':'decimal(10,4)',
                     'vol':'decimal(15,2)',
                     'amount': 'decimal(15,2)',
                     '前收盘':'decimal(12,4)',
                     'CHG':'decimal(12,4)',
                     'PCHG':'decimal(10,4)',
                     'TCAP':'decimal(18,2)',
                     'MCAP':'decimal(18,2)',
                    }
    df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)

    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)
    last_trading_datetime = gcf.get_last_trading_daytime()
    last_trading_date = last_trading_datetime.date()
    last_fetch_date = df2db.get_last_fetch_date(global_module_name,format ='date')

    if last_fetch_date and last_fetch_date >= last_trading_date:
        logprint('No need to fetch dialybar since last_fetch_date %s is later than or equal to last trading date %s' %(last_fetch_date,last_trading_date))
        return

    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' %row['Stock_ID'])
        if last_fetch_date:
            begin_time = last_fetch_date
        elif not row['上市日期']:
            logprint('No stock dailybars can be found for stockid %s' % row['Stock_ID'])
            continue
        elif row['上市日期'].date() > R50_general.general_constants.Global_dailybar_begin_date:
            begin_time = row['上市日期'].date()
        else:
            begin_time = R50_general.general_constants.Global_dailybar_begin_date
        begin_time = begin_time.strftime('%Y%m%d')

        dfm_stk_info = get_dialybar_by_mktstkid(row['Market_ID'],row['Stock_ID'],begin_time = begin_time,end_time=last_trading_date.strftime('%Y%m%d'))

        # TODO: error handling
        if len(dfm_stk_info) == 0:
            logprint('No stock dailybars can be found for stockid %s' %row['Stock_ID'])
        else:
            # step2: format raw data into prop data type
            # gcf.dfmprint(dfm_stk_info)
            gcf.dfm_col_type_conversion(dfm_stk_info, columns=dict_cols_cur)
            gcf.dfmprint(dfm_stk_info)
            df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(row['Market_ID'], row['Stock_ID'], dfm_stk_info, table_name,
                                                                dict_misc_pars,
                                                                processing_mode='w_update',float_fix_decimal=4,partial_ind= True)

    if stockid =='':
        df2db.updateDB_last_fetch_date(global_module_name,last_trading_datetime)


def get_dialybar_by_mktstkid(market_id,stock_id:str,begin_time,end_time):
    """
    :param mrkstk_id: for example, 'sz300712'
    :return:
    """
    if market_id=='SH':
        mktstk_id ='0'+stock_id
    elif market_id =="SZ":
        mktstk_id ='1'+stock_id
    else:
        assert False,'unknown market id %s, please check' %market_id

    dailybar_url= R50_general.general_constants.weblinks['stock_dailybar_netease'] %(mktstk_id,begin_time,end_time)
    '''
    'http://quotes.money.163.com/service/chddata.html?code=%s&start=19900101&end=%s&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'%(index_id,time.strftime("%Y%m%d"))
    收盘价;最高价;最低价;开盘价;前收盘;涨跌额;涨跌幅;换手率;成交量;成交金额;总市值;流通市值;
    '''
    dailybar_str = gcf.get_webpage(dailybar_url,flg_return_json=True,decode ='gb2312')
    # print(dailybar_str)
    # page=get_page(url).decode('gb2312') #该段获取原始数据
    ls_dailybar=dailybar_str.split('\r\n')
    # gcf.print_list_nice(ls_dailybar)

    if len(ls_dailybar) > 1 and ls_dailybar[0].strip() == '日期,股票代码,名称,收盘价,最高价,最低价,开盘价,前收盘,涨跌额,涨跌幅,换手率,成交量,成交金额,总市值,流通市值':
        ls_dailybar = ls_dailybar[1:]
    else:
        return DataFrame()

    ls_dailybar_dfm =[]
    ls_index =[]
    for dailybar in ls_dailybar :
        if dailybar:
            # print(dailybar)
            ls_figs = dailybar.split(',')
            # if value = 'None',then change it to python None
            ls_figs = [x if x != 'None' else None for x in ls_figs]
            if len(ls_figs) == 15:
                transdate = datetime.strptime(ls_figs[0],'%Y-%m-%d')
                dt_dailybar ={}
                dt_dailybar['close'] = ls_figs[3]
                dt_dailybar['high']= ls_figs[4]
                dt_dailybar['low']= ls_figs[5]
                dt_dailybar['open']= ls_figs[6]
                dt_dailybar['前收盘']= ls_figs[7]
                dt_dailybar['CHG']= ls_figs[8]
                dt_dailybar['PCHG']= ls_figs[9]
                dt_dailybar['turnover']= ls_figs[10]
                dt_dailybar['vol']= ls_figs[11]
                dt_dailybar['amount']= ls_figs[12]
                dt_dailybar['TCAP']= ls_figs[13]
                dt_dailybar['MCAP']= ls_figs[14]
                ls_dailybar_dfm.append(dt_dailybar)
                ls_index.append(transdate)
            else:
                assert False, '%s is inconsistent,not 15 cols in total, please check' %ls_figs
    return DataFrame(ls_dailybar_dfm,index =ls_index)
        # col_info=ls_dailybar[0].split(',')   #各列的含义
    # dailybar_data=page[1:]     #真正的数据
    # #为了与现有的数据库对应，这里我还修改了列名，大家不改也没关系
    # col_info[col_info.index('日期')]='交易日期'   #该段更改列名称
    # col_info[col_info.index('股票代码')]='指数代码'
    # col_info[col_info.index('名称')]='指数名称'
    # col_info[col_info.index('成交金额')]='成交额'
    #
    # index_data=[x.replace("'",'') for x in index_data]  #去掉指数编号前的“'”
    # index_data=[x.split(',') for x in index_data]
    #
    # index_data=index_data[0:index_data.__len__()-1]   #最后一行为空，需要去掉
    # pos1=col_info.index('涨跌幅')
    # pos2=col_info.index('涨跌额')
    # posclose=col_info.index('收盘价')
    # index_data[index_data.__len__()-1][pos1]=0      #最下面行涨跌额和涨跌幅为None改为0
    # index_data[index_data.__len__()-1][pos2]=0
    # for i in range(0,index_data.__len__()-1):       #这两列中有些值莫名其妙为None 现在补全
    #     if index_data[i][pos2]=='None':
    #         index_data[i][pos2]=float(index_data[i][posclose])-float(index_data[i+1][posclose])
    #     if index_data[i][pos1]=='None':
    #         index_data[i][pos1]=(float(index_data[i][posclose])-float(index_data[i+1][posclose]))/float(index_data[i+1][posclose])
    #
    # # print(col_info)
    # return [index_data,col_info]

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call=fetch2DB, wait_seconds=60)


if __name__ == '__main__':
    # fetch2DB('300328')
    auto_reprocess()
