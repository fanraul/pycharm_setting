import time
import urllib.error
from datetime import datetime
import os

import R50_general.dfm_to_table_common as df2db
from R50_general.general_helper_funcs import logprint

def auto_reprocess_dueto_ipblock(identifier:str,func_to_call,wait_seconds:int = 0):
    """
    sample usage:
     ahf.auto_reprocess_dueto_ipblock(identifier='fetch_stock_structure_hist_from_sina', func_to_call= fetch2DB, wait_seconds= 600)
    :param identifier:
    :param func_to_call:
    :param wait_seconds:
    :return:
    """
    # use a temp file to store the stockid processed
    append_log_file = open('process_log_%s.txt' %identifier,'a')   #先用a模式生成文件,再用r模式读取文件,否则会报找不到文件的错误
    read_log_file = open('process_log_%s.txt' %identifier,'r')
    ls_stockids_processed = ["'%s'" %line.strip() for line in read_log_file.readlines()]
    if ls_stockids_processed:
        str_stockids_processed = ','.join(ls_stockids_processed)
    else:
        str_stockids_processed = "''"
    # step2.1: get current stock list
    dfm_stocks_to_process = df2db.get_cn_stocklist('',str_stockids_processed)
    for index,row in dfm_stocks_to_process.iterrows():
        try:
            func_to_call(row['Stock_ID'])
            append_log_file.write(row['Stock_ID'] + '\n')
            append_log_file.flush()  #由于缓冲，字符串可能实际上没有出现在该文件中，直到调用flush()或close()方法被调用.
        except (urllib.error.HTTPError,urllib.error.URLError):
            append_log_file.close()
            read_log_file.close()
            logprint('Web scrapping exception raised, auto reprocess after 600 seconds. Current time is %s' % datetime.now())
            time.sleep(wait_seconds)
            auto_reprocess_dueto_ipblock(identifier)
            return

    append_log_file.close()
    read_log_file.close()
    os.remove('process_log_%s.txt' %identifier)

