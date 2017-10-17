import email
import email.mime.multipart as multipart  # import MIMEMultipart
import mimetypes
import os.path
import smtplib
import time
import urllib.error
import urllib.request
from datetime import datetime
from email.mime.base import MIMEBase  # import MIMEBase
from email.mime.text import MIMEText  # import MIMEText

import pandas as pd
import tushare as ts
from bs4 import BeautifulSoup
from pandas import DataFrame

import tquant.getdata as gt

# global variable for log processing
log = True
log_folder = 'C:/00_RichMinds/log/'
log_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = ''
log_job_name =''

def logprint(*args, sep=' ',  end='\n',  file=None ):
    global log_file
    if log:
        print(*args,sep=' ', end='\n', file=None)
        if log_file == '':
            log_file = log_folder +'tmp/templog'+log_timestamp+'.txt'
        print(*args, sep=' ', end='\n', file= open(log_file,'a'))

def setup_log_file(jobname:str):
    global log_file,log_job_name
    log_job_name = jobname
    log_file = log_folder + jobname + '_' + log_timestamp + '.txt'

def get_webpage(weblink_str :str, time_wait = 0, flg_return_json= False):
    """

    :param weblink_str: str of web link to web scrap
    :param time_wait: time to wait before open web link
    :param flg_return_json: True-> return str of repsonse,used in json senario
                            False-> return beautifulsoap object
    :return:
    """
    req = urllib.request.Request(weblink_str)
    user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36'
    req.add_header('User-Agent', user_agent)
    req.add_header('Referer',weblink_str)
    try:
        time.sleep(time_wait)
        print('web page %s loading start..' %weblink_str)
        response = urllib.request.urlopen(req)
        html = response.read()
        print('web page loading end..')
        if flg_return_json:
            return html.decode()
        soup = BeautifulSoup(html,"lxml")
    #        print (soup.prettify())
        return soup
    except urllib.error.HTTPError as e:
        logprint("The server couldn't fulfill the request.Page link is %s" % weblink_str)
        logprint('Error code: %s; Error Reason: %s' % (e.code, e.reason))
        raise e
        # if time_wait > 600:
        #     raise
        # logprint("Web page retry after %s..." %(time_wait+300))
        # get_webpage(weblink_str,time_wait + 300)
    except urllib.error.URLError as e:
        logprint('We failed to reach a server. Page link is %s' % weblink_str)
        logprint('Error Reason: %s' % (e.reason))
        raise e
        # if time_wait > 600:
        #     raise
        # logprint("Web page retry after %s..." %(time_wait+300))
        # get_webpage(weblink_str, time_wait + 300)
    except Exception as e:
        logprint("Weblink %s open failed error unkown:" % weblink_str, e,type(e))
        raise


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
                # ☆if no date is specified, put '1900-1-1' as non-sense date so that all date type field has value!
                dfm[col] = dfm[col].map(lambda x: datetime.strptime(x,dateformat) if x != '--' and x != '' else datetime(1900,1,1))
            elif 'varchar' in columns[col] or 'str' == columns[col]:
                dfm[col] = dfm[col].astype('str')
            elif 'int' in columns[col]:
                dfm[col] = dfm[col].map(lambda x: x if x != '--' else 0)
                dfm[col] = dfm[col].astype('int')
            elif 'float' in columns[col] or 'double' in columns[col]:
                dfm[col] = dfm[col].map(float_conversion)


def float_conversion(x):
    if type(x) == type('a'):
        if x == '--':
            return 0.0
        elif len(x) >2 and x[-2:] == '万股':
            return float(x[:-2])

    return float(x)

def print_list_nice(ls_tmp):
    print('List content is shown as below:')
    print("\n------------------------------\n".join(list(map(lambda x: str(x), ls_tmp))))
    print('List content Ends.')

def dfmprint(*args, sep=' ',  end='\n',  file=None):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(*args, sep=' ',  end='\n',  file=None)

def dfm_A_minus_B(A:DataFrame,B:DataFrame, key_cols:list)->DataFrame:
    """
    A - B return the entries which in A but not in B based on key cols, it is mainly used to remove duplicate entries and
    then insert to DB
    :param A:
    :param B:
    :return:
    """
    if len(B) == 0:
        return A
    B_tmp=B[key_cols].copy()
    B_tmp['tmp_col_duplicated'] = 'Y'
    dfm_merge_by_keycols = A.merge(B_tmp, how='left', on = key_cols)
    dfm_merge_by_keycols.fillna({'tmp_col_duplicated':'N'},inplace = True)
    dfm_dif_by_keycols = dfm_merge_by_keycols[dfm_merge_by_keycols.tmp_col_duplicated.isin(['N'])]
    del dfm_dif_by_keycols['tmp_col_duplicated']
    return dfm_dif_by_keycols
    # 不知道为什么,就是不能用A[A.col=='X']进行过滤,找到原因的,duplicated是个函数名,所以不能用其作为列名,有歧义!!
    # print(dfm_merge_by_keycols[dfm_merge_by_keycols.duplicated == 'X'])
    # del dfm_merge_by_keycols['duplicated']

def dfm_A_intersect_B(A:DataFrame,B:DataFrame, key_cols:list)->DataFrame:
    """
    A - B return the entries which in A and in B based on key cols, it is mainly used to identify duplicate entries and
    then update to DB
    :param A:
    :param B:
    :return:
    """
    if len(B) == 0:
        return DataFrame(columns=list(A.columns))
    B_tmp=B[key_cols].copy()
    B_tmp['tmp_col_duplicated'] = 'Y'
    dfm_merge_by_keycols = A.merge(B_tmp, how='left', on = key_cols)
    dfm_merge_by_keycols.dropna(inplace = True)
    del dfm_merge_by_keycols['tmp_col_duplicated']
    return dfm_merge_by_keycols

def dfm_value_to_set(dfm_data:DataFrame,cols:list) -> set:
    """
    convert dfm values of column cols into one set, {(value of col1,value of col2,...),(value of col1,...),...} each
    tuple represent for one line of dfm
    :param dfm_data:
    :param cols:
    :return:
    """
    ls_values =[]
    # check dfm_data is Series or Dataframe type
    # if Series:
    if isinstance(dfm_data,pd.Series):
        ls_values.append(tuple(dfm_data[cols]))
    # if Dataframe
    elif isinstance(dfm_data,pd.DataFrame):
        for id in range(len(dfm_data)):
            ls_values.append(tuple(dfm_data.iloc[id][cols]))
    else:
        assert 0==1,'dfm_value_to_set parameter is not a type of Dataframe or Series!'
    # print(ls_values)
    return set(tuple(ls_values))

def setdif_dfm_A_to_B(dfm1,dfm2,cols:list)->set:
    """
    return a set of difference for dfm1 to dfm2,meaning in dfm1 but not in dfm2 based on the column name list cols
    the set contains touples, each touple represent one line which in dfm1 but not in dfm2
    :param dfm1:
    :param dfm2:
    :return:
    """
    set_dfm1_values = dfm_value_to_set(dfm1, cols)
    set_dfm2_values = dfm_value_to_set(dfm2, cols)

    set_dif = set_dfm1_values - set_dfm2_values
    return set_dif

def func_call_with_trace(func_name,*func_args,dt_args_w_name = {},program_name:str = ''):
    """
    这个函数用于显示一个函数的开始执行和结束执行的日志并显示执行花费的时间(按秒显示)
    :param func_name:
    :param func_args:
    :param dt_args_w_name:
    :return:
    """
    start_time = datetime.now()
    logprint('*********         Start function call %s.%s     ***********' % (program_name,
                                                                              func_name.__name__))
    func_name(*func_args,**dt_args_w_name)
    end_time = datetime.now()
    time_spent = end_time-start_time
    logprint('*********  End function call %s.%s, time spent: %d seconds  *************' % (program_name,
                                                                                            func_name.__name__,
                                                                                            time_spent.total_seconds()))

def get_market_calendar(startdate,enddate,market_id:str='') ->list:
    """

    :param startdate:
    :param enddate:
    :param market_id:
    :return: list of timestamp which is trading day
    """
    if market_id == '':
        return (gt.get_calendar(startdate, enddate))


def get_last_trading_day(market_id:str='SH'):
    # TODO: currently only work for CN market, future support HK,US etc. market
    # gt.get_calendar result is incorrect, below 3 lines code is obsolete
    # last_trading_day = datetime.now()
    # while gt.get_calendar(last_trading_day,last_trading_day) :
    #     last_trading_day = last_trading_day - timedelta(days=1)

    # use index 000001 上证指数 to get the last tradin day
    if market_id in ('','SH','SZ'):
        dfm_000001 = ts.get_k_data('000001',index=True)
        last_trading_day = datetime.strptime(dfm_000001.iloc[len(dfm_000001) - 1]['date'],'%Y-%m-%d')

    return last_trading_day.date()

def get_last_trading_daytime(market_id:str='SH'):
    last_trading_day = get_last_trading_day(market_id)
    last_trading_daytime = datetime.strptime(str(last_trading_day), '%Y-%m-%d')
    return last_trading_daytime

def send_email(receiver, title, content, attachments):
    From = "fanraul@163.com"
    To = receiver
    server = smtplib.SMTP("smtp.163.com")
    server.login("fanraul@163.com", "121357468@")  # 仅smtp服务器需要验证时

    # 构造MIMEMultipart对象做为根容器
    main_msg = multipart.MIMEMultipart()

    # 构造MIMEText对象做为邮件显示内容并附加到根容器
    text_msg = MIMEText(content, _charset="utf-8")
    main_msg.attach(text_msg)

    # 构造MIMEBase对象做为文件附件内容并附加到根容器
    for attachment in attachments:
        file_name = attachment  # 附件名 :"c:/1.png"
        # 读入文件内容并格式化 [方式1]－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－
        data = open(file_name, 'rb')
        ctype, encoding = mimetypes.guess_type(file_name)
        if ctype is None or encoding is not None:
            ctype = 'application/octet-stream'
        maintype, subtype = ctype.split('/', 1)
        file_msg = MIMEBase(maintype, subtype)
        file_msg.set_payload(data.read())
        data.close()
        email.encoders.encode_base64(file_msg)  # 把附件编码
        '''
         测试识别文件类型：mimetypes.guess_type(file_name) 
         rar 文件             ctype,encoding值：None None（ini文件、csv文件、apk文件） 
         txt text/plain None 
         py  text/x-python None 
         gif image/gif None 
         png image/x-png None 
         jpg image/pjpeg None 
         pdf application/pdf None 
         doc application/msword None 
         zip application/x-zip-compressed None 
        encoding值在什么情况下不是None呢？以后有结果补充。 
        '''
    # －－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－
        ## 设置附件头
        basename = os.path.basename(file_name)
        file_msg.add_header('Content-Disposition', 'attachment', filename=basename)  # 修改邮件头
        main_msg.attach(file_msg)

    # 设置根容器属性
    main_msg['From'] = From
    main_msg['To'] = To
    main_msg['Subject'] = title
    main_msg['Date'] = email.utils.formatdate()

    # 得到格式化后的完整文本
    fullText = main_msg.as_string()

    # 用smtp发送邮件
    try:
        server.sendmail(From, To, fullText)
    except:
        raise
    finally:
        server.quit()

def send_daily_job_log(content:str,flg_except:bool = False):
    receiver = 'fanraul@icloud.com;terry.fan@sparkleconsulting.com'
    if not flg_except:
        title = '%s job log %s' %(log_job_name,datetime.now().date())
    else:
        title = 'Daily job %s %s failed, please review the attachment and error text!' %(log_job_name,datetime.now().date())
        # TODO: error handling
    attachment = [log_file]
    content = content
    send_email(receiver, title, content, attachment)

def exception_handler():
    # TODO finish exception handler
    pass

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
    # dfm_temp = DataFrame([{'A':'1997-1-3','B':2,'C':'12.23'},{'A':'1997-1-4','B':3,'C':'12.35'}],index=['2017-1-1','2017-1-2'])
    # print(dfm_temp)
    # dfm_col_type_conversion(dfm_temp,index= 'datetime',columns={'A': 'datetime','B':'varchar(50)','C':'double(18,6)'})
    # print(dfm_temp)
    # print(list(dfm_temp.index))
    # print(type(dfm_temp.iloc[0][0]), type(dfm_temp.iloc[0][1]),type(dfm_temp.iloc[0][2]))
    # dfm1 = DataFrame([{'A':'A1','B':'B1','C':'C1'},{'A':'A2','B':'B2','C':'C2'},{'A':'A3','B':'B3','C':'C3'},])
    # dfm2 = DataFrame([{'A':'A1','B':'B1','C':'C1'},{'A':'A2','B':'B4','C':'C4'},{'A':'A3','B':'B3','C':'C5'},])
    # # dfm2 = DataFrame(columns = ['A', 'B', 'C', 'D'] )
    # print(dfm1,dfm2,sep='\n')
    # print(dfm_A_minus_B(dfm1,dfm2,key_cols = ['A','B']))
    # print(dfm_A_intersect_B(dfm1,dfm2,key_cols=['A','B']))

    # dfm4 = DataFrame(
    #     [{'A': 'A1', 'B': 'B1', 'C': 'C1'}, {'A': 'A2', 'B': 'B2', 'C': 'C2'}, {'A': 'A3', 'B': 'B3', 'C': 'C3'}, ])
    #
    # for index,row in dfm4.iterrows():
    #     print(index,type(index))
    #     print(row,type(row))

    # 7.test for get_last_trading_day
    print(get_last_trading_day())
    # 8.test for func func_call_with_trace
    # dt_args_w_name={}
    # dt_args_w_name['sep'] = ','
    # func_call_with_trace(print,'this is a test','hello',dt_args_w_name = dt_args_w_name)
    # func_call_with_trace(print_list_nice, [(1,2)]*1000000)

    # 8.test for send_email
    # send_email('fanraul@icloud.com;terry.fan@sparkleconsulting.com', 'email test',
    #            'this is a test for email attachement',
    #            ['C:/00 RichMinds/Github/RichMinds/R10_sensor/tmp.xls','C:/00 RichMinds/Github/RichMinds/README.md'])


