import R50_general.general_constants
from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
from R50_general.general_helper_funcs import logprint
import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import R50_general.general_helper_funcs as gcf
from datetime import datetime

#initial steps:
# step1: get DB connection
dcm_sql = dcm(echo=False)
conn = dcm_sql.getconn()

def get_cn_stocklist(stock :str ="", str_excluded_stockids="''") -> DataFrame:
    """
    获得沪市和深市的股票清单
    :return: dataframe of SH and SZ stocks
    """
    if stock == "":
        dfm_stocks = pd.read_sql_query('''select Market_ID,Stock_ID,Stock_Name,is_active,
                                            Market_ID + Stock_ID as MktStk_ID 
                                            from stock_basic_info 
                                            where ((Market_ID = 'SH' and Stock_ID like '6%') 
                                            or (Market_ID = 'SZ' and (Stock_ID like '0%' or Stock_ID like '3%' )))
                                            and sec_type = '1' ''' +  'and Stock_ID not in (%s)' %str_excluded_stockids
                                   , conn)
       # print(dfm_stocks)
    else:
        dfm_stocks = pd.read_sql_query('''select Market_ID,Stock_ID,Stock_Name,is_active,
                                            Market_ID + Stock_ID as MktStk_ID 
                                            from stock_basic_info
                                                where  (Market_ID = 'SH' or Market_ID = 'SZ')  
                                                and sec_type = '1'
                                                    ''' + "and Stock_ID = '%s'" %stock
                                       , conn)
    return dfm_stocks

def get_all_stocklist(stock :str ="") -> DataFrame:
    """
    获得所有stocklist中的stock清单,不单单是股票
    :return: dataframe of SH and SZ stocks
    """
    if stock == "":
        dfm_stocks = pd.read_sql_query('''select * from stock_basic_info   
                                                '''
                                   , conn)
       # print(dfm_stocks)
    else:
        dfm_stocks = pd.read_sql_query('''select * from stock_basic_info 
                                                where   
                                                    ''' + "and Stock_ID = '%s'" %stock
                                       , conn)
    return dfm_stocks


def get_chars(origin = '',usages = [],freq='D',charids=[]) ->DataFrame:
    """
    获得股票的属性chars清单
    :param origin:
    :param usages: 可以传入一个list,包含多个值,但是不支持模糊查询
    :param frq:
    :param charids:可以传入一个list,包含多个值,但是不支持模糊查询
    :return:
    """

    ls_sel_str =[]
    sel_str_orgin = "Char_Origin = '%s'" %origin if origin else ''
    sel_str_usage = "Char_Usage in ('%s')" %"','".join(usages) if usages else ''
    sel_str_freq = "Char_Freq = '%s'" %freq if freq else ''
    sel_str_charids = "Char_ID in ('%s')" %"','".join(charids) if charids else ''
    sel_str = 'select * from ZCFG_character %s' %concatenate_sel_str(sel_str_orgin,sel_str_usage,sel_str_freq,sel_str_charids)
#    print(sel_str)
    dfm_cur_chars = pd.read_sql_query( sel_str
                                      , conn)

    return dfm_cur_chars

def concatenate_sel_str(*args):
    ls_sel_str = [x for x in args if x]
    if ls_sel_str:
        return 'WHERE ' + ' AND '.join(ls_sel_str)
    else:
        return ''

def create_table_by_template(table_name:str,table_type:str):
    dfm_table_check = pd.read_sql_query(" select * from sys.objects where type = 'U' and name = '%s'" % table_name.strip(),
                                        conn)
    if len(dfm_table_check) == 0:
        if table_type == 'stock_date':
            crt_str = R50_general.general_constants.dbtemplate_stock_date % {'table':table_name.strip()}
        elif table_type == 'stock_date_multi_value':
            crt_str = R50_general.general_constants.dbtemplate_stock_date_multi_value % {'table':table_name.strip()}
        elif table_type == 'stock_wo_date':
            crt_str = R50_general.general_constants.dbtemplate_stock_wo_date % {'table':table_name.strip()}
        elif table_type == 'catg_date':
            crt_str = R50_general.general_constants.dbtemplate_catg_date % {'table': table_name.strip()}
        else:
            raise
        conn.execute(crt_str)
        logprint('Table %s is created' %table_name)

def add_new_chars_and_cols(dict_cols_cur:dict,ls_cols_db:list,table_name:str,dict_misc_pars:dict):
    """
    do 2 things:
    1) add new cols as new chars into table ZCFG_character
    2) alter related transaction table adding new cols
    :param dict_cols_cur: cols in dataframe got from API or web, key: value -> col name: data type
    :param ls_cols_db: cols in database, each col is a char in ZCFG_character
    :return: N/A
    """
    ls_cols_newadded = [x.strip() for x in dict_cols_cur.keys() if x.strip() not in ls_cols_db]
    #to be a col name in SQL server, there are some rules, so special process is required.
    ls_db_col_name_newadded = list(map(special_process_col_name,ls_cols_newadded))

    # insert to char master data and alter transaction table adding new columns
    # 一次性对表加入多列的sql 语句语法:ALTER TABLE stock_fin_balance ADD 测试1 decimal(18,2), 测试2 decimal(18,2)

    # use transaction to commit all in one batch
    trans = conn.begin()
    timestamp = datetime.now()

    # no use for SQL server, SQL server doesn't support add col_name after col_name grammer!!
    col_insert_after = get_col_for_insert(table_name)
    try:
        if ls_db_col_name_newadded:
            alter_str = "ALTER TABLE %s ADD " %table_name
            for i in range(len(ls_db_col_name_newadded)):
                alter_str += "%(col_name)s %(data_type)s," %{'col_name':ls_db_col_name_newadded[i],
                                                             'data_type': dict_cols_cur[ls_cols_newadded[i]]}
                logprint("new column added in table %s:" %table_name, ls_db_col_name_newadded[i])
            alter_str = alter_str[:-1]
            print(alter_str)
            conn.execute(alter_str)

        ls_new_chars = []
        for i in range(len(ls_cols_newadded)):
            ls_new_chars.append((dict_misc_pars['char_origin'],
                                 dict_misc_pars['char_usage'],
                                 dict_misc_pars['char_freq'],
                                 ls_cols_newadded[i],
                                 ls_cols_newadded[i],
                                 dict_misc_pars['allow_multiple'],
                                 table_name,
                                 ls_db_col_name_newadded[i],
                                 timestamp,
                                 dict_misc_pars['created_by']))

        if ls_new_chars:
            ins_str = """INSERT INTO ZCFG_character 
                        (Char_Origin,Char_Usage,Char_Freq,Char_ID,Char_Name,Allow_multiple,Data_table,
                        Data_column,Created_datetime,Created_by)
                         VALUES (?,?,?,?,?,?,?,?,?,?)"""

            conn.execute(ins_str,tuple(ls_new_chars))

        trans.commit()
    except:
        trans.rollback()
        raise

def load_dfm_to_db_by_mkt_stk_w_hist(market_id,item,dfm_data:DataFrame,table_name:str,dict_misc_pars:dict,processing_mode:str):
    """
    本函数用于单值属性的char的历史数据更新.
    本函数只支持单值类型的chars的表更新,如果char是多值的,请用load_dfm_to_db_multi_value_by_mkt_stk_w_hist(尚未开发)
    导入的dfm中的数据到table中,processing_mode决定了处理方式:
    1) 'w_update: dfm包含历史数据,如不存在,则insert,存在并且数据发生了变化,就update
    2) "wo_update: dfm包含历史数据,如不存在,则insert,但是不做update(效率更高)
    注意:传入的datafram中的index必须是时间.
    :param market_id:
    :param stock_id:
    :param dfm:
    :param table_name:
    :param dict_misc_pars:
    :processing_mode:
    :return:
    """
    # load DB contents
    timestamp = datetime.now()

    dfm_db_data = pd.read_sql_query("select * from %s where Market_ID = ? and Stock_ID = ?" %table_name
                                        , conn, params=(market_id, item), index_col='Trans_Datetime')

    ins_str_cols = ''
    ins_str_pars = ''
    ls_ins_pars = []
    for ts_id in dfm_data.index:
        if ts_id in dfm_db_data.index:
            #entry already exist
            if processing_mode == 'w_update':
                # update logic: only update the value changed cols
                ls_upt_cols = []
                ls_upt_pars = []
                for col in dfm_data.columns:
                    # dfm_data的列名有可能带[],但是dataframe从sql server中读出时的列名是都不带[],所以要把[]去掉,再进行数据比较.
                    tmp_colname = col.replace('[', '').replace(']', '')
                    if tmp_colname in dfm_db_data.columns:
                        if dfm_data.loc[ts_id][col] != dfm_db_data.loc[ts_id][tmp_colname]:
                            ls_upt_cols.append(special_process_col_name(tmp_colname) + '=?')
                            ls_upt_pars.append(dfm_data.loc[ts_id][col])
                            logprint("Update %s %s Period %s Column %s from %s to %s"
                                     % (table_name,item, ts_id, tmp_colname, dfm_db_data.loc[ts_id][tmp_colname],
                                        dfm_data.loc[ts_id][col]))
                    else:
                        logprint("Column num %s doesn't exist in table %s" %(tmp_colname,table_name))

                if ls_upt_cols:
                    upt_str = ",".join(ls_upt_cols) + ", Last_modified_datetime = ?,Last_modified_by=?"
                    ls_upt_pars.extend([timestamp, dict_misc_pars['update_by'], market_id, item,
                                        datetime.strptime(str(ts_id.date()), '%Y-%m-%d')])
                    update_str = '''UPDATE %s SET %s 
                        WHERE Market_ID = ? AND Stock_ID = ? AND Trans_Datetime = ? ''' %(table_name,upt_str)
                    conn.execute(update_str, tuple(ls_upt_pars))
            else:
                raise
            continue
        # insert logic
        logprint('Insert %s stock %s Period %s' % (table_name,item, ts_id.date()))
        # rename the df with new cols name,use rename function with a dict of old column to new column mapping
        ls_colnames_dbinsert = list(map(special_process_col_name,dfm_data.columns))
        ins_str_cols = ','.join(ls_colnames_dbinsert)
        ins_str_pars = ','.join('?' * len(ls_colnames_dbinsert))
        #        print(ins_str_cols)
        #        print(ins_str_pars)
        # convert into datetime type so that it can update into SQL server
#        trans_datetime = datetime.strptime(str(ts_id.date()), '%Y-%m-%d')
        trans_datetime = ts_id.to_pydatetime()
        ls_ins_pars.append((market_id, item, trans_datetime, timestamp, dict_misc_pars['update_by'])
                           + tuple(dfm_data.loc[ts_id]))

    if ins_str_cols:
        ins_str = '''INSERT INTO %s (Market_ID,Stock_ID,Trans_Datetime,Created_datetime,Created_by,%s) VALUES (?,?,?,?,?,%s)''' % (
            table_name,ins_str_cols, ins_str_pars)
        # print(ins_str)
        try:
            for ins_par in ls_ins_pars:
                conn.execute(ins_str, ins_par)
            # conn.execute(ins_str, ls_ins_pars)
        except:
            raise

def load_dfm_to_db_by_mkt_stk_wo_hist():
    pass
#    3) 'wo_hist': dfm包含当日数据,无需考虑db中的历史数据,直接insert即可.

def load_dfm_to_db_multi_value_by_mkt_stk_cur(market_id,item,dfm_data:DataFrame,table_name:str,dict_misc_pars:dict,
                                              process_mode = 'w_check'):
    """
    本函数用于多值属性的char的当前数据更新.
    :param market_id:
    :param item:
    :param dfm_data:
    :param table_name:
    :param dict_misc_pars:
    :return:
    """
    # load DB contents
    timestamp = datetime.now()

    last_trading_day = gcf.get_last_trading_day()
    last_trading_daytime = datetime.strptime(str(last_trading_day), '%Y-%m-%d')
    dfm_db_data = pd.read_sql_query("select * from %s where Market_ID = ? and Stock_ID = ? order by Trans_Datetime DESC" %table_name
                                        , conn, params=(market_id, item), index_col='Trans_Datetime')
    # print(dfm_data)
    # print(dfm_db_data)
    if pd.Timestamp(last_trading_day) in dfm_db_data.index:
        # entry already exist in table, since it is current date db insert, no need to update history data already exist
        # TODO: may add logic to allow multiple update per day
        logprint('Table %s entry market %s stock %s no need to update since only one update per day,but value check to ensure consistency!'
                 %(table_name,market_id,item))
        # check cur value is the same as db value,if not, inconsistency found, need manually processing
        #get latest db data set
        set_latest_db_values = dfm_db_data.loc[dfm_db_data.index[0]]
        #compare whether the cur dfm values is the same as db dfm values of latest fetching
        set_dif_cur2db = gcf.setdif_dfm_A_to_B(dfm_data,set_latest_db_values,dfm_data.columns)
        set_dif_db2cur = gcf.setdif_dfm_A_to_B(set_latest_db_values,dfm_data,dfm_data.columns)
        # TODO: error handling
        if len(set_dif_cur2db) > 0:
            logprint('Inconsistency found for multiple fetchs in one day! Stock %s new value set appears: %s' %(item,set_dif_cur2db))
        if len(set_dif_db2cur) > 0:
            logprint('Inconsistency found for multiple fetchs in one day! Stock %s old value set removed: %s' %(item,set_dif_db2cur))
        return

    if process_mode == 'w_check' and len(dfm_db_data) > 0:
        # only insert DB in case the value is different from latest datetime's values
        # build a set to hold all multi values in dfm_data

        # set_cur_values = gcf.dfm_value_to_set(dfm_data,dfm_data.columns)
        # #get latest db data set
        # set_db_values = gcf.dfm_value_to_set(dfm_db_data.loc[dfm_db_data.index[0]],dfm_data.columns)
        # set_dif_cur2db = set_cur_values - set_db_values
        # set_dif_db2cur = set_db_values - set_cur_values

        #get latest db data set
        set_latest_db_values = dfm_db_data.loc[dfm_db_data.index[0]]
        #compare whether the cur dfm values is the same as db dfm values of latest fetching
        set_dif_cur2db = gcf.setdif_dfm_A_to_B(dfm_data,set_latest_db_values,dfm_data.columns)
        set_dif_db2cur = gcf.setdif_dfm_A_to_B(set_latest_db_values,dfm_data,dfm_data.columns)

        if set_dif_cur2db:
            logprint('Stock %s new value set appears:' %item,set_dif_cur2db)
        if set_dif_db2cur:
            logprint('Stock %s old value set removed:' %item,set_dif_db2cur)
        if not set_dif_cur2db and not set_dif_db2cur:
            # no need to update, it is the same as last record
            # logprint('Stock %s value set no changes:' % item)
            return

    # insert into db
    ls_ins_pars = []
    logprint('Insert %s stock %s Period %s' % (table_name,item, last_trading_day))
    # rename the df with new cols name,use rename function with a dict of old column to new column mapping
    ls_colnames_dbinsert = list(map(special_process_col_name,dfm_data.columns))
    ins_str_cols = ','.join(ls_colnames_dbinsert)
    ins_str_pars = ','.join('?' * len(ls_colnames_dbinsert))

    for id in range(len(dfm_data)):
        ls_ins_pars.append((market_id, item, last_trading_daytime, id, timestamp, dict_misc_pars['update_by'])
                       + tuple(dfm_data.loc[id]))

    if ins_str_cols:
        ins_str = '''INSERT INTO %s (Market_ID,Stock_ID,Trans_Datetime,Sqno,Created_datetime,Created_by,%s) VALUES (?,?,?,?,?,?,%s)''' % (
            table_name,ins_str_cols, ins_str_pars)
        # print(ins_str)
        try:
            conn.execute(ins_str, ls_ins_pars)
        except:
            raise



def load_snapshot_dfm_to_db(dfm_log:DataFrame,table_name,mode:str = '', w_timestamp:bool = False ):
    """
    currently support 2 mode:
    1. used for YY_tables as log table update, in this case, w_timestamp = False, func will add a new column Update_time
    2. used as general insert entries into table(no update func, just insert), in this case, set w_timestamp = True
    caution: dfm_log must have same columns as db table.
    :param dfm_log: the dataframe to insert into db
    :param table_name:  the db table name
    :param mode: 'del&recreate' ->del all table entries first; ''-> only insert
    :param w_timestamp: True -> don't add Update_time col; False-> add Update_time col
    :return:
    """
    if not w_timestamp:
        dfm_log['Update_time'] = datetime.now()

    if mode == 'del&recreate':
        conn.execute('DELETE FROM %s' %table_name)

    ls_colnames_dbinsert = list(map(special_process_col_name, dfm_log.columns))
    ins_str_cols = ','.join(ls_colnames_dbinsert)
    ins_str_pars = ','.join('?' * len(ls_colnames_dbinsert))
    ls_ins_pars = []
    for i in range(len(dfm_log)):
        ls_par=[]
        for par in dfm_log.iloc[i]:
            if not pd.isnull(par):
                ls_par.append(par)
            else:
                ls_par.append(None)
        ls_ins_pars.append(ls_par)
    ins_str = "INSERT INTO %s (%s) VALUES (%s)" %(table_name,ins_str_cols,ins_str_pars)
    for ins_par in ls_ins_pars:
        conn.execute(ins_str,ins_par)


def special_process_col_name(tempstr:str):
    """
    due to col name in db has some restriction, need to reprocess dfm columns name to make it align with DB's column name
    SQL server2008里面如果列名里有特殊字符,则会自动在列名前后加上[],如果没有特殊字符,则列名不变;在查询,修改时,如果列名是带方括号的,则必须
    用[]的列名,如果没有带[]的,则列名用[]或不用[]都是可以的.
    此外,同dataframe从sql server2008读出的列名里都是不带[]的.
    所以这里的特殊处理就是
    such as add[]
    :param tempstr:
    :return:
    """
    tempstr = tempstr.replace("[","")
    tempstr = tempstr.replace("]","")
    tempstr = '['+ tempstr+ ']'
    return tempstr

def get_stock_catg(origin:str) -> DataFrame:
    """
    获得股票的分类信息
    :return: dataframe of category
    """
    if origin == "":
        dfm_catg = pd.read_sql_query('''select * from ZCFG_category '''
                                   , conn)
       # print(dfm_stocks)
    else:
        dfm_catg = pd.read_sql_query('''select * from ZCFG_category
                                                where Catg_Origin = '%s' ''' %origin
                                       , conn)
    return dfm_catg

def load_dfm_to_db_cur(dfm_cur_data:DataFrame,key_cols:list,table_name:str,dict_misc_pars:dict,processing_mode:str='w_update'):
    """
    本函数用于单值属性的char的当前数据更新.
    导入的dfm中的数据到table中,processing_mode决定了处理方式:
    1) 'w_update: key_cols存在时, 进行update
    2) "wo_update: key_cols存在时,不进行update

    传入的dataframe中无需包含Trans_Datetime,Created_datetime,Created_by,Last_modified_datetime,Last_modified_by这四列.
    trans_datetime是考虑交易所的交易日历的
    Trans_Datetime默认取当前日期.
    :param dfm:
    :param table_name:
    :param dict_misc_pars:
    :processing_mode:
    :return:
    """
    # load DB contents
    timestamp = datetime.now()
    last_trading_day = gcf.get_last_trading_day()
    last_trading_daytime = gcf.get_last_trading_daytime()
    dfm_db_data = pd.read_sql_query("select * from %s where Trans_Datetime = ?" %table_name
                                        , conn, params=(last_trading_daytime,))
    dfm_cur_data['Trans_Datetime'] = last_trading_daytime
    dfm_insert_data = gcf.dfm_A_minus_B(dfm_cur_data,dfm_db_data,key_cols)
    dfm_update_data = gcf.dfm_A_intersect_B(dfm_cur_data,dfm_db_data,key_cols)

    # gcf.dfmprint(dfm_insert_data)
    if len(dfm_insert_data) > 0:
        ins_str_cols = ''
        ins_str_pars = ''
        ls_ins_pars = []
        for index,row in dfm_insert_data.iterrows():
            # insert logic
            logprint('Insert table %s new Record %s at Period %s' % (table_name, [row[col] for col in key_cols] , last_trading_day))
            # rename the df with new cols name,use rename function with a dict of old column to new column mapping
            ls_colnames_dbinsert = list(map(special_process_col_name,dfm_insert_data.columns))
            ins_str_cols = ','.join(ls_colnames_dbinsert)
            ins_str_pars = ','.join('?' * len(ls_colnames_dbinsert))
            #        print(ins_str_cols)
            #        print(ins_str_pars)
            # convert into datetime type so that it can update into SQL server
            # trans_datetime = datetime.strptime(str(ts_id.date()), '%Y-%m-%d')
            # trans_datetime = ts_id.to_pydatetime()
            ls_ins_pars.append((timestamp, dict_misc_pars['created_by'])
                               + tuple(row))
        if ins_str_cols:
            ins_str = '''INSERT INTO %s (Created_datetime,Created_by,%s) VALUES (?,?,%s)''' % (
                table_name,ins_str_cols, ins_str_pars)
            # print(ins_str)
            try:
                conn.execute(ins_str, ls_ins_pars)
            except:
                raise

    if len(dfm_update_data) >0 and processing_mode == 'w_update':
        # update logic: update the entry
        ls_upt_cols = []
        ls_upt_pars = []
        for index,row in dfm_update_data.iterrows():
            logprint('Update table %s Record %s at Period %s' % (table_name, [row[col] for col in key_cols], last_trading_day))
            # rename the df with new cols name,use rename function with a dict of old column to new column mapping
            ls_colnames_dbupdate = list(map(special_process_col_name,dfm_update_data.columns))
            upt_str_cols = '=?,'.join(ls_colnames_dbupdate) +'=?'+ ", Last_modified_datetime = ?,Last_modified_by=?"
            upt_str_keycols = '=? AND '.join(key_cols) +'=?'
            ls_upt_pars.append(tuple(row) + (timestamp, dict_misc_pars['update_by']) +
                               tuple([row[col] for col in key_cols]))

        if ls_upt_pars:
            update_str = '''UPDATE %s SET %s 
                WHERE %s ''' % (table_name, upt_str_cols,upt_str_keycols)
            conn.execute(update_str, tuple(ls_upt_pars))


def get_col_for_insert(table_name, col_insert_before:str ='Created_datetime'):
    """
    there are 4 fours currently in table template for DD_tables defination, but I want to insert new columns before these
    4 common cols, so I use sys tabel to get the last col name before these 4 standard template cols.
    Since SQL only support "insert after", no "insert before"
    :param table_name:
    :param col_insert_before:
    :return:
    """
    dfm_db1 = pd.read_sql_query(
        "select * from SysColumns Where ID = OBJECT_ID('%s')" %table_name
        , conn)

    #print(dfm_db1)
    insert_offset = dfm_db1[dfm_db1.name == col_insert_before].index[0] -1

    return dfm_db1.iloc[insert_offset]['name']


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
#    conn.execute('INSERT INTO YY_stock_changes_qq ([公布前内容],[公布后内容],[公布日期],[变动日期],[变动项目],[Stock_ID],
    # [Market_ID],[Update_time]) VALUES (?,?,?,?,?,?,?,?)', ('北京中天信会计师事务所', '四川华信(集团)会计师事务所',
    # '2002-01-12 00:00:00', None, '境内会计师事务所', '000155', 'SZ', '2017-09-12 21:13:12'))



    pass
