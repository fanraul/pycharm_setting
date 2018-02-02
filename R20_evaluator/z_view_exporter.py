import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import re
from datetime import datetime
from R50_general.general_helper_funcs import logprint
import R50_general.DBconnectionmanager as dbm
import R50_general.dfm_to_table_common as df2db
import R50_general.advanced_helper_funcs as ahf
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf

dbm.DB_connection_string = 'mssql+pyodbc://Richmind:121357468@Richmind_Remote32'
view_name = 'BD_L2_00_cn_stocklist_with_general_info_now'
df2db.get_data_from_DB(view_name).to_excel('views.xls')