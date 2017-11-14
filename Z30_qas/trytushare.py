import tushare as ts
import R50_general.general_helper_funcs as ghf

# dfm1 = ts.get_today_all()
#
# ghf.dfmprint(dfm1)

dfm1 = ts.forecast_data(2017,4)
ghf.dfmprint(dfm1)