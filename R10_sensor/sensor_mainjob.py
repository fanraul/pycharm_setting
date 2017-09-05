import R10_sensor.fetch_stocklist_from_eastmoney as fetch_stocklist
import R10_sensor.fetch_stock_3fin_report_from_tquant as fetch_financials

from datetime import datetime as dt

fetch_stocklist.fetch2DB()

weekday = dt.now().weekday()


# only fetch financial data at Friday, Saturday, Sunday
# in python, Monday is 0,Sunday is 6
if weekday > 3:
    fetch_financials.fetch2DB()




