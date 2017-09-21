from datetime import datetime as dt

import R10_sensor.fetch_stock_fin_reports_from_tquant as fetch_financials
import R10_sensor.fetch_stocklist_from_Tquant as fetch_stocklist

fetch_stocklist.fetch2DB()

weekday = dt.now().weekday()


# only fetch financial data at Friday, Saturday, Sunday
# in python, Monday is 0,Sunday is 6
if weekday > 3:
    fetch_financials.fetch2DB()




