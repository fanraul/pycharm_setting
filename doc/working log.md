## 2018-2-7


## 2018-2-6
1. program to get the cn market stock name changes is obsolete due to the web link doesn't work any more. so program **fetch_stock_change_record_from_qq** is obsolete as well
2. build another program **fetch_stock_name_changes_from_Tquant** to update the stock name change table inside program **fetch_stocklist_from_Tquant** after update stock list into db
3. build a general program **zz_insert_db_from_excel** which load data from excel and convert them into an excel which contains a list of *insert SQL statement* , this program can be used for manual db table insert scenario
4. deactive program **fetch_stock_category_and_daily_status_from_qq** in the *main sensor* daily job due to poor performance(web block by qq) and poor data quality
5. qq is bad data source for web scrap, the quality of stock infos in qq is poor, only scrap data from qq if the data is hist data can be scraped weekly eg. and there is no alternative data source.
6. manual prepare the gap data of stock name changes between 2018/1/13 and 2018/2/6 due to **fetch_stock_change_record_from_qq** is obsolete. The data may not be very accurate and I think it doesn't cause issue.
7. build a general function **exec_store_procedure** inside program **df2db** to call SQL server store procedure.
8. program **fetch_stock_name_changes_from_Tquant** go live in production.