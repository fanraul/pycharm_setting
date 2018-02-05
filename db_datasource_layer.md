## Overview
in sql server, we use views and store procedure(sp) to do the basic data extraction design so that python can get data directly and easily. And by this way, python code is de-linked with db table structures and table source(one data source may have different sources,from api or from web scrap).

this document records:
+ the design guide line
+ views created list
+ store procedure list


## Design guide line
views and sps are designed layer by layer
level1 layer is the lowest layer normally based on one table or two tables
level2 build based on level1 views and tables
level3 build based on lower views and tables 
and so on...

## SQL server views list
| View | Description | datasource | conditions |comment|Used in program|
|---|---|---|---|---|---|
|BD_L1_00_cn_stock_code_list|上海和深圳股票清单|stock_basic_info(tquant)|-|带上市日期|used in get_cn_stocklist in df2db module,don't change this view|
|BD_L1_05_cn_stock_general_info_now|上海和深圳股票基本信息的最新数据view|DD_stock_company_general_info_eastmoney|-|-|-|
|BD_L2_00_cn_stocklist_with_general_info_now|上海和深圳股票清单,含基本信息,发行信息的最新数据view|BD_L1_00_cn_stock_code_list BD_L1_05_cn_stock_general_info_now|-|-|-|


## SQL server store procedure

