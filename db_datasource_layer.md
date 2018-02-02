## Overview
in sql server, we use views and store procedure(sp) to do the basic data extraction design so that python can get data directly and easily. And by this way, python code is de-linked with db table structures and table source(one data source may have different sources,from api or from web scrap).

this document records:
+ the design guide line
+ views created list
+ store procedure list


## Design guide line
views and sps are designed layer by layer
level1 layer is always based on one table
level2 build based on level1 views
level3 build based on level2 and so on...

## SQL server views list
| View | Description | datasource | conditions |comment|
|---|---|---|---|---|
|BD_L1_00_cn_stock_code_list|上海和深圳股票清单|stock_basic_info(tquant)|-|没有上市日期|
|BD_L1_05_cn_stock_general_info_now|上海和深圳股票最新基本信息|DD_stock_company_general_info_eastmoney|-|-|



## SQL server store procedure

