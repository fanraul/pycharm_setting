# Readme
## Program overview
程序名|数据对象|数据源|存储模式|数据更新函数|其他
-|-|-|-|-|-

1.学号|姓名|分数
2.-|-|-
3.小明|男|75
4.小红|女|79
5.小陆|男|92


|学号|姓名|分数|
|-|-|-|
|小明|男|75|
|小红|女|79|
|小陆|男|92|

| Header 1 | Header 2 |
| -------- | -------- |
| Data 1   | Data 2   |

## Template program for different scenarios
### stock id + transdate as key
+ Current data, web site, single value -> DB: fetch_stock_company_general_info_from_eastmoney
+ Current data, web site, multiple value -> DB:
+ History data, web site, single value  -> DB: 
+ History data, web site, multiple value  -> DB: 
+ Current data, json, single value -> DB: fetch_stock_company_general_info_from_eastmoney
+ Current data, json, multiple value -> DB:
+ History data, json, single value  -> DB: 
+ History data, json, multiple value  -> DB: 
+ Current data, api, single value -> DB: fetch_stock_current_dailybar_from_sina
+ Current data, api, multiple value -> DB: 
+ History data, api, single value  -> DB: fetch_stock_dailybar_from_tquant
+ History data, api, multiple value  -> DB: 

### stock id  as key
### obj other than stock id + transdate as key
### obj other than stock id as key