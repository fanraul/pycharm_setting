import tquant.getdata as gt
from pandas import DataFrame as df

"get_financial 获得股票财务报表接口"
ls_financial = gt.get_financial('992851')
print(type(ls_financial[0]))

ls_financial[0].to_excel("资产负债表.xls")
ls_financial[1].to_excel("利润表.xls")
ls_financial[2].to_excel("现金流量表.xls")

