import tquant.getdata as gt
import tquant.myquant as mt
from pandas import DataFrame as df


"get_financial 获得股票财务报表接口"
ls_financial = gt.get_financial('300274')
print(type(ls_financial[0]))

ls_financial[0].to_excel("资产负债表.xls")
ls_financial[1].to_excel("利润表.xls")
ls_financial[2].to_excel("现金流量表.xls")



#dfm_ipo = gt.ths_ipo()
#print(dfm_ipo)


#var=gt.get_tfp('2016-10-21')
#print(var)

# ls_brief = gt.get_brief(['600100','600000','600030','000002','300314'])
#
# ls_brief.to_excel('股票简略信息.xls')
#
# dfm_latest = gt.get_lastest(['600100','600000','600030','000002','300314'])
# dfm_latest.to_excel()

#dfm_index = mt.get_constituents(index_symbol ='SHSE.000300')
#print(dfm_index)

df_shstocklist = mt.get_szse()
df_shstocklist.to_excel('深圳股票清单.xls')

df_indexlist = mt.get_index()
df_indexlist.to_excel('沪深指数列表.xls')