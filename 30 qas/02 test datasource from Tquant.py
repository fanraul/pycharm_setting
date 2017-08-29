import tquant.getdata as gt
from pandas import DataFrame as df

ls_financial = gt.get_financial('002851')

tmpfile = open('tempfile','w')

for item in ls_financial:
    print(item,file = tmpfile)