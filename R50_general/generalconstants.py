log = True

weblinks = {
    'stock_list_easymoney': 'http://quote.eastmoney.com/stocklist.html'
}

#table created by program dfm2table has prefix DD!
dbtables = {
    'finpreports_Tquant' :['DD_stock_fin_balance_tquant', 'DD_stock_fin_profit_tquant','DD_stock_fin_cashflow_tquant']
}

dbtemplate_stock_date = """
CREATE TABLE [%(table)s](
	[Market_ID] [nvarchar](50) NOT NULL,
	[Stock_ID] [nvarchar](50) NOT NULL,
	[Trans_Datetime] [datetime] NOT NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY 
(
	[Market_ID] ASC,
	[Stock_ID] ASC,
	[Trans_Datetime] ASC
))
"""



def logprint(*args, sep=' ',  end='\n',  file=None ):
    if log:
        print(*args,sep=' ', end='\n', file=None)
        print(*args, sep=' ', end='\n', file= open('templog.txt','a'))
