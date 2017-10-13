weblinks = {
    'stock_list_easymoney': 'http://quote.eastmoney.com/stocklist.html',   # obselete
    'stock_change_record_qq': 'http://stock.finance.qq.com/corp1/profile.php?zqdm=%(stock_id)s',
    'stock_category_qq': 'http://stockapp.finance.qq.com/mstats/?mod=all', #obselete
    'stock_category_w_detail_qq': ["http://stock.gtimg.cn/data/view/bdrank.php?&t=%(catg_type)s/averatio&p=1&o=0&l=9999&v=list_data",
                                   "http://qt.gtimg.cn/q=%(catg_list)s",
                                   'http://stock.gtimg.cn/data/index.php?appn=rank&t=pt%(catg_code)s/chr&p=1&o=0&l=9999&v=list_data'],
    'stock_structure_sina':'http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockStructure/stockid/%s.phtml',
    'stock_core_concept_eastmoney':"http://emweb.securities.eastmoney.com/PC_HSF10/CoreConception/CoreConceptionAjax?code=%s",
}


#table created by program dfm2table has prefix DD!
dbtables = {
    'finpreports_Tquant' :['DD_stock_fin_balance_tquant', 'DD_stock_fin_profit_tquant','DD_stock_fin_cashflow_tquant'],
    'name_hist_qq': 'DD_stock_name_change_hist_qq',
    'fixed_basic_info_tquant': 'DD_stock_fixed_basic_info_tquant',
    'basic_info1_tquant': 'DD_stock_basic_info1_tquant',
    'stock_category_relation_qq':'DD_stock_category_assignment_qq',
    'category_daily_trans_qq': 'DD_category_daily_noauth_qq',
    'stock_structure_sina':'DD_stock_structure_sina',
    'stock_core_concept_eastmoney':'DD_stock_core_concept_eastmoney',
    'stock_category_relation_eastmoney': 'DD_stock_category_assignment_eastmoney'
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
dbtemplate_stock_wo_date = """
CREATE TABLE [%(table)s](
	[Market_ID] [nvarchar](50) NOT NULL,
	[Stock_ID] [nvarchar](50) NOT NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY 
(
	[Market_ID] ASC,
	[Stock_ID] ASC
))
"""
dbtemplate_stock_date_multi_value = """
CREATE TABLE [%(table)s](
	[Market_ID] [nvarchar](50) NOT NULL,
	[Stock_ID] [nvarchar](50) NOT NULL,
	[Trans_Datetime] [datetime] NOT NULL,
	[Sqno] [int] NOT NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY 
(
	[Market_ID] ASC,
	[Stock_ID] ASC,
	[Trans_Datetime] ASC,
	[Sqno] ASC
))
"""
dbtemplate_catg_date = """
CREATE TABLE [%(table)s](
	[Catg_Type] [nvarchar](50) NOT NULL,
	[Catg_Name] [nvarchar](50) NOT NULL,
	[Trans_Datetime] [datetime] NOT NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY 
(
	[Catg_Type] ASC,
	[Catg_Name] ASC,
	[Trans_Datetime] ASC
))
"""