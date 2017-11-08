Global_Job_Log_Base_Direction = 'C:/00_RichMinds/log/'

weblinks = {
    'stock_list_easymoney': 'http://quote.eastmoney.com/stocklist.html',   # obselete
    'stock_change_record_qq': 'http://stock.finance.qq.com/corp1/profile.php?zqdm=%(stock_id)s',
    'stock_category_qq': 'http://stockapp.finance.qq.com/mstats/?mod=all', #obselete
    'stock_category_w_detail_qq': ["http://stock.gtimg.cn/data/view/bdrank.php?&t=%(catg_type)s/averatio&p=1&o=0&l=9999&v=list_data",
                                   "http://qt.gtimg.cn/q=%(catg_list)s",
                                   'http://stock.gtimg.cn/data/index.php?appn=rank&t=pt%(catg_code)s/chr&p=1&o=0&l=9999&v=list_data'],
    'stock_structure_sina':'http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockStructure/stockid/%s.phtml',
    'stock_core_concept_eastmoney':"http://emweb.securities.eastmoney.com/PC_HSF10/CoreConception/CoreConceptionAjax?code=%s",
    'stock_shareholder_eastmoney':'http://emweb.securities.eastmoney.com/PC_HSF10/ShareholderResearch/ShareholderResearchAjax?code=%s',
    'stock_general_info_eastmoney':'http://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax?code=%s',
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
    'stock_category_relation_eastmoney': 'DD_stock_category_assignment_eastmoney',  # TODO
    'stock_shareholder_number_eastmony': 'DD_stock_shareholder_number_eastmoney',
    'stock_top_ten_tradable_shareholder_eastmoney': 'DD_stock_shareholder_top_ten_tradable_eastmoney',
    'stock_top_ten_shareholder_eastmoney':'DD_stock_shareholder_top_ten_eastmoney',
    'stock_top_ten_shareholder_shares_changes_eastmoney':'DD_stock_shareholder_top_ten_shares_changes_eastmoney',
    'stock_fund_shareholder_eastmoney':'DD_stock_shareholder_fund_eastmoney',
    'stock_nontradable_shares_release_eastmoney':'DD_stock_shareholder_nontradable_shares_release_eastmoney',
    'stock_company_general_info_eastmoney':'DD_stock_company_general_info_eastmoney',
    'stock_company_issuance_info_eastmoney':'DD_stock_company_issuance_info_eastmoney',
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


# the job sheduler for background programs
# key is the program name
scheduleman = {
    'fetch_stock_fin_reports_from_tquant':{
        'rule':'W',
        'weekdays':[4]  # Friday
    } ,
    'fetch_stocklist_from_Tquant':{
        'rule': 'W',
        'weekdays': [0, 1, 2, 3, 4]  # monday to Friday
    },
    'fetch_stock_category_and_daily_status_from_qq':{
        'rule': 'W',
        'weekdays': [0, 1, 2, 3, 4, 6]  # monday to Friday and Sunday
    },
    'fetch_stock_change_record_from_qq':{
        'rule': 'W',
        'weekdays': [4]  # Friday
    },
    'fetch_stock_core_concept_from_eastmoney':{
        'rule': 'W',
        'weekdays': [0, 1, 2, 3, 4, 6]  # monday to Friday and Sunday
    },
    'fetch_stock_structure_hist_from_sina':{
        'rule': 'W',
        'weekdays': [4]  # Friday
    },
    'fetch_stock_shareholder_from_eastmoney':{
        'rule': 'W',
        'weekdays': [0,2,4]  # Monday,wednesday,Friday
    },
    'fetch_stock_company_general_info_from_eastmoney':{
        'rule': 'W',
        'weekdays': [2,]  # wednesday
    }

}

# the date which shouldn't run the job
excluded_dates =['2018-1-1',]