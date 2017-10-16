from datetime import datetime

import R10_sensor.fetch_stock_fin_reports_from_tquant as fetch_stock_fin_reports_from_tquant
import R10_sensor.fetch_stocklist_from_Tquant as fetch_stocklist_from_Tquant
import R10_sensor.fetch_stock_category_and_daily_status_from_qq as fetch_stock_category_and_daily_status_from_qq
import R10_sensor.fetch_stock_change_record_from_qq as fetch_stock_change_record_from_qq
import R10_sensor.fetch_stock_core_concept_from_eastmoney as fetch_stock_core_concept_from_eastmoney
import R10_sensor.fetch_stock_structure_hist_from_sina as fetch_stock_structure_hist_from_sina



import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint

if __name__ == '__main__':
    gcf.setup_log_file('sensor_mainjob')

    weekday = datetime.now().weekday()
    # in python, Monday is 0,Sunday is 6
    if weekday > 3:
        weekly_update = True
    else:
        weekly_update = False


    # daily update
    # step 10: update stock list
    gcf.func_call_with_trace(fetch_stocklist_from_Tquant.fetch2DB,program_name='fetch_stocklist_from_Tquant')

    # step 20: update qq category info
    gcf.func_call_with_trace(fetch_stock_category_and_daily_status_from_qq.fetch2DB,
                             program_name='fetch_stock_category_and_daily_status_from_qq')
    # step 30: update eastmoney core concept
    gcf.func_call_with_trace(fetch_stock_core_concept_from_eastmoney.auto_reprocess,
                             program_name='fetch_stock_core_concept_from_eastmoney')


    # weekly update
    if weekly_update:
    # only fetch financial data at Friday, Saturday, Sunday

        # step 10: update 3 fin reports
        gcf.func_call_with_trace(fetch_stock_fin_reports_from_tquant.fetch2DB,program_name='fetch_stock_fin_reports_from_tquant')

        # step 20: update stock changes hist record, especially name
        fetch_stock_change_record_from_qq.mode = 'update_log'
        gcf.func_call_with_trace(fetch_stock_change_record_from_qq.auto_reprocess,
                                 program_name='fetch_stock_change_record_from_qq')
        # step30: update stock structure info, 股本信息.
        gcf.func_call_with_trace(fetch_stock_structure_hist_from_sina.auto_reprocess,program_name='fetch_stock_structure_hist_from_sina')


    gcf.send_daily_job_log('Job is successfully finished, please check log file for details.')




#     dt_args_w_name={}
#     dt_args_w_name['sep'] = ','
#     func_call_with_trace('print','this is a test','hello',dt_args_w_name = dt_args_w_name)
#     func_call_with_trace('print_list_nice', [(1,2)]*1000000)