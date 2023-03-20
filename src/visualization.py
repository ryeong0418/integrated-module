import os
import glob
from pathlib import Path

from src import common_module as cm
from src.analysis_target import SaTarget
from src.common.constants import SystemConstants
from src.common.utils import SystemUtils
from datetime import datetime


class Visualization(cm.CommonModule):

    def __init__(self, logger):

        self.logger = logger
        self.logger.info("visualization init")
        self.st = None

    def main_process(self):

        self.logger.debug('Visualization')
        self.st = SaTarget(self.logger, self.config)
        self.st.init_process()

        query_folder = self.config['home'] + '/' + SystemConstants.SQL_PATH
        excel_path = self.config['home'] + '/' + SystemConstants.EXCEL_PATH

        Path(query_folder).mkdir(exist_ok=True, parents=True)
        Path(excel_path).mkdir(exist_ok=True, parents=True)

        sql_name_list = os.listdir(query_folder)
        sql_name_list_sort = sorted(sql_name_list,key=lambda x: (x[0:3]))

        for sql_name in sql_name_list_sort:
            sql_query = SystemUtils.get_file_in_path(query_folder, sql_name)
            df = self.st.sql_query_convert_df(sql_query)
            result_df = SystemUtils.data_processing(df)
            sheet_name_txt = sql_name.split('.')[0]

            now_day = datetime.now()
            prtitionDate = now_day.strftime('%y%m%d')

            if sheet_name_txt[2] == '1':
                excel_file = excel_path + "/" + sheet_name_txt + "_" + prtitionDate + '.xlsx'
                SystemUtils.excel_export(excel_file, sheet_name_txt, result_df)

            else:
                sheet_name = sheet_name_txt
                xlsx_file_list = glob.glob(excel_path+"/**.xlsx")

                for i in xlsx_file_list:
                    excel_file_name = i.split('\\')[-1]
                    excel_date = excel_file_name.split('_')[-1].split(".")[0]

                    if sheet_name[0] == excel_file_name[0] and excel_date == prtitionDate:
                        SystemUtils.excel_export(i, sheet_name, result_df)


