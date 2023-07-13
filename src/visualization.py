import os
from pathlib import Path
from datetime import datetime

from src import common_module as cm
from src.common.constants import SystemConstants
from src.common.utils import SystemUtils


class Visualization(cm.CommonModule):

    def __init__(self, logger):
        super().__init__(logger)

    def main_process(self):

        self.logger.debug('Visualization')
        self._init_sa_target()

        query_folder = self.config['home'] + '/' + SystemConstants.SQL_PATH
        excel_path = self.config['home'] + '/' + SystemConstants.EXCEL_PATH

        SystemUtils.get_filenames_from_path(query_folder)
        SystemUtils.get_filenames_from_path(excel_path)

        sql_name_list = os.listdir(query_folder)
        sql_split = [i.split(" ") for i in sql_name_list]
        sql_split_sort = sorted(sql_split, key=lambda x: (int(x[0].split("-")[0]), int(x[0].split("-")[1])))
        sql_name_list_sort = [" ".join(i) for i in sql_split_sort]

        for sql_name in sql_name_list_sort:
            sql_query = SystemUtils.get_file_in_path(query_folder, sql_name)
            df = self.st.sql_query_convert_df(sql_query)
            result_df = SystemUtils.data_processing(df)
            sheet_name_txt = sql_name.split('.')[0]
            now_day = datetime.now()
            prtitionDate = now_day.strftime('%y%m%d')
            sql_number = sheet_name_txt.split(' ')[0].split('-')[1]

            if sql_number == '1' and len(sql_number) == 1:
                excel_file = excel_path + "/" + sheet_name_txt + "_" + prtitionDate + '.xlsx'
                SystemUtils.excel_export(excel_file, sheet_name_txt, result_df)

            else:
                SystemUtils.excel_export(excel_file, sheet_name_txt, result_df)
