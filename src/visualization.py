import os

from src import common_module as cm
from src.analysis_target import SaTarget
from src.common.constants import SystemConstants
from src.common.utils import SystemUtils


class Visualization(cm.CommonModule):

    def __init__(self, logger):
        self.logger = logger
        self.logger.info("visualization init")
        self.st = None

    def main_process(self):
        self.logger.debug('Visualization')
        self.st = SaTarget(self.logger, self.config)
        self.st.init_process()

        root = self.config['home']
        query_folder = root + '/' + SystemConstants.SQL_PATH
        excel_file = root + '/' + SystemConstants.CSV_PATH

        SystemUtils.folder_check(root)

        sql_name_list = os.listdir(query_folder)

        for sql_name in sql_name_list:
            sql_query = SystemUtils.get_file_in_path(query_folder, sql_name)

            df = self.st.sql_query_convert_df(sql_query)
            result_df = SystemUtils.visualization_data_processing(df)

            SystemUtils.excel_export(excel_file, sql_name, result_df)
