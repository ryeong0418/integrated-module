import os
import psycopg2 as db

from src import common_module as cm
from src.analysis_target import SaTarget
from src.common.constants import SystemConstants
from src.common.utils import TargetUtils, SystemUtils


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

        TargetUtils.folder_check(root)

        analysis_conn_str = TargetUtils.get_db_conn_str(self.config['analysis_repo'])
        sa_conn = db.connect(analysis_conn_str)

        # # 시각화 sql filenames 획득, 시각화 데이터 sql 요청
        sql_name_list = os.listdir(query_folder)
        for sql_name in sql_name_list:
            sql_query = self.st.visualization_data(query_folder, sql_name)

            # data frame data 전처리
            df = TargetUtils.get_target_data_by_query(self.logger, sa_conn, sql_query)
            result_df = TargetUtils.visualization_data_processing(df)

            # excel 존재 유무
            TargetUtils.excel_export(excel_file, sql_name, result_df)
