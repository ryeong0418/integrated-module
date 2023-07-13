from src import common_module as cm
from src.common.utils import InterMaxUtils, SummarizerUtils, SqlUtils
from src.common.constants import SystemConstants
from sql.common_sql import CommonSql
import glob
from datetime import datetime


class Summarizer(cm.CommonModule):

    def __init__(self, logger):
        super().__init__(logger)

    def main_process(self):
        self.logger.debug('Summarizer')
        self._init_sa_target()

        self.st_engine = self._get_st_engine()
        self.st_conn = self._get_st_conn()

        self.excute_summarizer()

    def excute_summarizer(self):

        date_conditions = InterMaxUtils.set_intermax_date(self.config['args']['s_date'],
                                                          self.config['args']['interval'])

        summarizer_file_list = glob.glob(self.sql_file_root_path + '/'+SystemConstants.TEMP_PATH + '/*.txt')

        for date in date_conditions:
            start_date, end_date = SummarizerUtils.summarizer_set_date(date)
            date_dict = {'StartDate': start_date, 'EndDate': end_date}

            for summarizer_temp_file in summarizer_file_list:
                temp_table_name = summarizer_temp_file.split('.')[0].split('\\')[-1]

                replace_dict = {'table_name': temp_table_name}
                delete_table_query = SqlUtils.sql_replace_to_dict(CommonSql.DELETE_TABLE_DEFAULT_QUERY,
                                                                  replace_dict)

                with open(summarizer_temp_file, mode='r', encoding='utf-8') as file:
                    query = file.read()
                    temp_query = SqlUtils.sql_replace_to_dict(query, date_dict)

                    try:
                        self.st._default_execute_query(self.st_conn, delete_table_query)
                        temp_df = self.st._get_target_data_by_query(self.st_conn, temp_query, temp_table_name)
                        self.st._insert_engine_by_df(self.st_engine, temp_table_name, temp_df)
    #
                    except Exception as e:
                        self.logger.exception(
                            f"{summarizer_temp_file.split('.')[0]} table, create_temp_table execute error")
                        self.logger.exception(e)

            self.summary_join(date_dict)

    def summary_join(self, date_dict):

        """
        ae_txn_detail_summary_temp, ae_txn_sql_detail_summary_temp 테이블 조인 기능 함수
        날짜 별로 delete -> join data insert 기능 수행
        """

        summarizer_file_list = glob.glob(self.sql_file_root_path + '/' + SystemConstants.SUMMARY_PATH + '/*.txt')
        delete_query = CommonSql.DELETE_SUMMARY_TABLE_BY_DATE_QUERY

        for summary_file in summarizer_file_list:

            with open(summary_file, mode='r', encoding='utf-8') as file:
                query = file.read()

                join_query = SqlUtils.sql_replace_to_dict(query, date_dict)
                table_name = summary_file.split('.')[0].split('\\')[-1]
                datetime_format = datetime.strptime(date_dict['StartDate'].split()[0], '%Y-%m-%d')
                formatted_date = datetime_format.strftime('%Y%m%d')
                delete_dict = {'table_name': table_name, 'date': formatted_date}

                try:
                    sa_delete_query = SqlUtils.sql_replace_to_dict(delete_query, delete_dict)
                    self.st._default_execute_query(self.st_conn, sa_delete_query)
                    self.logger.info(f"delete query execute : {sa_delete_query}")

                    for df in self.st._get_df_by_chunk_size(self.st_engine, join_query):
                        self.st._insert_engine_by_df(self.st_engine, table_name, df)

                except Exception as e:
                    self.logger.exception(f"{summary_file.split('.')[0]} table, summary insert execute error")
                    self.logger.exception(e)
