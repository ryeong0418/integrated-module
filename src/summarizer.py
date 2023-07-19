from src import common_module as cm
from src.common.utils import InterMaxUtils, SummarizerUtils, SqlUtils, SystemUtils
from src.common.constants import SystemConstants
from sql.common_sql import CommonSql
from datetime import datetime


class Summarizer(cm.CommonModule):

    """
    ae_txn_detail_summary_temp, ae_txn_sql_detail_summary_temp 테이블에 데이터 insert 후,
    두 테이블을 join하여 ae_txn_sql_summary 테이블에 데이터 insert
    """

    def __init__(self, logger):
        super().__init__(logger)

    def main_process(self):
        self.logger.debug('Summarizer')
        self._init_sa_target()

        self.excute_summarizer()

    def excute_summarizer(self):

        """
        2개의 temp table에 데이터 delete -> insert 로직을 실행하여 최신 날짜 하루만 데이터 insert 되도록 한다.
        ae_txn_sql_summary table은 중복 데이터를 제외하고 날짜별로 데이터 insert 실행해 누적되도록 한다.
        """

        summarizer_file_path = self.sql_file_root_path+SystemConstants.TEMP_PATH
        summarizer_file_list = SystemUtils.get_filenames_from_path(summarizer_file_path,'','txt')

        delete_query = CommonSql.DELETE_TABLE_DEFAULT_QUERY
        date_conditions = InterMaxUtils.set_intermax_date(self.config['args']['s_date'],
                                                          self.config['args']['interval'])

        for date in date_conditions:

            start_date, end_date = SummarizerUtils.summarizer_set_date(date)
            date_dict = {'StartDate': start_date, 'EndDate': end_date}

            for summarizer_temp_file in summarizer_file_list:
                temp_table_name = summarizer_temp_file.split('.')[0].split('\\')[-1]
                with open(summarizer_file_path+summarizer_temp_file, mode='r', encoding='utf-8') as file:
                    query = file.read()
                temp_query = SqlUtils.sql_replace_to_dict(query, date_dict)

                try:
                    delete_dict = {'table_name': temp_table_name}
                    self.st.delete_data(delete_query, delete_dict)

                    for temp_df in self.st.get_table_data_by_chunksize(temp_query,
                                                                       self.config['data_handling_chunksize']*5):

                        self.st.insert_detail_data(temp_df, temp_table_name)

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
        summarizer_file_path = self.sql_file_root_path+SystemConstants.SUMMARY_PATH
        summarizer_file_list = SystemUtils.get_filenames_from_path(summarizer_file_path,'','txt')

        delete_query = CommonSql.DELETE_SUMMARY_TABLE_BY_DATE_QUERY

        for summary_file in summarizer_file_list:

            with open(summarizer_file_path+summary_file, mode='r', encoding='utf-8') as file:
                query = file.read()
                join_query = SqlUtils.sql_replace_to_dict(query, date_dict)
                table_name = summary_file.split('.')[0].split('\\')[-1]
                datetime_format = datetime.strptime(date_dict['StartDate'].split()[0], '%Y-%m-%d')
                formatted_date = datetime_format.strftime('%Y%m%d')
                delete_dict = {'table_name': table_name, 'date': formatted_date}

                try:
                    self.st.delete_data(delete_query, delete_dict)
                    for df in self.st.get_table_data_by_chunksize(join_query,self.config['data_handling_chunksize']*5):
                        self.st.insert_detail_data(df, table_name)

                except Exception as e:
                    self.logger.exception(f"{summary_file.split('.')[0]} table, summary insert execute error")
                    self.logger.exception(e)
