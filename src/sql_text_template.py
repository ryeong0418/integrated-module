import time
import pandas as pd
import inspect

from src import common_module as cm
from src.common.module_exception import ModuleException
from src.common.enum_module import MessageEnum
from src.common.background_task import BackgroundTask
from src.common.timelogger import TimeLogger
from src.common.constants import SystemConstants
from src.common.utils import MaxGaugeUtils, SqlUtils
from src.drain.drain_worker import DrainWorker
from resources.logger_manager import Logger


class SqlTextTemplate(cm.CommonModule):
    """
    SqlTextTemplate Class

    리터럴 쿼리 분석을 위한 Class
    """

    def __init__(self, logger):
        super().__init__(logger)
        self.sql_text_template_logger = None
        self.chunk_size = 0
        self.sql_template_select_only = False
        self.chd_threads = []
        self.wait_times_cnt = 0
        self.sel_worker = None
        self.etc_worker = None
        self.extract_cnt = 1000

    def main_process(self):
        if not self.config['intermax_repo']['use'] and not self.config['maxgauge_repo']['use']:
            error_code = 'E004'
            self.logger.error(MessageEnum[error_code].value)
            raise ModuleException(error_code)

        self.chunk_size = self.config.get('data_handling_chunksize', 10_000) * 10
        self.sql_template_select_only = self.config.get('sql_template_select_only', False)

        self._add_sql_text_template_logger()
        self._init_sa_target()

        self.sel_worker = DrainWorker(self.config, self.sql_text_template_logger, "select")
        self.sel_worker.init_drain()

        self.etc_worker = DrainWorker(self.config, self.sql_text_template_logger, "etc")
        self.etc_worker.init_drain()

        if self.config['intermax_repo']['use']:
            self._was_sql_text_template()

            self._wait_end_of_threads()
            self._update_unanalyzed_was_sql_text()

        elif self.config['maxgauge_repo']['use']:
            self._db_sql_text_template()
            self._wait_end_of_threads()

        self._save_top_cluster_template(self.extract_cnt)

        self._print_drain_tree()

    def _wait_end_of_threads(self):
        """
        cluster_id를 update하는 thread 종료를 기다리기 위한 함수
        :return:
        """
        self.logger.debug(f"Total child thread count {len(self.chd_threads)}")

        while any(t.is_alive() for t in self.chd_threads):
            self.wait_times_cnt += 1
            self.logger.info(f"Child thread waiting one seconds.. {self.wait_times_cnt} times")
            time.sleep(1)

        [self.chd_threads.pop(idx) for idx, t in enumerate(self.chd_threads) if not t.is_alive()]

    def _add_sql_text_template_logger(self):
        """
        Drain 모듈에서 sql text template 관련 logging 위해 looger를 생성하는 함수
        :return:
        """
        self.sql_text_template_logger = Logger(self.config['env']).\
            get_default_logger(self.config['log_dir'], SystemConstants.SQL_TEXT_TEMPLATE_LOG_FILE_NAME)

    def _was_sql_text_template(self):
        """
        Was sql text 전처리 및 drain 알고리즘으로 cluster 분석 하는 함수
        :return:
        """
        self.sql_text_template_logger.info("--- Start was sql text drain processing")

        start_time = time.time()

        for df in self.st.get_ae_was_sql_text_by_no_cluster(chunk_size=self.chunk_size):

            with TimeLogger(inspect.currentframe().f_code.co_name, self.sql_text_template_logger):
                sel_df, etc_df = self._preprocessing(df)

            self._drain_match_and_upt(sel_df, etc_df, 'was', self.st.update_cluster_id_by_sql_id)

        self._after_drain_finished(start_time)

    def _preprocessing(self, df):
        """
        sql text 전처리 함수
        :param df: 전처리 전 데이터 프레임
        :return: 전처리 후 select, ~select 데이터 프레임
        """
        sel_list = ['select']
        etc_list = ['insert', 'update', 'delete']

        filter_list = []
        filter_list.extend(sel_list)

        if not self.sql_template_select_only:
            filter_list.extend(etc_list)

        df = df[df['sql_text'].str.contains('|'.join(filter_list), na=False, case=False)]
        df = df[~df['sql_text'].str.contains('sql is too big', na=False, case=False)]

        df = SqlUtils.remove_unnecess_char(df, 'sql_text', contains_comma=True)
        df['sql_text'] = df['sql_text'].str.lower().str.replace(r'\s+', ' ', regex=True)

        sel_df = df[df['sql_text'].str.contains('|'.join(sel_list), na=False, case=False)]
        etc_df = df[~df['sql_text'].str.contains('|'.join(sel_list), na=False, case=False)]

        sel_df = SqlUtils.rex_processing(sel_df)
        etc_df = SqlUtils.rex_processing(etc_df)

        return sel_df, etc_df

    def _print_drain_tree(self):
        """
        Drain에서 분석한 분석 tree를 출력하기 위한 함수
        :return:
        """
        self.sel_worker.print_drain_tree()
        self.etc_worker.print_drain_tree()

    def _save_top_cluster_template(self, extract_cnt=1000):
        """
        Top cluster의 갯수 및 template 저장하기 위한 함수
        :param extract_cnt: 저장하려는 cluster 갯수
        :return:
        """
        sel_cluster_df = self.sel_worker.get_top_cluster_template()
        etc_cluster_df = self.etc_worker.get_top_cluster_template()

        cluster_df = pd.concat([sel_cluster_df, etc_cluster_df])

        return_df = self.st.get_cluster_cnt_by_grouping(extract_cnt)

        merged_df = pd.merge(cluster_df, return_df, on=['cluster_id'],)
        merged_df.sort_values('cluster_cnt', ascending=False, inplace=True)

        self.st.insert_ae_sql_template(merged_df)

    def _update_unanalyzed_was_sql_text(self):
        """
        was sql text 중 분석하지 않는 데이터 처리를 위한 함수
        :return:
        """
        self.st.update_unanalyzed_was_sql_text()

    def _db_sql_text_template(self):
        """
        DB sql text 전처리 및 drain 알고리즘으로 cluster 분석 하는 함수
        :return:
        """
        start_time = time.time()

        ae_db_info_df = self.st.get_ae_db_info()
        ae_db_infos = ae_db_info_df['lpad_db_id'].to_list()

        date_conditions = MaxGaugeUtils.set_maxgauge_date(
            self.config['args']['s_date'], self.config['args']['interval']
        )

        for partition_key in [f"{date}{db_info}" for db_info in ae_db_infos for date in date_conditions]:

            for df in self.st.get_ae_db_sql_text_by_1seq(partition_key, chunksize=self.chunk_size):
                if len(df) == 0:
                    break

                results = self.st.get_all_ae_db_sql_text_by_1seq(df, chunksize=self.chunk_size)

                grouping_df = MaxGaugeUtils.reconstruct_by_grouping(results)

                with TimeLogger(inspect.currentframe().f_code.co_name, self.sql_text_template_logger):
                    sel_df, etc_df = self._preprocessing(grouping_df)

                self._drain_match_and_upt(sel_df, etc_df, 'db', self.st.update_cluster_id_by_sql_id)

        self._after_drain_finished(start_time)

    def _after_drain_finished(self, start_time):
        """
        Drain 분석 종료 후 결과를 로깅하기 위한 함수
        :param start_time: Drain 분석 시작 시간
        :return:
        """
        total_match_cnt = self.sel_worker.line_count + self.etc_worker.line_count

        time_took = time.time() - start_time
        rate = total_match_cnt / time_took
        self.logger.info(f"--- Done processing file in {time_took:.2f} sec. "
                         f"Total of {total_match_cnt} lines, rate {rate:.1f} lines/sec")

    def _drain_match_and_upt(self, sel_df, etc_df, target, upt_func=None):
        """
        Drain 분석 후 결과를 저장 하기 위한 함수
        :param sel_df: select가 포함된 sql text를 분석한 데이터 프레임
        :param etc_df: select가 포함되지 않은 sql text를 분석한 데이터 프레임
        :param upt_func: 분석한 결과 데이터 프레임을 update 하는 함수 (Background 실행 : Max thread = 1)
        :return:
        """

        sel_df = self.sel_worker.match(sel_df, 'sql_text')
        etc_df = self.etc_worker.match(etc_df, 'sql_text')

        result_df = pd.concat([sel_df, etc_df])

        if upt_func is not None:
            self._wait_end_of_threads()

            bgt = BackgroundTask(self.logger, upt_func, df=result_df, target=target)
            bgt.start()

            self.chd_threads.append(bgt)

        self.logger.info(f"select drain match processed line_count {self.sel_worker.line_count}")
        self.logger.info(f"etc drain match processed line_count {self.etc_worker.line_count}")
