import re
import time
import pandas as pd
import inspect

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.file_persistence import FilePersistence

from src import common_module as cm
from src.common.module_exception import ModuleException
from src.common.enum_module import MessageEnum
from resources.logger_manager import Logger
from src.common.background_task import BackgroundTask
from src.common.timelogger import TimeLogger
from src.common.constants import SystemConstants


class SqlTextTemplate(cm.CommonModule):

    def __init__(self, logger):
        self.logger = logger
        self.sql_text_template_logger = None
        self.drain_config = None
        self.persistence = None
        self.template_miner = None
        self.chunk_size = 0
        self.sql_template_select_only = False
        self.line_count = 0
        self.chd_threads = []
        self.wait_times_cnt = 0
        self.sql_text_template_tree = None

    def main_process(self):
        if not self.config['intermax_repo']['use'] and not self.config['maxgauge_repo']['use']:
            error_code = 'E004'
            self.logger.error(MessageEnum[error_code].value)
            raise ModuleException(error_code)

        self.chunk_size = self.config.get('data_handling_chunksize', 10_000) * 10
        self.sql_template_select_only = self.config.get('sql_template_select_only', False)

        self._add_sql_text_template_logger()

        self._init_sa_target()

        self._init_drain()

        self._was_sql_text_template()

        self._wait_end_of_threads()

        self._update_unanalyzed_was_sql_text()

        self._save_top_cluster_template(extract_cnt=1_000)

        self._print_drain_tree()

    def _init_drain(self):
        self.drain_config = TemplateMinerConfig()
        self.drain_config.load(f"{self.config['home']}"
                               f"{SystemConstants.DRAIN_CONF_PATH}"
                               f"{SystemConstants.DRAIN_INI_FILE_NAME}")
        self.persistence = FilePersistence(f"{self.config['home']}"
                                           f"{SystemConstants.DRAIN_CONF_PATH}"
                                           f"{SystemConstants.DRAIN_MODEL_FILE_NAME}")

        self.sql_text_template_tree = f"{self.config['home']}" \
                                      f"{SystemConstants.DRAIN_CONF_PATH}" \
                                      f"{SystemConstants.DRAIN_TEMPLATE_TREE_FILE_NAME}"

        self.template_miner = TemplateMiner(self.persistence, self.drain_config)

        self.logger.info(f"{len(self.drain_config.masking_instructions)} masking instructions are in use")

    def _wait_end_of_threads(self):
        self.logger.info(f"Total child thread count {len(self.chd_threads)}")

        while any([t.is_alive() for t in self.chd_threads]):
            self.wait_times_cnt += 1
            self.logger.info(f"Child thread waiting one seconds.. {self.wait_times_cnt} times")
            time.sleep(1)

    def _add_sql_text_template_logger(self):
        self.sql_text_template_logger = Logger(self.config['env']).\
            get_default_logger(self.config['log_dir'], SystemConstants.SQL_TEXT_TEMPLATE_LOG_FILE_NAME)

    def _was_sql_text_template(self):
        start_time = time.time()

        for df in self.st.get_ae_was_sql_text_by_no_cluster(chunk_size=self.chunk_size):

            with TimeLogger(inspect.currentframe().f_code.co_name, self.sql_text_template_logger):
                df = self._preprocessing(df)

            df['cluster_id'] = '0'

            for idx, row in df.iterrows():
                cluster = self.template_miner.match(row['sql_text'])
                self.line_count += 1

                if cluster is None:
                    self.sql_text_template_logger.info(f"No Match found")
                    result = self.template_miner.add_log_message(row['sql_text'])
                    cluster_id = result['cluster_id']

                    self.sql_text_template_logger.info(f"Create template id # {cluster_id}")
                else:
                    # template = cluster.get_template()
                    self.sql_text_template_logger.info(f"Matched template # {cluster.cluster_id}")
                    cluster_id = cluster.cluster_id

                df.loc[idx, 'cluster_id'] = str(cluster_id)

            bgt = BackgroundTask(self.logger, self.st.update_cluster_id_by_sql_id, df=df)
            bgt.start()

            self.chd_threads.append(bgt)

            self.sql_text_template_logger.info(f"template predict line_count {self.line_count}")

        time_took = time.time() - start_time
        rate = self.line_count / time_took
        self.logger.info(f"--- Done processing file in {time_took:.2f} sec. "
                         f"Total of {self.line_count} lines, rate {rate:.1f} lines/sec")

    def _preprocessing(self, df):
        filter_list = ['select']

        if not self.sql_template_select_only:
            filter_extend_list = ['insert', 'update', 'delete']
            filter_list.extend(filter_extend_list)

        df = df[df['sql_text'].str.contains('|'.join(filter_list), na=False, case=False)]
        df = df[~df['sql_text'].str.contains('sql is too big', na=False, case=False)]

        df = SqlTextTemplate._remove_unnecess_char(df, 'sql_text')
        df['sql_text'] = df['sql_text'].str.lower().str.replace(r'\s+', ' ', regex=True)

        df = SqlTextTemplate._rex_processing(df)

        return df

    @staticmethod
    def _rex_processing(df):
        df['sql_text'] = df['sql_text'].str.replace(r'\s+in\s?\([^)]*\)', ' in(<:args:>)', regex=True)
        df['sql_text'] = df['sql_text'].str.replace(r'\s+values\s?\([^)]*\)', ' values(<:args:>)', regex=True)
        df['sql_text'] = df['sql_text'].str.replace(r"\d{2,4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}\.?\d{0,3}", '<DATETIME>',
                                                    regex=True)
        df['sql_text'] = df['sql_text'].str.replace(r"\d{2,4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.?\d{0,3}", '<DATETIME>',
                                                    regex=True)
        df['sql_text'] = df['sql_text'].str.replace(r"\d{4}\-\d{2}\-\d{2}", '<DATE>', regex=True)
        return df

    @staticmethod
    def _remove_unnecess_char(df, target_c: str):
        """
        정규식을 이용한 /t, /n, /r, , 치환 함수
        :param df: 원본 데이터프레임
        :param target_c: 대상 타겟 컬럼
        :return: 치환된 데이터프레임
        """
        repls = {r'\\t': ' ', r'\\n': ' ', r'\\r': ' ', '\t': ' ', '\n': ' ', '\r': ' ', ',': ' '}
        rep = dict((re.escape(k), v) for k, v in repls.items())
        pattern = re.compile("|".join(rep.keys()))

        df[target_c] = df[target_c].apply(
            lambda x: pattern.sub(lambda m: rep[re.escape(m.group(0))], x)
        )
        return df

    def _print_drain_tree(self):
        self.template_miner.drain.print_tree(file=open(self.sql_text_template_tree, 'w', encoding="utf-8"))

        sorted_clusters = sorted(self.template_miner.drain.clusters, key=lambda it: it.size, reverse=True)

        for cluster in sorted_clusters:
            self.sql_text_template_logger.info(cluster)

    def _save_top_cluster_template(self, extract_cnt=0):
        cluster_list = []

        for cluster in self.template_miner.drain.clusters:
            cluster_list.append((str(cluster.cluster_id), ' '.join(cluster.log_template_tokens)))

        cluster_df = pd.DataFrame(cluster_list, columns=['cluster_id', 'sql_template'])
        cluster_df['cluster_cnt'] = 0

        return_df = self.st.get_ae_was_sql_text_cluster_cnt_by_grouping(extract_cnt)

        merged_df = pd.merge(cluster_df, return_df, on=['cluster_id'],)

        merged_df.rename(columns={'cluster_cnt_y': 'cluster_cnt'}, inplace=True)
        merged_df.sort_values('cluster_cnt', ascending=False, inplace=True)
        merged_df.drop('cluster_cnt_x', axis=1, inplace=True)

        self.st.insert_ae_sql_template(merged_df)

    def _update_unanalyzed_was_sql_text(self):
        self.st.update_unanalyzed_was_sql_text()
