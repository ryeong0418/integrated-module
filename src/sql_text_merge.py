import os

import numpy as np
import pandas as pd
import sqlparse
import re

from pathos.multiprocessing import Pool

from src import common_module as cm
from src.common.timelogger import TimeLogger
from src.common.constants import SystemConstants
from src.common.file_export import ParquetFile
from src.analysis_target import InterMaxTarget, SaTarget


class SqlTextMerge(cm.CommonModule):

    def __init__(self, logger):
        self.logger = logger
        self.st = None
        self.imt = None
        self.mgt = None

    def parallelize(self, data, func, num_of_processes=4):
        data_split = np.array_split(data, num_of_processes)
        pool = Pool(num_of_processes)
        data = pd.concat(pool.map(func, data_split))
        pool.close()
        pool.join()
        return data

    def main_process(self):
        self.logger.debug("SqlTextMerge main")

        if not self.config['intermax_repo']['use'] and not self.config['maxgauge_repo']['use']:
            self.logger.error(f"intermax_repo or maxgauge_repo use false.. please check config")
            return

        self.st = SaTarget(self.logger, self.config)
        self.st.init_process()

        chunksize = 50
        export_parquet_root_path = f'{self.config["home"]}/{SystemConstants.EXPORT_PARQUET_PATH}'

        parquet_file_name = f"{SystemConstants.DB_SQL_TEXT_FILE_NAME}" \
                            f"_" \
                            f"{self.config['args']['s_date']}" \
                            f"{SystemConstants.DB_SQL_TEXT_FILE_SUFFIX}"

        if self.config['args']['sub_proc'] == 'export':
            pf = ParquetFile(self.logger, self.config)
            pf.remove_parquet(export_parquet_root_path, parquet_file_name)

            with TimeLogger(f"Make_parquet_file_ae_db_sql_text : ", self.logger):
                for df in self.st.get_ae_db_sql_text_1seq(chunksize=chunksize):
                    results = self.st.get_ae_db_sql_text_by_1seq(df, chunksize=chunksize)

                    grouping_df = self._reconstruct_by_grouping(results)
                    grouping_df = self._preprocessing(grouping_df)

                    pf.make_parquet_by_df(grouping_df, export_parquet_root_path, parquet_file_name)

        else:
            # self.st.drop_table_for_sql_text_merge()

            # self.imt = InterMaxTarget(self.logger, self.config)
            # self.imt.init_process()
            # xapm_sql_df = self.imt.get_xapm_sql_text()

            filenames = [file for file in os.listdir(path=export_parquet_root_path)]

            for xapm_sql_df in self.st.get_ae_was_sql_text(chunksize=chunksize):
                p_was_df = self._preprocessing(xapm_sql_df)

                compare_col_list = ['len', 'first_t', 'last_t']
                for _, was_df in p_was_df.iterrows():
                    result_df = pd.DataFrame()
                    len_result_df = pd.DataFrame()
                    for filename in filenames:
                        p_ae_sql_df = pd.read_parquet(f'{export_parquet_root_path}/{filename}')
                        len_ae_sql_df = p_ae_sql_df[(p_ae_sql_df['total_len'] == was_df['total_len'])]

                        p_ae_sql_df = p_ae_sql_df[(p_ae_sql_df['total_len'] == was_df['total_len']) &
                                                  (p_ae_sql_df['last_token_len'] == was_df['last_token_len']) &
                                                  (p_ae_sql_df['first_token'] == was_df['first_token']) &
                                                  (p_ae_sql_df['last_token'] == was_df['last_token'])
                        ]

                        # merge_df = was_df.merge(p_ae_sql_df, how='inner', on=compare_col_list)
                        result_df = result_df.append(p_ae_sql_df)
                        len_result_df = len_result_df.append(len_ae_sql_df)
                        self.logger.info("end")

                    self.logger.info(result_df.head())

                self.logger.info("stop")

        self.logger.debug("SqlTextMerge End")

    def _reconstruct_by_grouping(self, results):
        results_df = pd.DataFrame(results, columns=['sql_text', 'partition_key', 'sql_uid', 'seq'])
        results_df = results_df.groupby(['sql_uid', 'partition_key'], as_index=False).agg({'sql_text': ''.join})
        return results_df

    def _preprocessing(self, xapm_sql_df):
        self.logger.info(f"_preprocessing start {os.getpid()}")
        # 메모리 문제로 타겟 컬럼과 도착지 컬럼을 같게(None) 줘야할수도 있을듯
        xapm_sql_df = self._remove_unnecess_char(xapm_sql_df, 'sql_text')
        # xapm_sql_df = self._set_common_sql_format(xapm_sql_df, 'sql_text')
        xapm_sql_df = self._parse_sql_text(xapm_sql_df, 'sql_text', 'p_sql_text')
        xapm_sql_df = self._set_token_compare_info(xapm_sql_df, 'p_sql_text')
        return xapm_sql_df

    def _remove_unnecess_char(self, df, target_c: str, dest_c: str = None):
        dest_c = target_c if dest_c is None else dest_c

        repls = {r'\\t': '', r'\\n': '', r'\\r': '', '\t': '', '\n': '', '\r': ''}
        rep = dict((re.escape(k), v) for k, v in repls.items())
        pattern = re.compile("|".join(rep.keys()))

        df[dest_c] = df[target_c].apply(
            lambda x: pattern.sub(lambda m: rep[re.escape(m.group(0))], x)
        )
        return df

    def _parse_sql_text(self, df, target_c: str, dest_c: str = None):
        dest_c = target_c if dest_c is None else dest_c

        df[dest_c] = df[target_c].apply(
            lambda x: sqlparse.parse(
                x
                # sqlparse.format(x, keyword_case='upper', identifier_case='upper', strip_comments=True)
            )
        )

        return df

    def _set_token_compare_info(self, df, target_c):
        df['total_len'] = df[target_c].apply(
            lambda x: len(x[0].tokens)
        )
        df['first_token'] = df[target_c].apply(
            lambda x: str(x[0].tokens[0])
        )
        df['last_token_len'] = df[target_c].apply(
            lambda x: len(x[0].tokens[-1].tokens) if getattr(x[0].tokens[-1], 'tokens', False) else 1
        )
        df['last_token'] = df[target_c].apply(
            lambda x: str(x[0].tokens[-1])
        )
        # df['sql_text'] = df[target_c].apply(
        #     self._tokens_flatten
        # )
        return df

    def _tokens_flatten(self, x):
        return tuple(str(t) for t in x[0].tokens if str(t).strip() != '')

    def _merged_df_filter(self, merged_df):
        merged_df.rename(columns={'sql_id': 'was_sql_id'}, inplace=True)
        merged_df.rename(columns={'sql_uid': 'db_sql_uid'}, inplace=True)
        merged_df.drop(["sql_text"], axis=1, inplace=True)
        return merged_df
