import itertools
import numpy as np
import pandas as pd
import re

from datetime import datetime, timedelta

from src import common_module as cm
from src.common.timelogger import TimeLogger
from src.common.constants import SystemConstants
from src.common.file_export import ParquetFile
from src.common.utils import SystemUtils
from src.analysis_target import InterMaxTarget, SaTarget


class SqlTextMerge(cm.CommonModule):

    def __init__(self, logger):
        self.logger = logger
        self.st = None
        self.imt = None
        self.mgt = None
        self.CHUNKSIZE = 10000
        self.export_parquet_root_path = None
        # Sql Text 전처리 및 매치 모드 분리 (str, token)
        self.MATCH_MODE = 'token'
        self.sql_match_sensitive = 5

    # def parallelize(self, data, func, num_of_processes=4):
    #     data_split = np.array_split(data, num_of_processes)
    #     pool = Pool(num_of_processes)
    #     data = pd.concat(pool.map(func, data_split))
    #     pool.close()
    #     pool.join()
    #     return data

    def main_process(self):
        if not self.config['intermax_repo']['use'] and not self.config['maxgauge_repo']['use']:
            self.logger.error(f"intermax_repo or maxgauge_repo use false.. please check config")
            return

        if self.MATCH_MODE != 'str' and self.MATCH_MODE != 'token':
            self.logger.error(f"MATCH_MODE invalid value.. please check sql_text_merge.py near Line 28 ")
            return

        self.st = SaTarget(self.logger, self.config)
        self.st.init_process()

        self.export_parquet_root_path = f'{self.config["home"]}/{SystemConstants.EXPORT_PARQUET_PATH}'
        self.CHUNKSIZE = self.config.get('sql_merge_text_chunksize', 10000)
        self.sql_match_sensitive = self.config.get('sql_match_sensitive', 5)

        self._export_db_sql_text()

        self._sql_text_merge()

    def _sql_text_merge(self):
        # self.st.drop_table_for_sql_text_merge()

        self.imt = InterMaxTarget(self.logger, self.config)
        self.imt.init_process()
        self.imt.create_im_engine()

        export_filename_suffixs = self._get_export_filename_suffix()
        export_filenames = []
        export_filenames.extend(SystemUtils.get_filenames_from_path(self.export_parquet_root_path, prefix=suffix)
                                for suffix in export_filename_suffixs)
        export_filenames = list(itertools.chain(*export_filenames))

        insert_result_columns = ['sql_id', 'sql_uid', 'sql_text_100', ]
        default_merge_column = ['sql_text_len']
        first_token_merge_column = ['first_token']
        last_token_merge_column = [f"last_token{i}" for i in range(1, self.sql_match_sensitive * 50 + 1)]

        total_match_len = 0

        if self.MATCH_MODE == 'token':
            default_merge_column = default_merge_column + first_token_merge_column + last_token_merge_column

        # InterMax DB 바라보게 추후 바꿔야함
        # for ae_was_df in self.st.get_ae_was_sql_text(chunksize=self.CHUNKSIZE):
        for ae_was_df in self.imt.get_xapm_sql_text(chunksize=self.CHUNKSIZE):
            result_df_list = []

            ae_was_df = self._preprocessing(ae_was_df)
            ae_was_df['sql_text'].apply(np.array)

            for filename in export_filenames:
                ae_db_sql_df = pd.read_parquet(f'{self.export_parquet_root_path}/{filename}')

                merge_df = pd.merge(ae_was_df, ae_db_sql_df, on=default_merge_column, how='inner')
                if len(merge_df) == 0:
                    continue

                if self.MATCH_MODE == 'str':
                    merge_df['compare'] = np.where(merge_df['sql_text_x'] == merge_df['sql_text_y'], True, False)
                elif self.MATCH_MODE == 'token':
                    merge_df['compare'] = [True if np.array_equal(was_sql, db_sql) else False
                                           for was_sql, db_sql in zip(merge_df['sql_text_x'], merge_df['sql_text_y'])]

                merge_df = merge_df[merge_df['compare']]
                result_df_list.append(merge_df[insert_result_columns])

            self.logger.info('End of all export file compare')
            result_df = pd.concat(result_df_list, ignore_index=True) if len(result_df_list) > 0 else pd.DataFrame()

            if len(result_df) == 0:
                continue
            else:
                total_match_len += len(result_df)
                self.logger.info(f"Matching Data Length : {len(result_df)}")

            self._insert_merged_result(result_df)

        self.logger.info(f"Final Total match count {total_match_len}")

    def _export_db_sql_text(self):
        pf = ParquetFile(self.logger, self.config)

        ae_db_info_df = self.st.get_ae_db_info()
        ae_db_infos = ae_db_info_df['lpad_db_id'].to_list()

        date_conditions = self.st.get_maxgauge_date_conditions()

        for date in date_conditions:
            for db_id in ae_db_infos:
                total_row_cnt = 0
                partition_key = f"{date}{db_id}"

                parquet_file_name = f"{SystemConstants.DB_SQL_TEXT_FILE_NAME}" \
                                    f"_{partition_key}{SystemConstants.DB_SQL_TEXT_FILE_SUFFIX}"

                pf.remove_parquet(self.export_parquet_root_path, parquet_file_name)
                pqwriter = None

                with TimeLogger(f"Make_parquet_file_ae_db_sql_text (date:{date}, db_id:{db_id}),elapsed ", self.logger):
                    for df in self.st.get_ae_db_sql_text_by_1seq(partition_key, chunksize=self.CHUNKSIZE):
                        if len(df) == 0:
                            break

                        results = self.st.get_all_ae_db_sql_text_by_1seq(df, chunksize=self.CHUNKSIZE)

                        grouping_df = self._reconstruct_by_grouping(results)
                        grouping_df = self._preprocessing(grouping_df)

                        if pqwriter is None:
                            pqwriter = pf.get_pqwriter(self.export_parquet_root_path, parquet_file_name, grouping_df)

                        pf.make_parquet_by_df(grouping_df, pqwriter)
                        total_row_cnt += len(grouping_df)

                    self.logger.info(f"Total export data count (date:{date}, db_id:{db_id}): {total_row_cnt} rows")

                if pqwriter:
                    pqwriter.close()

    def _insert_merged_result(self, result_df):
        result_df.rename(columns={'sql_id': 'was_sql_id'}, inplace=True)
        result_df.rename(columns={'sql_uid': 'db_sql_uid'}, inplace=True)
        result_df['state_code'] = 0

        self.st.insert_merged_result(result_df)

    @staticmethod
    def _reconstruct_by_grouping(results):
        results_df = pd.DataFrame(results, columns=['sql_text', 'partition_key', 'sql_uid', 'seq'])
        results_df = results_df.groupby(['sql_uid', 'partition_key'], as_index=False).agg({'sql_text': ''.join})
        results_df.drop(columns='partition_key', inplace=True)
        return results_df

    def _preprocessing(self, xapm_sql_df):
        # 메모리 문제로 타겟 컬럼과 도착지 컬럼을 같게(None) 줘야할수도 있을듯
        xapm_sql_df = self._remove_unnecess_char(xapm_sql_df, 'sql_text')
        xapm_sql_df = self._split_parse_sql_text(xapm_sql_df, 'sql_text', self.MATCH_MODE, self.sql_match_sensitive)
        return xapm_sql_df

    @staticmethod
    def _split_parse_sql_text(xapm_sql_df: pd.DataFrame, target_c: str, match_mode: str, sql_match_sensitive):
        xapm_sql_df[target_c] = xapm_sql_df[target_c].str.lower().str.split(r"\s+")

        if match_mode == 'token':
            xapm_sql_df['first_token'] = xapm_sql_df[target_c].apply(lambda x: x[0])

            for i in range(1, sql_match_sensitive * 50 + 1):
                xapm_sql_df[f'last_token{i}'] = xapm_sql_df[target_c].apply(lambda x: x[-i] if len(x) > i+1 else None)

        elif match_mode == 'str':
            xapm_sql_df[target_c] = xapm_sql_df[target_c].apply(str)

        xapm_sql_df['sql_text_len'] = xapm_sql_df[target_c].apply(len).astype(np.int32)

        return xapm_sql_df

    @staticmethod
    def _remove_unnecess_char(df, target_c: str, dest_c: str = None):
        dest_c = target_c if dest_c is None else dest_c

        repls = {r'\\t': ' ', r'\\n': ' ', r'\\r': ' ', '\t': ' ', '\n': ' ', '\r': ' '}
        rep = dict((re.escape(k), v) for k, v in repls.items())
        pattern = re.compile("|".join(rep.keys()))

        df[dest_c] = df[target_c].apply(
            lambda x: pattern.sub(lambda m: rep[re.escape(m.group(0))], x)
        )
        return df

    @staticmethod
    def _parse_sql_text(df, target_c: str, dest_c: str = None):
        dest_c = target_c if dest_c is None else dest_c

        df[dest_c] = df[target_c].apply(
            # lambda x: sqlparse.parse(
            #     x
            #     # sqlparse.format(x, keyword_case='upper', identifier_case='upper', strip_comments=True)
            # )
        )

        return df

    @staticmethod
    def _set_token_compare_info(df, target_c):
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
        df['sql_text'] = df[target_c].apply(
            SqlTextMerge._tokens_flatten
        )
        return df

    @staticmethod
    def _tokens_flatten(x):
        return tuple(str(t) for t in x[0].tokens if str(t).strip() != '')

    def _get_export_filename_suffix(self):
        prefix = []

        for i in range(0, int(self.config['args']['interval'])):
            from_date = datetime.strptime(str(self.config['args']['s_date']), '%Y%m%d')
            date_condition = from_date + timedelta(days=i)
            date_condition = date_condition.strftime('%y%m%d')

            parquet_file_name_suffix = f"{SystemConstants.DB_SQL_TEXT_FILE_NAME}_{date_condition}"

            prefix.append(parquet_file_name_suffix)

        return prefix
