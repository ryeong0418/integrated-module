import itertools
import numpy as np
import pandas as pd

from datetime import datetime, timedelta

from src import common_module as cm
from src.common.timelogger import TimeLogger
from src.common.constants import SystemConstants
from src.common.file_export import ParquetFile
from src.common.utils import SystemUtils, MaxGaugeUtils, SqlUtils
from src.common.module_exception import ModuleException
from src.common.enum_module import MessageEnum


class SqlTextMerge(cm.CommonModule):
    """
    SqlTextMerge Class

    Was sql text와 DB sql text를 전체 비교 하기 위한 Class.
    """

    def __init__(self, logger):
        super().__init__(logger)
        self.chunk_size = 0
        self.export_parquet_root_path = None
        # Sql Text 전처리 및 매치 모드 분리 (str, token)
        self.match_mode = 'token'
        self.sql_match_sensitive = 5

    def main_process(self):
        if not self.config['intermax_repo']['use'] and not self.config['maxgauge_repo']['use']:
            error_code = 'E007'
            self.logger.error(MessageEnum[error_code].value)
            raise ModuleException(error_code)

        self._init_sa_target()

        self.export_parquet_root_path = f'{self.config["home"]}/{SystemConstants.EXPORT_PARQUET_PATH}'
        self.chunk_size = self.config.get('data_handling_chunksize', 10000) * 3
        self.sql_match_sensitive = int(self._calc_sql_match_sensitive())

        self._export_db_sql_text()
        self._sql_text_merge()

    def _calc_sql_match_sensitive(self):
        """
        sql text match시 사용되는 token 갯수를 조정하기 위한 값을 구하는 함수
        :return: sql_match_sensitive 값
        """
        sample_df = self.st.get_ae_was_sql_text(extract_cnt=200_000)

        sample_df = SqlUtils.remove_unnecess_char(sample_df, 'sql_text', contains_comma=True)
        token_cnt = sample_df['sql_text'].str.count(r"\s+")

        m = np.mean(token_cnt)
        result = m // 50

        return result if result > 0 else 1

    def _sql_text_merge(self):
        """
        was sql text - db sql text match 함수
        """
        self._init_im_target()

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

        if self.match_mode == 'token':
            default_merge_column = default_merge_column + first_token_merge_column + last_token_merge_column

        s_date, e_date = SystemUtils.get_each_date_by_interval(
            self.config['args']['s_date'], self.config['args']['interval']
        )

        for ae_was_df in self.st.get_ae_was_sql_text_by_term(s_date, e_date, chunksize=self.chunk_size):
            result_df_list = []

            ae_was_df = self._preprocessing(ae_was_df)
            ae_was_df['sql_text'].apply(np.array)

            for filename in export_filenames:
                ae_db_sql_df = pd.read_parquet(f'{self.export_parquet_root_path}/{filename}')

                merge_df = pd.merge(ae_was_df, ae_db_sql_df, on=default_merge_column, how='inner')
                if len(merge_df) == 0:
                    continue

                if self.match_mode == 'str':
                    merge_df['compare'] = np.where(merge_df['sql_text_x'] == merge_df['sql_text_y'], True, False)
                elif self.match_mode == 'token':
                    merge_df['compare'] = [
                        np.array_equal(was_sql, db_sql)
                        for was_sql, db_sql in zip(merge_df['sql_text_x'], merge_df['sql_text_y'])
                    ]

                merge_df = merge_df[merge_df['compare']]
                result_df_list.append(merge_df[insert_result_columns])

            self.logger.info('End of all export file compare')

            result_df = pd.concat(result_df_list, ignore_index=True) if len(result_df_list) > 0 else pd.DataFrame()
            result_df = result_df.drop_duplicates(subset=['sql_id', 'sql_uid'])

            if len(result_df) == 0:
                continue
            else:
                total_match_len += len(result_df)
                self.logger.info(f"Matching Data Length : {len(result_df)}")

            self._insert_merged_result(result_df)

        self.logger.info(f"Final Total match count {total_match_len}")

    def _export_db_sql_text(self):
        """
        db sql text 재조합 및 parquet 파일 추출 함수
        """
        pf = ParquetFile(self.logger, self.config)

        ae_db_info_df = self.st.get_ae_db_info()
        ae_db_infos = ae_db_info_df['lpad_db_id'].to_list()

        date_conditions = MaxGaugeUtils.set_maxgauge_date(
            self.config['args']['s_date'], self.config['args']['interval']
        )

        for date in date_conditions:
            for db_id in ae_db_infos:
                total_row_cnt = 0
                partition_key = f"{date}{db_id}"

                parquet_file_name = f"{SystemConstants.DB_SQL_TEXT_FILE_NAME}" \
                                    f"_{partition_key}{SystemConstants.PARQUET_FILE_EXT}"

                pf.remove_parquet(self.export_parquet_root_path, parquet_file_name)
                pq_writer = None

                with TimeLogger(f"Make_parquet_file_ae_db_sql_text (date:{date}, db_id:{db_id}),elapsed ", self.logger):
                    for df in self.st.get_ae_db_sql_text_by_1seq(partition_key, chunksize=self.chunk_size):
                        if len(df) == 0:
                            break

                        results = self.st.get_all_ae_db_sql_text_by_1seq(df, chunksize=self.chunk_size)

                        grouping_df = MaxGaugeUtils.reconstruct_by_grouping(results)
                        grouping_df = self._preprocessing(grouping_df)

                        if pq_writer is None:
                            pq_writer = pf.get_pqwriter(self.export_parquet_root_path, parquet_file_name, grouping_df)

                        pf.make_parquet_by_df(grouping_df, pq_writer)
                        total_row_cnt += len(grouping_df)

                    self.logger.info(f"Total export data count (date:{date}, db_id:{db_id}): {total_row_cnt} rows")

                if pq_writer:
                    pq_writer.close()

    def _insert_merged_result(self, result_df):
        """
        Was sql text와 DB sql text가 일치한 데이터를 저장하기 위한 함수
        :param result_df: 일치한 데이터 데이터 프레임
        """
        result_df.rename(columns={'sql_id': 'was_sql_id'}, inplace=True)
        result_df.rename(columns={'sql_uid': 'db_sql_uid'}, inplace=True)
        result_df['state_code'] = 0

        self.st.insert_ae_sql_text_by_merged_df(result_df)

    def _preprocessing(self, xapm_sql_df):
        """
        sql text 데이터 프레임 전처리 함수
        :param xapm_sql_df: sql text를 포함한 데이터 프레임
        :return: 전처리 된 sql text 데이터 프레임
        """
        xapm_sql_df = SqlUtils.remove_unnecess_char(xapm_sql_df, 'sql_text', contains_comma=True)
        xapm_sql_df = self._split_parse_sql_text(xapm_sql_df, 'sql_text', self.match_mode, self.sql_match_sensitive)
        return xapm_sql_df

    @staticmethod
    def _split_parse_sql_text(xapm_sql_df: pd.DataFrame, target_c: str, match_mode: str, sql_match_sensitive):
        """
        match_mode별 sql text match를 위한 전처리
        :param xapm_sql_df: 대상 데이터 프레임
        :param target_c: 타겟 컬럼
        :param match_mode: match_mode (str : sql_text 공백 split str,
                                    token : sql_text split 첫번째 토큰/마지막 토큰/전체길이/-sql_match_sensitive 길이 토큰 추출)
        :param sql_match_sensitive: sql_match의 merge에 사용할 컬럼의 갯수
        :return: 전처리된 데이터 프레임
        """
        xapm_sql_df[target_c] = xapm_sql_df[target_c].str.lower().str.split(r"\s+")

        if match_mode == 'token':
            xapm_sql_df['first_token'] = xapm_sql_df[target_c].apply(lambda x: x[0])

            for i in range(1, sql_match_sensitive * 50 + 1):
                xapm_sql_df[f'last_token{i}'] = xapm_sql_df[target_c].apply(lambda x: x[-i] if len(x) > i+1 else None)

        elif match_mode == 'str':
            xapm_sql_df[target_c] = xapm_sql_df[target_c].apply(str)

        xapm_sql_df['sql_text_len'] = xapm_sql_df[target_c].apply(len).astype(np.int32)

        return xapm_sql_df

    def _get_export_filename_suffix(self):
        """
        db sql text export 파일 날짜 접미사 추출 함수
        :return: 날짜 list
        """
        prefix = []

        for i in range(0, int(self.config['args']['interval'])):
            from_date = datetime.strptime(str(self.config['args']['s_date']), '%Y%m%d')
            date_condition = from_date + timedelta(days=i)
            date_condition = date_condition.strftime('%y%m%d')

            parquet_file_name_suffix = f"{SystemConstants.DB_SQL_TEXT_FILE_NAME}_{date_condition}"

            prefix.append(parquet_file_name_suffix)

        return prefix
