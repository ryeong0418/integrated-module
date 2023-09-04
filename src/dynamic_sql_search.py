import itertools
import numpy as np
import pandas as pd
import faulthandler
import os

from pathlib import Path
from datetime import datetime, timedelta
from sql_metadata.parser import Parser
from pathos import multiprocessing

from src import common_module as cm
from src.common.timelogger import TimeLogger
from src.common.constants import SystemConstants
from src.common.file_export import ParquetFile
from src.common.utils import SystemUtils, MaxGaugeUtils, SqlUtils, DateUtils


class DynamicSourceQuery:
    """
    DynamicSourceQuery Class

    Dynamic sql text 찾기 위한 source query
    """

    def __init__(self, sql_uid="temp"):
        self.select_col_list = []
        self.table_name_list = []
        self.where_col_list = []
        self.where_col_dict = {}
        self.origin_where_sql = None
        self.sql_uid = sql_uid
        self.origin_sql_text = None

    def compare_select_col_list(self, col_list):
        """
        select 컬럼 비교 함수
        :param col_list: 비교 대상 컬럼 list
        :return: 일치 flag
        """
        if len(col_list) == len(self.select_col_list):
            if set(col_list) == set(self.select_col_list):
                return True

        return False

    def compare_table_name(self, table_list):
        """
        table명 비교 함수
        :param table_list: 비교 대상 table list
        :return: 일치 flag
        """
        if len(table_list) == len(self.table_name_list):
            if set(table_list) == set(self.table_name_list):
                return True

        return False


class DynamicSqlSearch(cm.CommonModule):
    """
    DynamicSqlSearch Class

    Dynamic sql text 분석 및 찾기 위한 Class
    """

    def __init__(self, logger):
        super().__init__(logger)
        self.export_parquet_root_path = None
        self.chunk_size = 0
        self.dynamic_source_sql_list = []
        self.n_cores = 0

    def main_process(self):
        """
        Main process 함수
        """
        self.logger.info("DynamicSqlSearch main_process")
        faulthandler.enable()
        self.n_cores = int(os.cpu_count() * 0.5)

        self.chunk_size = self.config.get("data_handling_chunksize", 10000)
        self.export_parquet_root_path = f'{self.config["home"]}/{SystemConstants.EXPORT_PARQUET_PATH}'

        # self._export_db_sql_text()

        # self.set_dynamic_source_query()

        self.search_dynamic_sql_text()

    def search_dynamic_sql_text(self):
        """
        dynamic sql 실제 찾는 함수
        """
        export_filename_suffixs = self._get_export_filename_suffix()
        export_filenames = []
        export_filenames.extend(
            SystemUtils.get_filenames_from_path(self.export_parquet_root_path, prefix=suffix)
            for suffix in export_filename_suffixs
        )
        export_filenames = list(itertools.chain(*export_filenames))

        for filename in export_filenames:
            pd.read_parquet(f"{self.export_parquet_root_path}/{filename}")

            # with TimeLogger("Timelogger", self.logger):
            #     ae_db_sql_df["sql_text"] = ae_db_sql_df["sql_text"].str.lower().str.strip()
            #     ae_db_sql_df = ae_db_sql_df[~ae_db_sql_df["sql_text"].str.startswith(tuple(sql_filter))]
            #
            #     ae_db_sql_df = self.parallelize_dataframe(ae_db_sql_df, self.sql_metadata_parsing_by_df)

            self.logger.info("11")

    def set_dynamic_source_query(self):
        """
        dynamic source query 추출 함수
        """
        home_parent_path = Path(self.config["home"]).parent
        dynamic_sql_path = os.path.join(home_parent_path, SystemConstants.DYNAMIC_SQL_TEXT_PATH)

        if not os.path.isdir(dynamic_sql_path):
            os.mkdir(dynamic_sql_path)

        self.logger.debug(f"dynamic sql child path exist {bool(os.listdir(dynamic_sql_path))}")

        dynamic_sql_list = os.listdir(dynamic_sql_path)

        self.logger.info(f"dynamic sql path {len(dynamic_sql_list)} things exist")

        self._set_dynamic_sql_text_in_txt(dynamic_sql_path, dynamic_sql_list)

    def _set_dynamic_sql_text_in_txt(self, dynamic_sql_path, dynamic_sql_list):
        """
        txt 파일에서 dynamic sql 추출하여 object 생성 함수
        :param dynamic_sql_path: dynamic sql txt 파일 path
        :param dynamic_sql_list: dynamic sql sql_uid로 폴더링된 list
        """
        for sql_uid in dynamic_sql_list:
            self.logger.info(sql_uid)

            dynamic_sql_files = SystemUtils.get_filenames_from_path(
                os.path.join(dynamic_sql_path, sql_uid), suffix=".sql"
            )

            if len(dynamic_sql_files) > 1:
                self.logger.warn(f"[{sql_uid}] dynamic sql file {len(dynamic_sql_files)} exist. Check file in path")

            dynamic_sql_file = dynamic_sql_files[0]

            sql_text = SqlUtils.get_sql_text_in_file(os.path.join(dynamic_sql_path, sql_uid, dynamic_sql_file))
            self._set_dynamic_source_sql_object(sql_uid, sql_text)

    def _set_dynamic_source_sql_object(self, sql_uid, sql_text):
        """
        dynamic source sql 객체 생성 및 값 세팅 함수
        :param sql_uid: 분석하는 sql text의 sql_uid
        :param sql_text: 분석하는 sql text
        """
        sql_df = pd.DataFrame(data=[[sql_uid, sql_text]], columns=["sql_uid", "sql_text"])
        sql_df = self._preprocessing(sql_df)

        sql_df = self.sql_metadata_parsing_by_df(sql_df)

        dsq = DynamicSourceQuery(sql_uid)
        dsq.select_col_list = sql_df.loc[0, "sel_cols"].get("select", [])
        dsq.table_name_list = sql_df.loc[0, "tables"]
        dsq.where_col_list = sql_df.loc[0, "sel_cols"].get("where", [])
        dsq.origin_sql_text = sql_text
        self.dynamic_source_sql_list.append(dsq)

    @staticmethod
    def parallelize_dataframe(df, func, n_cores=4):
        """
        데이터 프레임 병렬 처리 함수
        :param df: 병렬 처리하려는 데이터 프레임
        :param func: 데이터 프레임에 apply func
        :param n_cores: 병렬처리하려는 cpu core 갯수
        :return: 병합된 결과 데이터 프레임
        """
        df_split = np.array_split(df, n_cores)
        pool = multiprocessing.Pool(n_cores)
        result_list = pool.map(func, df_split)
        if not result_list:
            return pd.DataFrame()

        df = pd.concat(result_list)
        pool.close()
        pool.join()
        return df

    # def sqlglot_parsing_by_df(self, df):
    #     df["parse_sql"] = df["sql_text"].apply(lambda x: self.sqlglot_parsing(x))
    #     return df
    #
    # @staticmethod
    # def sqlglot_parsing(sql_text):
    #     try:
    #         return parse_one(sql_text, read="oracle", dialect="oracle", max_errors=100, error_level=ErrorLevel.WARN)
    #     except:
    #         return None

    def sql_metadata_parsing_by_df(self, df):
        """
        sql_metadata lib를 사용한 sql parsing
        :param df: 파싱 전 데이터 프레임
        :return: 결과 데이터 프레임
        """
        df["sel_cols"] = df["sql_text"].apply(lambda x: self.parsing_columns(x))
        df["tables"] = df["sql_text"].apply(lambda x: self.parsing_tables(x))
        return df

    @staticmethod
    def parsing_columns(sql_text):
        """
        컬럼 파싱 함수
        :param sql_text: 파싱 대상 sql text
        :return: 파싱 결과
        """
        try:
            return Parser(sql_text).columns_dict if Parser(sql_text).columns_dict is not None else {}
        except Exception:
            return {}

    @staticmethod
    def parsing_tables(sql_text):
        """
        테이블명 파싱 함수
        :param sql_text: 파싱 대상 sql text
        :return: 파싱 결과 (위에 로직과 함쳐서 한개로..)
        """
        try:
            return Parser(sql_text).tables if Parser(sql_text).tables is not None else []
        except Exception:
            return []

    def _export_db_sql_text(self):
        """
        db sql text 재조합 및 parquet 파일 추출 함수
        """
        pf = ParquetFile(self.logger, self.config)

        self._init_sa_target()

        ae_db_info_df = self.st.get_ae_db_info()
        ae_db_infos = ae_db_info_df["lpad_db_id"].to_list()

        date_conditions = DateUtils.set_date_conditions_by_interval(
            self.config["args"]["s_date"], self.config["args"]["interval"], return_fmt="%y%m%d"
        )

        for date in date_conditions:
            for db_id in ae_db_infos:
                total_row_cnt = 0
                partition_key = f"{date}{db_id}"

                parquet_file_name = (
                    f"{SystemConstants.DB_SQL_TEXT_FOR_DYNAMIC_FILE_NAME}"
                    f"_{partition_key}{SystemConstants.PARQUET_FILE_EXT}"
                )
                file_name = f"{self.export_parquet_root_path}/{parquet_file_name}"

                if os.path.isfile(file_name):
                    continue

                # pf.remove_parquet(self.export_parquet_root_path, parquet_file_name)
                pq_writer = None

                with TimeLogger(f"Make_parquet_file_ae_db_sql_text (date:{date}, db_id:{db_id}),elapsed ", self.logger):
                    for df in self.st.get_ae_db_sql_text_by_1seq(partition_key, chunksize=self.chunk_size):
                        if len(df) == 0:
                            break

                        results = self.st.get_all_ae_db_sql_text_by_1seq(df, chunksize=self.chunk_size)

                        grouping_df = MaxGaugeUtils.reconstruct_by_grouping(results)
                        grouping_df = self._preprocessing(grouping_df)

                        self._teardown_sa_target()

                        grouping_df = self.parallelize_dataframe(
                            grouping_df, self.sql_metadata_parsing_by_df, n_cores=self.n_cores
                        )
                        grouping_df = grouping_df.astype({"sel_cols": "str", "tables": "str"})

                        self._init_sa_target()

                        if pq_writer is None:
                            pq_writer = pf.get_pqwriter(self.export_parquet_root_path, parquet_file_name, grouping_df)

                        pf.make_parquet_by_df(grouping_df, pq_writer)
                        total_row_cnt += len(grouping_df)

                    self.logger.info(f"Total export data count (date:{date}, db_id:{db_id}): {total_row_cnt} rows")

                if pq_writer:
                    pq_writer.close()

    @staticmethod
    def _preprocessing(grouping_df):
        """
        전처리 함수
        :param grouping_df: grouping된 db sql text 데이터 프레임
        :return: 필터링, 전처리된 데이터 프레임임
        """
        sql_filter = [
            "begin",
            "declare",
            "merge",
            "insert",
            "update",
            "delete",
            "call",
            "set",
            "with",
            "grant",
            "analyze",
            "alter",
            "lock",
            "create",
            "explain",
            "truncate",
        ]

        grouping_df["sql_text"] = grouping_df["sql_text"].str.lower().str.strip()
        grouping_df = grouping_df[~grouping_df["sql_text"].str.startswith(tuple(sql_filter))]
        return grouping_df

    def _get_export_filename_suffix(self):
        """
        db sql text export 파일 날짜 접미사 추출 함수
        :return: 날짜 list
        """
        prefix = []

        for i in range(0, int(self.config["args"]["interval"])):
            from_date = datetime.strptime(str(self.config["args"]["s_date"]), "%Y%m%d")
            date_condition = from_date + timedelta(days=i)
            date_condition = date_condition.strftime("%y%m%d")

            parquet_file_name_suffix = f"{SystemConstants.DB_SQL_TEXT_FOR_DYNAMIC_FILE_NAME}_{date_condition}"

            prefix.append(parquet_file_name_suffix)

        return prefix
