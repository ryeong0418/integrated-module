import numpy as np
import pandas as pd
import faulthandler
import os
import pyarrow.parquet as pq
import sqlparse

from ast import literal_eval
from pathlib import Path
from sql_metadata.parser import Parser
from pathos import multiprocessing

from src import common_module as cm
from src.common.timelogger import TimeLogger
from src.common.constants import SystemConstants, TableConstants
from src.common.file_export import ParquetFile
from src.common.utils import SystemUtils, MaxGaugeUtils, DateUtils
from src.common.module_exception import ModuleException


class DynamicSourceQuery:
    """
    DynamicSourceQuery Class

    Dynamic sql text source object
    """

    def __init__(self, sql_uid="temp"):
        self.select_col_list = []
        self.table_name_list = []
        self.where_col_list = []
        self.origin_where_sql = None
        self.sql_uid = sql_uid
        self.join_col_list = []
        self.origin_sql_text = None
        self.valid_sql_df = None
        self.from_token_index_list = []
        self.where_token_index_list = []
        self.sql_text_without_comment = None
        self.invalid_sql_df = None

    def __repr__(self):
        """
        __repr__ 함수
        """
        return (
            f"{self.sql_uid} parsing result\n"
            f"select column : {self.select_col_list} \n"
            f"table name : {self.table_name_list} \n"
            f"join column : {self.join_col_list} \n"
            f"where column : {self.where_col_list}"
        )

    def analyze_sql_text(self, sql_text):
        """
        Dynamic sql text 분석 및 매칭 데이터 세팅
        :param sql_text: 로컬에 저장된 sql text 원본 데이터
        """
        sql_text = sql_text.lower().strip()
        sql_text = sql_text.replace("(+)", "")

        parts_of_cols_dict = DynamicSqlSearch.parsing_columns(sql_text)
        sql_text_without_comment = sqlparse.format(sql_text, strip_comments=True)
        from_token_index = DynamicSqlSearch.get_from_token_index(sql_text_without_comment)
        where_token_index = [
            token.position
            for token in Parser(sql_text_without_comment).tokens
            if token.is_keyword and token.normalized == "WHERE"
        ]

        self.select_col_list = parts_of_cols_dict.get("select", [])
        self.where_col_list = parts_of_cols_dict.get("where", [])
        self.join_col_list = parts_of_cols_dict.get("join", [])

        self.table_name_list = DynamicSqlSearch.parsing_tables(sql_text)
        self.from_token_index_list = from_token_index
        self.origin_sql_text = sql_text
        self.sql_text_without_comment = sql_text_without_comment
        self.where_token_index_list = where_token_index


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
        self.dynamic_sql_parquet_file_name = None

    def main_process(self):
        """
        Main process 함수
        """
        self.logger.info("DynamicSqlSearch main_process")
        faulthandler.enable()
        self.n_cores = int(os.cpu_count() * 0.5)

        self.chunk_size = int(self.config.get("data_handling_chunksize", 10000))
        self.export_parquet_root_path = f'{self.config["home"]}/{SystemConstants.EXPORT_PARQUET_PATH}'

        self.dynamic_sql_parquet_file_name = (
            f"{SystemConstants.DB_SQL_TEXT_FOR_DYNAMIC_FILE_NAME}{SystemConstants.PARQUET_FILE_EXT}"
        )

        if self.config["args"]["proc"] == "p":
            self.logger.info("dynamic query parsing process execute..")

            with TimeLogger("_export_db_sql_text(),elapsed ", self.logger):
                self._export_db_sql_text()

        elif self.config["args"]["proc"] == "d":
            self.logger.info("dynamic query analyze process execute..")

            with TimeLogger("_set_dynamic_source_query(),elapsed ", self.logger):
                self._set_dynamic_source_query()

            with TimeLogger("_search_dynamic_sql_text(),elapsed ", self.logger):
                self._search_dynamic_sql_text()

            with TimeLogger("_upsert_valid_df_in_sql_list(),elapsed ", self.logger):
                self._upsert_valid_df_in_sql_list()

            with TimeLogger("_make_excel(),elapsed ", self.logger):
                self._make_excel()

    def _upsert_valid_df_in_sql_list(self):
        """
        분석 후 유효한 데이터의 추가 정보 추출 및 DB 저장 기능.
        """
        self._init_sa_target()

        ae_db_info_df = self.st.get_ae_db_info()
        ae_db_infos = ae_db_info_df["lpad_db_id"].to_list()

        date_conditions = DateUtils.set_date_conditions_by_interval(
            self.config["args"]["s_date"], self.config["args"]["interval"], return_fmt="%y%m%d"
        )
        partition_key_list = [f"{date}{db_info}" for db_info in ae_db_infos for date in date_conditions]
        list_type_col_names = ["select", "where", "tables", "join"]
        fillna_col_names = ["avg_lio", "avg_pio", "avg_elapsed_time", "exec"]

        for dynamic_source_sql in self.dynamic_source_sql_list:
            if dynamic_source_sql.valid_sql_df is None:
                continue

            sql_uid_list = dynamic_source_sql.valid_sql_df["sql_uid"].to_list()

            df = self.st.get_ae_sql_stat_10min_by_sql_uid_and_partition_key(sql_uid_list, partition_key_list)

            dynamic_source_sql.valid_sql_df = pd.merge(dynamic_source_sql.valid_sql_df, df, on="sql_uid", how="left")
            dynamic_source_sql.valid_sql_df[fillna_col_names] = dynamic_source_sql.valid_sql_df[
                fillna_col_names
            ].fillna(0)
            dynamic_source_sql.valid_sql_df["sql_id"].fillna("", inplace=True)

            tmp_df = dynamic_source_sql.valid_sql_df.copy()

            tmp_df.drop(
                columns=["check_contains", "parts_of_cols", "from_token_index", "sql_text_without_comment", "sql_text"],
                axis=1,
                inplace=True,
            )

            tmp_df["source_sql_uid"] = dynamic_source_sql.sql_uid
            tmp_df["source_where_cols"] = ",".join(dynamic_source_sql.where_col_list)
            tmp_df[list_type_col_names] = tmp_df[list_type_col_names].applymap(self._list_to_str)

            tmp_df.rename(
                columns={
                    "select": "source_sel_cols",
                    "tables": "source_table_names",
                    "join": "source_join_cols",
                    "sql_uid": "target_sql_uid",
                    "where": "target_where_cols",
                    "sql_id": "target_sql_id",
                },
                inplace=True,
            )

            self.st.upsert_data(tmp_df, TableConstants.AE_DYNAMIC_SQL_SEARCH_RESULT)

    def _make_excel(self):
        """
        분석 후 유효한 데이터의 엑셀 저장 기능.
        """

    def _search_dynamic_sql_text(self):
        """
        미리 분석된 MaxGauge sql text와 dynamic sql search 기능.
        """
        pre_proc_file = pq.ParquetFile(f"{self.export_parquet_root_path}/{self.dynamic_sql_parquet_file_name}")
        loop_cnt = 0

        for batch in pre_proc_file.iter_batches(batch_size=self.chunk_size):
            df = batch.to_pandas()
            df = self._extract_parts_of_cols(df)
            df["tables"] = df["tables"].apply(lambda x: literal_eval(x))

            for dynamic_source_sql in self.dynamic_source_sql_list:
                valid_sql_df = df[
                    df["select"].apply(lambda x: x == dynamic_source_sql.select_col_list)
                    & df["join"].apply(lambda x: x == dynamic_source_sql.join_col_list)
                    & df["tables"].apply(lambda x: x == dynamic_source_sql.table_name_list)
                ]

                if len(valid_sql_df) == 0:
                    continue

                valid_sql_df = self._sql_metadata_from_index_parsing_by_df(valid_sql_df)
                valid_sql_df = valid_sql_df[
                    valid_sql_df["from_token_index"].apply(lambda x: x == dynamic_source_sql.from_token_index_list)
                ]

                if len(valid_sql_df) == 0:
                    continue

                valid_sql_df["check_contains"] = valid_sql_df["where"].apply(
                    lambda x: self._check_contains_where_col(x, dynamic_source_sql.where_col_list)
                )

                valid_sql_df = valid_sql_df[valid_sql_df["check_contains"]]

                if len(valid_sql_df) == 0:
                    continue

                valid_sql_df["dynamic_pattern"] = valid_sql_df["sql_text_without_comment"].apply(
                    lambda x: self._make_from_where_pattern(x, dynamic_source_sql.from_token_index_list[-1])
                )

                dynamic_source_sql.valid_sql_df = pd.concat([dynamic_source_sql.valid_sql_df, valid_sql_df])

            loop_cnt += len(df)

            self.logger.info(f"{loop_cnt} rows processing..")

    def _set_dynamic_source_query(self):
        """
        dynamic source query 추출 함수
        """
        home_parent_path = Path(self.config["home"]).parent
        dynamic_sql_path = os.path.join(home_parent_path, SystemConstants.DYNAMIC_SQL_TEXT_PATH)

        if not os.path.isdir(dynamic_sql_path):
            os.mkdir(dynamic_sql_path)

        dynamic_sql_list = os.listdir(dynamic_sql_path)

        self.logger.info(f"dynamic sql path {len(dynamic_sql_list)} things exist")

        if len(dynamic_sql_list) == 0:
            raise ModuleException("W003")

        self._set_dynamic_sql_text_in_txt(dynamic_sql_path, dynamic_sql_list)

    def _set_dynamic_sql_text_in_txt(self, dynamic_sql_path, dynamic_sql_list):
        """
        txt 파일에서 dynamic sql 추출하여 object 생성 함수
        :param dynamic_sql_path: dynamic sql txt 파일 path
        :param dynamic_sql_list: dynamic sql sql_uid로 폴더링된 list
        """
        for sql_uid in dynamic_sql_list:
            self.logger.info(f"{sql_uid} parsing start..")

            dynamic_sql_files = SystemUtils.get_filenames_from_path(
                os.path.join(dynamic_sql_path, sql_uid), suffix=".sql"
            )

            if len(dynamic_sql_files) > 1:
                self.logger.warn(f"[{sql_uid}] dynamic sql file {len(dynamic_sql_files)} exist. Check file in path")

            dynamic_sql_file = dynamic_sql_files[0]
            sql_text = SystemUtils.get_file_content_in_path(os.path.join(dynamic_sql_path, sql_uid), dynamic_sql_file)

            self._set_dynamic_source_sql_object(sql_uid, sql_text)

    def _set_dynamic_source_sql_object(self, sql_uid, sql_text):
        """
        dynamic source sql 객체 생성 및 값 세팅 함수
        :param sql_uid: 분석하는 sql text의 sql_uid
        :param sql_text: 분석하는 sql text
        """
        dsq = DynamicSourceQuery(sql_uid)
        dsq.analyze_sql_text(sql_text)
        self.logger.info(dsq)
        self.dynamic_source_sql_list.append(dsq)

    def _sql_metadata_parsing_by_df(self, df):
        """
        sql_metadata lib를 사용한 sql parsing
        :param df: 파싱 전 데이터 프레임
        :return: 결과 데이터 프레임
        """
        df["parts_of_cols"] = df["sql_text"].apply(lambda x: self.parsing_columns(x))
        df["tables"] = df["sql_text"].apply(lambda x: self.parsing_tables(x))
        return df

    def _sql_metadata_from_index_parsing_by_df(self, df):
        """
        sql_metadata lib를 사용한 sql table toke index parsing
        :param df: 파싱 전 데이터 프레임
        :return: 결과 데이터 프레임
        """
        df["sql_text_without_comment"] = df["sql_text"].apply(lambda x: sqlparse.format(x, strip_comments=True))
        df["from_token_index"] = df["sql_text_without_comment"].apply(lambda x: self.get_from_token_index(x))
        return df

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

        dynamic_sql_parquet_file = f"{self.export_parquet_root_path}/{self.dynamic_sql_parquet_file_name}"
        is_exist_parquet_data = False

        if os.path.isfile(dynamic_sql_parquet_file):
            analyzed_sql_uid_df = pq.read_pandas(
                dynamic_sql_parquet_file,
                columns=[
                    "sql_uid",
                ],
            ).to_pandas()
            is_exist_parquet_data = True
        else:
            analyzed_sql_uid_df = pd.DataFrame({"sql_uid": []})

        self.logger.info(f"is_exist_parquet_data : {is_exist_parquet_data}, {len(analyzed_sql_uid_df)} rows")

        pq_writer = None

        for date in date_conditions:
            for db_id in ae_db_infos:
                total_row_cnt = 0
                partition_key = f"{date}{db_id}"

                with TimeLogger(f"Make_parquet_file_ae_db_sql_text (date:{date}, db_id:{db_id}),elapsed ", self.logger):
                    for df in self.st.get_ae_db_sql_text_by_1seq_orderby(partition_key, chunksize=self.chunk_size):
                        if len(df) == 0:
                            break

                        df = df[~df["sql_uid"].isin(analyzed_sql_uid_df["sql_uid"])]

                        if len(df) == 0:
                            continue

                        analyzed_sql_uid_df = pd.concat([analyzed_sql_uid_df, df], axis=0, ignore_index=True).drop(
                            columns="partition_key"
                        )

                        results = self.st.get_all_ae_db_sql_text_by_1seq(df, chunksize=self.chunk_size)

                        grouping_df = MaxGaugeUtils.reconstruct_by_grouping(results)
                        grouping_df = self._preprocessing(grouping_df)

                        if len(grouping_df) == 0:
                            continue

                        self._teardown_sa_target()

                        grouping_df = self.parallelize_dataframe(
                            grouping_df, self._sql_metadata_parsing_by_df, n_cores=self.n_cores
                        )
                        grouping_df = grouping_df.astype({"parts_of_cols": "str", "tables": "str"})

                        self._init_sa_target()

                        if pq_writer is None:
                            if is_exist_parquet_data:
                                # 기존파일이 커서 메모리 오류 발생하면 tmp 파일 변경 후 batch로 읽어들이면서 재생성 하도록 변경
                                grouping_df = pd.concat([pd.read_parquet(dynamic_sql_parquet_file), grouping_df])

                            pq_writer = pf.get_pqwriter(
                                self.export_parquet_root_path, self.dynamic_sql_parquet_file_name, grouping_df
                            )

                        pf.make_parquet_by_df(grouping_df, pq_writer)
                        total_row_cnt += len(grouping_df)

                    self.logger.info(f"Total export data count (date:{date}, db_id:{db_id}): {total_row_cnt} rows")

        if pq_writer:
            pq_writer.close()

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

    @staticmethod
    def _preprocessing(grouping_df):
        """
        전처리 함수
        :param grouping_df: grouping된 db sql text 데이터 프레임
        :return: 필터링, 전처리된 데이터 프레임임
        """
        sql_filter = (
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
        )

        grouping_df["sql_text"] = grouping_df["sql_text"].str.lower().str.strip()
        grouping_df["sql_text"] = grouping_df["sql_text"].str.replace(r"(+)", "", regex=False)
        grouping_df = grouping_df[~grouping_df["sql_text"].str.startswith(sql_filter)]
        return grouping_df

    @staticmethod
    def get_from_token_index(sql_text):
        """
        from 절 토큰 인덱스 추출 함수
        :param sql_text: 추출하려는 sql text
        :return: 토큰 인덱스 리스트
        :except: -1 리스트
        """
        try:
            from_token_indexs = [
                token.position for token in Parser(sql_text).tokens if token.is_keyword and token.normalized == "FROM"
            ]
            if len(from_token_indexs) == 0:
                from_token_indexs = [-1]
            return from_token_indexs
        except Exception:
            return [-1]

    @staticmethod
    def parsing_columns(sql_text):
        """
        컬럼 파싱 함수
        :param sql_text: 파싱 대상 sql text
        :return: 파싱 결과
        :except: 빈 dict
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
        :return: 파싱 결과
        :except: 빈 list
        """
        try:
            return Parser(sql_text).tables if Parser(sql_text).tables is not None else []
        except Exception:
            return []

    @staticmethod
    def _check_contains_where_col(target_where_col_list: list, dynamic_src_sql_where_col_list: list):
        """
        where 절 컬럼을 포함 여부 확인 함수
        :param target_where_col_list: 대상 where 컬럼 리스트
        :param dynamic_src_sql_where_col_list: 다이나믹 sql where 컬럼 리스트
        :return: 포함 여부
        """
        return all(col in target_where_col_list for col in dynamic_src_sql_where_col_list)

    @staticmethod
    def _extract_parts_of_cols(df):
        """
        sql 구문별 컬럼 추출 함수
        :param df: 추출하려는 sql text 데이터 프레임
        :return: 추출된 데이터 프레임
        """
        df["parts_of_cols"] = df["parts_of_cols"].apply(lambda x: literal_eval(x))

        df["select"] = df["parts_of_cols"].apply(lambda x: x.get("select", []))
        df["join"] = df["parts_of_cols"].apply(lambda x: x.get("join", []))
        df["where"] = df["parts_of_cols"].apply(lambda x: x.get("where", []))
        return df

    @staticmethod
    def _list_to_str(target_list, sep=","):
        """
        리스트를 문자열로 변환하는 함수
        :param l: 대상 리스트
        :param sep: 구분자
        :return: 변환된 문자열
        """
        return sep.join(target_list)

    @staticmethod
    def _make_from_where_pattern(sql_text_without_comment, from_position):
        """
        from ~ where patter str 생성 함수
        :param sql_text_without_comment: 주석 제거 sql text
        :param from_position: from절 포지션
        :return: from ~ where str
        """
        sql_str_list_by_from_position_tokens = [
            token for token in Parser(sql_text_without_comment).tokens if token.position >= from_position
        ]

        sql_str_list_by_from_position = [
            f"\n{str(token)}"
            if token.is_keyword and (token.normalized == "WHERE" or token.normalized == "AND")
            else str(token)
            for token in sql_str_list_by_from_position_tokens
        ]

        return " ".join(sql_str_list_by_from_position)
