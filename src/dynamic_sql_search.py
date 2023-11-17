import re
import faulthandler
import os

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from pathlib import Path
from sql_metadata.parser import Parser
from sql_metadata.keywords_lists import TokenType
from pathos import multiprocessing
from pprint import pformat
from datetime import datetime

from src import common_module as cm
from src.common.timelogger import TimeLogger
from src.common.constants import SystemConstants, TableConstants, DateFmtConstants
from src.common.file_export import ParquetFile
from src.common.utils import SystemUtils, MaxGaugeUtils, DateUtils
from src.common.module_exception import ModuleException
from src.excel.excel_writer import ExcelWriter, ExcelAlignment


class AnalyzedQueryPartObj:
    """
    AnalyzedQueryPartObj Class

    Analyzed Query Part Template object
    """

    def __init__(self):
        self.sql_fixed_part = {
            "select_col_list": {},
            "table_name_list": {},
            "join_col_list": {},
            "where_col_list": {},
            "etc_col_list": {},
        }
        self.sql_dynamic_part = {
            "select_col_list": {},
            "table_name_list": {},
            "join_col_list": {},
            "where_col_list": {},
            "etc_col_list": {},
        }


class DynamicSourceQuery(AnalyzedQueryPartObj):
    """
    DynamicSourceQuery Class

    Dynamic sql text source object
    """

    def __init__(self, sql_id, dynamic_where_index):
        super().__init__()
        self.sql_id = sql_id
        self.dynamic_where_index = dynamic_where_index

        self.sql_uid = ""
        self.origin_sql_text = None
        self.valid_sql_df = None
        self.where_token_index_list = []
        self.invalid_sql_df = None
        self.tmp_df = None
        self.first_fixed_sel_depth = 0
        self.first_fixed_sel_col = ""
        self.first_fixed_table_depth = 0
        self.first_fixed_table_name = ""

    def __repr__(self):
        """
        __repr__ 함수
        """
        return (
            f"{self.sql_uid} parsing result\n"
            f"sql_fixed_part : {pformat(self.sql_fixed_part)} \n"
            f"{'*' * 99}\n"
            f"sql_dynamic_part: {pformat(self.sql_dynamic_part)} \n"
        )

    def analyze_sql_text(self, sql_text):
        """
        Dynamic sql text 분석 및 매칭 데이터 세팅
        :param sql_text: 로컬에 저장된 sql text 원본 데이터
        :param dynamic_where_index: dynamic sql patter이 시작되는 where keyword index
        """
        sql_text = sql_text.lower().strip()
        sql_text = re.sub(r"\( *\+ *\)", "", sql_text)
        sql_text = re.sub(r"--.*\n", "", sql_text)
        sql_text = re.sub(r"--.*\r", "", sql_text)

        parser = DynamicSqlSearch.get_sql_metadata_obj(sql_text)

        # where idx 0 인것 고려해야함
        where_token_index = DynamicSqlSearch.get_where_token_idx(parser)

        index = 1
        for token in parser.tokens:
            # # 같은 depth 에서 키워드가 변경될때, select from where 에서만
            # if token.next_token is not None and token.next_token.last_keyword != stack.peek():
            #     stack.pop()
            #     stack.push(token.next_token.last_keyword)
            #
            # # 여는 소괄호가 나오면 1. 함수 2. 서브쿼리 반드시 select가 나옴
            # if token.is_left_parenthesis:
            #     if token.next_token is not None and token.next_token.normalized == "SELECT":
            #         stack.push(token.next_token.normalized)
            #     else:
            #         stack.push(token.next_token.last_keyword)
            #
            # # 닫는 소괄호가 나오면
            # elif token.is_right_parenthesis:
            #     stack.pop()

            if len(where_token_index) == 0 or token.position < where_token_index[int(self.dynamic_where_index) - 1]:
                DynamicSqlSearch.analyze_token_type(token, self.sql_fixed_part, parser.columns_aliases)
            else:
                DynamicSqlSearch.analyze_token_type(token, self.sql_dynamic_part, parser.columns_aliases)

        index += 1

        self.first_fixed_sel_depth, self.first_fixed_sel_col = DynamicSqlSearch.extract_first_key_and_value_in_obj(
            self.sql_fixed_part, "select_col_list"
        )
        self.first_fixed_table_depth, self.first_fixed_table_name = DynamicSqlSearch.extract_first_key_and_value_in_obj(
            self.sql_fixed_part, "table_name_list"
        )
        self.origin_sql_text = sql_text
        self.where_token_index_list = where_token_index


class DynamicSqlSearch(cm.CommonModule):
    """
    DynamicSqlSearch Class

    Dynamic sql text 분석 및 찾기 위한 Class
    """

    DYNAMIC_PATTERN_COLUMN_IDX = "D"
    DYNAMIC_PATTERN_COLUMN_WITDH = 80
    FREEZE_PANES_IDX = "A5"
    DEFAULT_CELL_HEIGHT = 100

    def __init__(self, logger):
        super().__init__(logger)
        self.export_parquet_root_path = None
        self.chunk_size = 0
        self.dynamic_source_sql_list = []
        self.n_cores = 0
        self.dynamic_sql_parquet_file_name = None
        self.dynamic_sql_path = None

    def main_process(self):
        """
        Main process 함수
        """
        self.logger.info("DynamicSqlSearch main_process")
        faulthandler.enable()
        self.n_cores = int(os.cpu_count() * 0.5)

        self.chunk_size = int(self.config.get("data_handling_chunksize", 10000))
        self.chunk_size = int(self.chunk_size / 2)
        self.export_parquet_root_path = f'{self.config["home"]}/{SystemConstants.EXPORT_PARQUET_PATH}'
        self.dynamic_sql_parquet_file_name = (
            f"{SystemConstants.DB_SQL_TEXT_FOR_DYNAMIC_FILE_NAME}_{DateUtils.get_now_timestamp()}"
            f"{SystemConstants.PARQUET_FILE_EXT}"
        )

        if self.config["args"]["proc"] == "p":
            self.logger.info("dynamic query parsing process execute..")

            with TimeLogger("_export_db_sql_text(),elapsed ", self.logger):
                self._export_db_sql_text()

        elif self.config["args"]["proc"] == "d":
            self.logger.info("dynamic query analyze process execute..")

            self._init_sa_target()

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
        ae_db_info_df = self.st.get_ae_db_info()
        ae_db_infos = ae_db_info_df["lpad_db_id"].to_list()

        date_conditions = DateUtils.set_date_conditions_by_interval(
            self.config["args"]["s_date"], self.config["args"]["interval"], return_fmt="%y%m%d"
        )

        self.logger.debug(f"ae_db_infos : {ae_db_infos}")
        self.logger.debug(f"request date_conditions : {date_conditions}")

        partition_key_list = [f"{date}{db_info}" for db_info in ae_db_infos for date in date_conditions]
        fillna_col_names = [
            "avg_lio",
            "avg_pio",
            "avg_elapsed_time",
            "avg_cpu_time",
            "exec",
            "sum_lio",
            "sum_pio",
            "sum_elapsed_time",
            "sum_cpu_time",
        ]
        sum_metrics = ["sum_lio", "sum_pio", "sum_elapsed_time", "sum_cpu_time"]

        for dynamic_source_sql in self.dynamic_source_sql_list:
            if dynamic_source_sql.valid_sql_df is None:
                self.logger.info(f"{dynamic_source_sql.sql_id} is no valid sql data.. continue...")
                continue

            sql_uid_list = dynamic_source_sql.valid_sql_df["sql_uid"].to_list()

            df = self.st.get_ae_sql_stat_10min_by_sql_uid_and_partition_key(sql_uid_list, partition_key_list)

            dynamic_source_sql.valid_sql_df = pd.merge(dynamic_source_sql.valid_sql_df, df, on="sql_uid", how="left")
            dynamic_source_sql.valid_sql_df[fillna_col_names] = (
                dynamic_source_sql.valid_sql_df[fillna_col_names].fillna(0).round(1)
            )

            dynamic_source_sql.valid_sql_df["sql_id"].fillna("", inplace=True)

            dynamic_source_sql.valid_sql_df["target_where_cols"] = (
                dynamic_source_sql.valid_sql_df["sql_dynamic_part_by_idx"]
                .apply(lambda x: x.get("where_col_list", {}))
                .astype(str)
            )

            DynamicSqlSearch.calc_ratio_each_metric(dynamic_source_sql, sum_metrics)

            tmp_df = dynamic_source_sql.valid_sql_df.copy()
            # tmp_df["where"] = tmp_df["where"].apply(lambda x: sorted(x))

            tmp_df.drop(
                columns=["sql_text", "sql_dynamic_part_by_idx", "sql_fixed_part_by_idx"],
                axis=1,
                inplace=True,
            )

            tmp_df["source_sql_uid"] = dynamic_source_sql.sql_uid
            tmp_df["source_sql_id"] = dynamic_source_sql.sql_id

            tmp_df["source_fixed_sel_cols"] = str(dynamic_source_sql.sql_fixed_part.get("select_col_list", {}))
            tmp_df["source_fixed_join_cols"] = str(dynamic_source_sql.sql_fixed_part.get("join_col_list", {}))
            tmp_df["source_fixed_table_names"] = str(dynamic_source_sql.sql_fixed_part.get("table_name_list", {}))
            tmp_df["source_fixed_where_cols"] = str(dynamic_source_sql.sql_fixed_part.get("where_col_list", {}))

            tmp_df["source_dynamic_sel_cols"] = str(dynamic_source_sql.sql_dynamic_part.get("select_col_list", {}))
            tmp_df["source_dynamic_join_cols"] = str(dynamic_source_sql.sql_dynamic_part.get("join_col_list", {}))
            tmp_df["source_dynamic_table_names"] = str(dynamic_source_sql.sql_dynamic_part.get("table_name_list", {}))
            tmp_df["source_dynamic_where_cols"] = str(dynamic_source_sql.sql_dynamic_part.get("where_col_list", {}))

            tmp_df.rename(
                columns={
                    "sql_uid": "target_sql_uid",
                    "sql_id": "target_sql_id",
                },
                inplace=True,
            )
            dynamic_source_sql.tmp_df = tmp_df
            self.st.upsert_data(tmp_df, TableConstants.AE_DYNAMIC_SQL_SEARCH_RESULT)

    def _make_excel(self):
        """
        분석 후 유효한 데이터 엑셀 저장 기능.
        """
        columns_with_width_dict = {
            DynamicSqlSearch.DYNAMIC_PATTERN_COLUMN_IDX: DynamicSqlSearch.DYNAMIC_PATTERN_COLUMN_WITDH
        }
        source_sql_style_option_dict = {"source_sql_text": ExcelAlignment.ALIGN_WRAP_TEXT}
        target_sql_style_option_dict = {"dynamic_pattern": ExcelAlignment.ALIGN_WRAP_TEXT}
        ratio_metrics = ["ratio_elapsed_time", "ratio_cpu_time", "ratio_lio", "ratio_pio"]

        for dynamic_source_sql_obj in self.dynamic_source_sql_list:
            source_df = self._make_export_excel_source_data_by_obj(dynamic_source_sql_obj)

            start_row_index = 1

            # 기본설정
            eww = ExcelWriter()
            eww.create_workbook()
            export_excel_file_name = eww.set_active_sheet_name(dynamic_source_sql_obj.sql_id)

            eww.set_cell_width_columns(columns_with_width_dict)
            eww.set_height_row(row=2, height=DynamicSqlSearch.DEFAULT_CELL_HEIGHT)
            eww.set_freeze_panes(DynamicSqlSearch.FREEZE_PANES_IDX)

            # 상단 source sql
            eww.set_value_from_pandas(source_df, start_row_index, align_vertical=True, contain_header=True)
            eww.set_style_by_option(source_df, start_row_index, source_sql_style_option_dict)
            eww.set_border_by_option(source_df, start_row_index - 1)

            # 하단 target sql
            if dynamic_source_sql_obj.tmp_df is not None and len(dynamic_source_sql_obj.tmp_df) > 0:
                export_excel_data_df = DynamicSqlSearch.make_export_excel_target_data_by_df(
                    dynamic_source_sql_obj.tmp_df,
                )

                target_columns = DynamicSqlSearch.extract_ratio_metric_column_idx(export_excel_data_df, ratio_metrics)

                export_excel_data_df = DynamicSqlSearch.rename_export_excel_data_df(export_excel_data_df)

                next_row_index = len(source_df) + start_row_index + 1

                eww.set_value_from_pandas(
                    export_excel_data_df, next_row_index, align_vertical=True, contain_header=True
                )

                eww.set_style_by_option(export_excel_data_df, next_row_index, target_sql_style_option_dict)
                eww.set_border_by_option(export_excel_data_df, next_row_index)
                eww.set_databar_by_value(export_excel_data_df, next_row_index, target_columns)

                for idx in range(len(export_excel_data_df)):
                    eww.set_height_row(row=next_row_index + 1 + idx + 1, height=DynamicSqlSearch.DEFAULT_CELL_HEIGHT)

                last_content_index = next_row_index + len(export_excel_data_df) + 2

                target_sql_uids = DynamicSqlSearch.make_debug_value_by_df(export_excel_data_df, "target_sql_uid")
                target_sql_ids = DynamicSqlSearch.make_debug_value_by_df(export_excel_data_df, "target_sql_id")

                eww.set_value_to_target(
                    ",\n".join(target_sql_uids),
                    f"A{last_content_index}",
                    last_content_index,
                    DynamicSqlSearch.DEFAULT_CELL_HEIGHT,
                )
                eww.set_value_to_target(
                    ",\n".join(target_sql_ids),
                    f"B{last_content_index}",
                    last_content_index,
                    DynamicSqlSearch.DEFAULT_CELL_HEIGHT,
                )

            else:
                self.logger.info(f"{dynamic_source_sql_obj.sql_id} sql_id not exists valid data")
                export_excel_file_name = "nodata"

            eww.save_workbook(
                f"{self.dynamic_sql_path}/{dynamic_source_sql_obj.sql_id}",
                f"{export_excel_file_name}_{datetime.now().strftime(DateFmtConstants.DATE_YMDHMS)}.xlsx"
                if self.config["env"] == "prod"
                else f"{export_excel_file_name}.xlsx",
            )

    def _make_export_excel_source_data_by_obj(self, dynamic_source_sql_obj):
        """
        source sql 데이터 엑셀 추출을 위한 변환 함수
        :param dynamic_source_sql_obj: dynamic source sql object
        :return: 추출된 데이터 프레임
        """
        start_dt, end_dt = DateUtils.get_each_date_by_interval2(
            self.config["args"]["s_date"], self.config["args"]["interval"], "%Y%m%d"
        )

        source_df = pd.DataFrame.from_dict(
            [
                {
                    "source_sql_uid": dynamic_source_sql_obj.sql_uid,
                    "source_sql_id": dynamic_source_sql_obj.sql_id,
                    "source_dynamic_where_cols": str(dynamic_source_sql_obj.sql_dynamic_part.get("where_col_list", {})),
                    "source_sql_text": dynamic_source_sql_obj.origin_sql_text,
                    "source_fixed_sel_cols": str(dynamic_source_sql_obj.sql_fixed_part.get("select_col_list", {})),
                    "source_fixed_table_names": str(dynamic_source_sql_obj.sql_fixed_part.get("table_name_list", {})),
                    "source_fixed_join_cols": str(dynamic_source_sql_obj.sql_fixed_part.get("join_col_list", {})),
                    "source_fixed_where_cols": str(dynamic_source_sql_obj.sql_fixed_part.get("where_col_list", {})),
                    "source_dynamic_sel_cols": str(dynamic_source_sql_obj.sql_dynamic_part.get("select_col_list", {})),
                    "source_dynamic_table_names": str(
                        dynamic_source_sql_obj.sql_dynamic_part.get("table_name_list", {})
                    ),
                    "source_dynamic_join_cols": str(dynamic_source_sql_obj.sql_dynamic_part.get("join_col_list", {})),
                    "analyze_date": f"{start_dt}~{end_dt}",
                }
            ]
        )

        return source_df

    def _search_dynamic_sql_text(self):
        """
        미리 분석된 MaxGauge sql text와 dynamic sql search 기능.
        """
        dynamic_sql_parquet_files = SystemUtils.get_filenames_from_path(
            self.export_parquet_root_path, prefix=SystemConstants.DB_SQL_TEXT_FOR_DYNAMIC_FILE_NAME
        )

        if dynamic_sql_parquet_files:
            analyzed_sql_uid_df = self._get_df_by_parquet(dynamic_sql_parquet_files, "sql_uid")
            self.logger.info(f"Analyzed sql text total data : {len(analyzed_sql_uid_df)} rows")
            del analyzed_sql_uid_df
        else:
            raise ModuleException("E010")

        loop_cnt = 0
        for dynamic_sql_parquet_file in dynamic_sql_parquet_files:
            pre_proc_file = pq.ParquetFile(f"{self.export_parquet_root_path}/{dynamic_sql_parquet_file}")

            for batch in pre_proc_file.iter_batches(batch_size=self.chunk_size):
                df = batch.to_pandas()

                for dynamic_source_sql in self.dynamic_source_sql_list:
                    valid_sql_df = df[
                        (df.first_fixed_sel_depth == dynamic_source_sql.first_fixed_sel_depth)
                        & (df.first_fixed_sel_col == dynamic_source_sql.first_fixed_sel_col)
                        & (df.first_fixed_table_depth == dynamic_source_sql.first_fixed_table_depth)
                        & (df.first_fixed_table_name == dynamic_source_sql.first_fixed_table_name)
                    ]

                    # where keyword index가 작은것들 제거
                    valid_sql_df = valid_sql_df[
                        ~valid_sql_df["where_token_len"].apply(
                            lambda x: int(x) < int(dynamic_source_sql.dynamic_where_index)
                        )
                    ]

                    if len(valid_sql_df) == 0:
                        continue

                    valid_sql_df = self._valid_sql_parse_by_where_index(
                        valid_sql_df, dynamic_source_sql.dynamic_where_index
                    )

                    # fixed parts 비교
                    valid_sql_df = valid_sql_df[
                        valid_sql_df["sql_fixed_part_by_idx"].apply(
                            lambda x: self.compare_dicts(
                                x.get("select_col_list", {}),
                                dynamic_source_sql.sql_fixed_part.get("select_col_list", {}),
                            )
                        )
                        & valid_sql_df["sql_fixed_part_by_idx"].apply(
                            lambda x: self.compare_dicts(
                                x.get("table_name_list", {}),
                                dynamic_source_sql.sql_fixed_part.get("table_name_list", {}),
                            )
                        )
                        & valid_sql_df["sql_fixed_part_by_idx"].apply(
                            lambda x: self.compare_dicts(
                                x.get("join_col_list", {}), dynamic_source_sql.sql_fixed_part.get("join_col_list", {})
                            )
                        )
                        & valid_sql_df["sql_fixed_part_by_idx"].apply(
                            lambda x: self.compare_dicts(
                                x.get("where_col_list", {}), dynamic_source_sql.sql_fixed_part.get("where_col_list", {})
                            )
                        )
                    ]

                    if len(valid_sql_df) == 0:
                        continue

                    valid_sql_df = valid_sql_df[
                        valid_sql_df["sql_dynamic_part_by_idx"].apply(
                            lambda x: DynamicSqlSearch.check_contains_where_col(
                                x.get("select_col_list", {}),
                                dynamic_source_sql.sql_dynamic_part.get("select_col_list", {}),
                            )
                        )
                        & valid_sql_df["sql_dynamic_part_by_idx"].apply(
                            lambda x: DynamicSqlSearch.check_contains_where_col(
                                x.get("table_name_list", {}),
                                dynamic_source_sql.sql_dynamic_part.get("table_name_list", {}),
                            )
                        )
                        & valid_sql_df["sql_dynamic_part_by_idx"].apply(
                            lambda x: DynamicSqlSearch.check_contains_where_col(
                                x.get("join_col_list", {}), dynamic_source_sql.sql_dynamic_part.get("join_col_list", {})
                            )
                        )
                        & valid_sql_df["sql_dynamic_part_by_idx"].apply(
                            lambda x: DynamicSqlSearch.check_contains_where_col(
                                x.get("where_col_list", {}),
                                dynamic_source_sql.sql_dynamic_part.get("where_col_list", {}),
                            )
                        )
                    ]

                    if len(valid_sql_df) == 0:
                        continue

                    valid_sql_df.drop(
                        columns=[
                            "where_token_len",
                            "first_fixed_sel_depth",
                            "first_fixed_sel_col",
                            "first_fixed_table_name",
                            "first_fixed_table_depth",
                            "parser",
                            "where_token_index",
                        ],
                        inplace=True,
                    )

                    valid_sql_df["dynamic_pattern"] = valid_sql_df["sql_text"].apply(
                        lambda x: DynamicSqlSearch.make_from_where_pattern(
                            x, int(dynamic_source_sql.dynamic_where_index) - 1
                        )
                    )

                    dynamic_source_sql.valid_sql_df = pd.concat([dynamic_source_sql.valid_sql_df, valid_sql_df])

                loop_cnt += len(df)

                self.logger.info(f"{loop_cnt} rows processing..")

    def _valid_sql_parse_by_where_index(self, valid_df, where_index):
        valid_df["parser"] = valid_df["sql_text"].apply(lambda x: self.get_sql_metadata_obj(x))
        valid_df["where_token_index"] = valid_df["parser"].apply(lambda x: DynamicSqlSearch.get_where_token_idx(x))

        valid_df[["sql_fixed_part_by_idx", "sql_dynamic_part_by_idx"]] = valid_df.apply(
            lambda x: self._extract_part_by_where_idx(x, where_index), axis=1
        )
        return valid_df

    @staticmethod
    def _extract_part_by_where_idx(row, where_index):
        query_part_obj = AnalyzedQueryPartObj()

        sql_fixed_part = query_part_obj.sql_fixed_part
        sql_dynamic_part = query_part_obj.sql_dynamic_part

        parser = row["parser"]
        where_token_index = row["where_token_index"]

        for token in parser.tokens:
            if len(where_token_index) == 0 or token.position < where_token_index[int(where_index) - 1]:
                DynamicSqlSearch.analyze_token_type(token, sql_fixed_part, parser.columns_aliases)
            else:
                DynamicSqlSearch.analyze_token_type(token, sql_dynamic_part, parser.columns_aliases)

        return pd.Series([sql_fixed_part, sql_dynamic_part])

    def _set_dynamic_source_query(self):
        """
        dynamic source query 추출 함수
        """
        home_parent_path = Path(self.config["home"]).parent
        self.dynamic_sql_path = os.path.join(home_parent_path, SystemConstants.DYNAMIC_SQL_TEXT_PATH)

        if not os.path.isdir(self.dynamic_sql_path):
            os.mkdir(self.dynamic_sql_path)

        dynamic_sql_list = SystemUtils.get_folder_to_path(self.dynamic_sql_path)

        self.logger.info(f"dynamic sql path {len(dynamic_sql_list)} things exist")

        if len(dynamic_sql_list) == 0:
            raise ModuleException("W003")

        self._set_dynamic_sql_text_in_txt(dynamic_sql_list)

        self._set_dynamic_sql_uid()

    def _set_dynamic_sql_uid(self):
        """
        sql_id 로 ae_session_stat_10min 테이블에서 sql_uid 조회하여 세팅하는 함수
        """
        self.logger.info("set dynamic sql matching sql_uid start..")

        for dynamic_source_sql in self.dynamic_source_sql_list:
            result_df = self.st.get_ae_session_stat_10min_by_sql_id(dynamic_source_sql.sql_id)

            if len(result_df) >= 1:
                dynamic_source_sql.sql_uid = result_df.at[0, "sql_uid"]

        self.logger.info("set dynamic sql matching sql_uid end..")

    def _set_dynamic_sql_text_in_txt(self, dynamic_sql_list):
        """
        txt 파일에서 dynamic sql 추출하여 object 생성 함수
        :param dynamic_sql_list: dynamic sql sql_uid로 폴더링된 list
        """
        for sql_id in dynamic_sql_list:
            self.logger.info(f"{sql_id} parsing start..")

            dynamic_sql_files = SystemUtils.get_filenames_from_path(
                os.path.join(self.dynamic_sql_path, sql_id), suffix=".sql"
            )

            if dynamic_sql_files is None:
                continue

            if len(dynamic_sql_files) > 1:
                self.logger.warn(f"[{sql_id}] dynamic sql file {len(dynamic_sql_files)} exist. Check file in path")

            dynamic_sql_file = dynamic_sql_files[0]
            sql_text = SystemUtils.get_file_content_in_path(
                os.path.join(self.dynamic_sql_path, sql_id), dynamic_sql_file
            )

            file_name = dynamic_sql_file.split(".")[0]

            if "_" in file_name:
                dynamic_where_index = file_name.split("_")[1]
            else:
                self.logger.warn(
                    f"[{sql_id}] dynamic sql filename not contain where index. set default index 1. "
                    f"please check filename ex) xxxxxx_(dynamic where index).sql"
                )
                dynamic_where_index = "1"

            if not dynamic_where_index.isdecimal():
                raise ModuleException("E010")

            self._set_dynamic_source_sql_object(sql_id, sql_text, dynamic_where_index)

    def _set_dynamic_source_sql_object(self, sql_id, sql_text, dynamic_where_index):
        """
        dynamic source sql 객체 생성 및 값 세팅 함수
        :param sql_uid: 분석하는 sql text의 sql_uid
        :param sql_text: 분석하는 sql text
        """
        dsq = DynamicSourceQuery(sql_id, dynamic_where_index)
        dsq.analyze_sql_text(sql_text)
        self.logger.info(dsq)
        self.dynamic_source_sql_list.append(dsq)

    def _sql_metadata_parsing_by_df(self, df):
        """
        sql_metadata lib를 사용한 sql parsing
        :param df: 파싱 전 데이터 프레임
        :return: 결과 데이터 프레임
        """
        df[
            [
                "where_token_len",
                "first_fixed_sel_depth",
                "first_fixed_sel_col",
                "first_fixed_table_depth",
                "first_fixed_table_name",
            ]
        ] = df.apply(self._parsing_sql_each_parts, axis=1)
        return df

    def _parsing_sql_each_parts(self, row):
        """
        sql의 고정 부분과 다이나믹 부분을 각각 파싱하는 함수
        :param row: 파싱하려는 데이터프레임 row
        :return: 파싱된 각 Series
        """
        sql_text = row["sql_text"]
        sql_fixed_parts = {}
        sql_dynamic_parts = {}
        first_fixed_sel_depth = "-1"
        first_fixed_sel_col = "-1"
        first_fixed_table_depth = "-1"
        first_fixed_table_name = "-1"

        parser = self.get_sql_metadata_obj(sql_text)

        if not parser:
            return pd.Series(
                [
                    -1,
                    first_fixed_sel_depth,
                    first_fixed_sel_col,
                    first_fixed_table_depth,
                    first_fixed_table_name,
                ]
            )

        where_token_idxs = self.get_where_token_idx(parser)

        [
            DynamicSqlSearch.analyze_each_idx(idx, sql_fixed_parts, sql_dynamic_parts, parser, where_token_idxs)
            for idx in range(len(where_token_idxs))
            if idx < 1
        ]

        if len(where_token_idxs) > 0:
            first_fixed_sel_depth, first_fixed_sel_col = self.extract_first_key_and_value_in_obj(
                sql_fixed_parts.get("0"), "select_col_list"
            )
            first_fixed_table_depth, first_fixed_table_name = self.extract_first_key_and_value_in_obj(
                sql_fixed_parts.get("0"), "table_name_list"
            )

        return pd.Series(
            [
                len(where_token_idxs),
                first_fixed_sel_depth,
                first_fixed_sel_col,
                first_fixed_table_depth,
                first_fixed_table_name,
            ]
        )

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

        self.logger.debug(f"ae_db_infos : {ae_db_infos}")
        self.logger.debug(f"request date_conditions : {date_conditions}")

        dynamic_sql_parquet_files = SystemUtils.get_filenames_from_path(
            self.export_parquet_root_path, prefix=SystemConstants.DB_SQL_TEXT_FOR_DYNAMIC_FILE_NAME
        )

        if dynamic_sql_parquet_files:
            analyzed_sql_uid_df = self._get_df_by_parquet(dynamic_sql_parquet_files, "sql_uid")

        else:
            analyzed_sql_uid_df = pd.DataFrame({"sql_uid": []})

        self.logger.info(f"Already analyzed sql parquet data : {len(analyzed_sql_uid_df)} rows")

        pq_writer = None

        try:
            for date in date_conditions:
                for db_id in ae_db_infos:
                    total_row_cnt = 0
                    real_row_cnt = 0
                    partition_key = f"{date}{db_id}"

                    with TimeLogger(
                        f"Make_parquet_file_ae_db_sql_text (date:{date}, db_id:{db_id}),elapsed ", self.logger
                    ):
                        for df in self.st.get_ae_db_sql_text_by_1seq_orderby(partition_key, chunksize=self.chunk_size):
                            # df = df[df["sql_uid"].isin(filted_sql_uids)]
                            total_row_cnt += len(df)

                            df = df[~df["sql_uid"].isin(analyzed_sql_uid_df["sql_uid"])]

                            if len(df) == 0:
                                continue

                            analyzed_sql_uid_df = pd.concat([analyzed_sql_uid_df, df], axis=0, ignore_index=True).drop(
                                columns="partition_key"
                            )

                            results = self.st.get_all_ae_db_sql_text_by_1seq(df, chunksize=self.chunk_size)

                            grouping_df = MaxGaugeUtils.reconstruct_by_grouping(results)
                            grouping_df = DynamicSqlSearch.preprocessing(grouping_df)

                            if len(grouping_df) == 0:
                                continue

                            self._teardown_sa_target()

                            self.logger.info(f"Sql parsing Start... {self.n_cores} cpu cores")
                            grouping_df = self.parallelize_dataframe(
                                grouping_df, self._sql_metadata_parsing_by_df, n_cores=self.n_cores
                            )
                            # grouping_df = self._sql_metadata_parsing_by_df(grouping_df)
                            self.logger.info("Sql parsing End...")

                            grouping_df = grouping_df[grouping_df["where_token_len"] >= 0]
                            self._init_sa_target()

                            if len(grouping_df) == 0:
                                continue

                            if pq_writer is None:
                                pq_writer = pf.get_pqwriter(
                                    self.export_parquet_root_path, self.dynamic_sql_parquet_file_name, grouping_df
                                )

                            pf.make_parquet_by_df(grouping_df, pq_writer)
                            real_row_cnt += len(grouping_df)

                        self.logger.info(
                            f"Total export data count (date:{date}, db_id:{db_id}): "
                            f"{total_row_cnt} total rows / {real_row_cnt} analysis real rows"
                        )

        except Exception as e:
            self.logger.exception(e)

            if os.path.isfile(f"{self.export_parquet_root_path} {self.dynamic_sql_parquet_file_name}"):
                os.remove(f"{self.export_parquet_root_path} {self.dynamic_sql_parquet_file_name}")

            raise ModuleException("E011")

        finally:
            if pq_writer:
                pq_writer.close()

    def _get_df_by_parquet(self, parquet_files, column):
        """
        parquet files에서 df으로 변환하는 함수
        :param parquet_files: parquet files
        :param column: 추출하려는 컬럼
        :return: 추출된 데이터 프레임
        """
        result_df = None
        for parquet_file in parquet_files:
            df = pq.read_pandas(
                f"{self.export_parquet_root_path}/{parquet_file}",
                columns=[
                    column,
                ],
            ).to_pandas()

            result_df = pd.concat([result_df, df], ignore_index=True)

        return result_df

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
    def preprocessing(grouping_df):
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
        grouping_df["sql_text"] = grouping_df["sql_text"].str.replace(r"\( *\+ *\)", "", regex=True)
        grouping_df["sql_text"] = grouping_df["sql_text"].str.replace(r"--.*\n", r"\n", regex=True)
        grouping_df["sql_text"] = grouping_df["sql_text"].str.replace(r"--.*\r", r"\r", regex=True)
        grouping_df = grouping_df[~grouping_df["sql_text"].str.startswith(sql_filter)]
        return grouping_df

    @staticmethod
    def check_contains_where_col(parquet_data_dict: dict, obj_dict: dict):
        """
        where 절 컬럼을 포함 여부 확인 함수
        :param parquet_data_dict: 대상 where 컬럼 dict
        :param obj_dict: 다이나믹 sql where 컬럼 dict
        :return: 포함 여부
        """
        for key, value in obj_dict.items():
            if key not in parquet_data_dict or not all(col in parquet_data_dict[key] for col in value):
                return False

        return True

    @staticmethod
    def make_from_where_pattern(sql_text, where_idx):
        """
        from ~ where patter str 생성 함수
        :param sql_text: 주석 제거 sql text
        :param where_idx: where절 포지션
        :return: from ~ where str
        """
        parser = Parser(sql_text)

        where_token_list = DynamicSqlSearch.get_where_token_idx(parser)

        sql_str_list_by_from_position_tokens = [
            token for token in parser.tokens if token.position >= where_token_list[where_idx]
        ]

        sql_str_list_by_from_position = [
            f"\n{str(token)}"
            if token.is_keyword
            and (
                token.normalized == "WHERE"
                or token.normalized == "AND"
                or token.normalized == "SELECT"
                or token.normalized == "FROM"
                or token.normalized == "ORDERBY"
                or token.normalized == "GROUPBY"
            )
            else str(token)
            for token in sql_str_list_by_from_position_tokens
        ]

        return " ".join(sql_str_list_by_from_position)

    @staticmethod
    def get_where_token_idx(parser):
        """
        Where position 획득 함수
        :param parser: sql parser
        :return: 파싱된 position list
        """
        return [token.position for token in parser.tokens if token.is_keyword and token.normalized == "WHERE"]

    @staticmethod
    def analyze_token_type(token, obj, columns_aliases):
        """
        Token 타입별 분석 함수
        :param token: 분석 하려는 token
        :param obj: 분석된 token을 저장하려는 obj
        :param columns_aliases: parser column_aliases (파싱된 alias를 select column으로 보정하기 위해)
        """
        part_obj_key = None

        if token.token_type is TokenType.TABLE:
            part_obj_key = "table_name_list"

        elif token.token_type is TokenType.COLUMN and not token.value.strip().startswith(":"):
            if token.last_keyword == "SELECT":
                part_obj_key = "select_col_list"

            elif token.last_keyword == "WHERE":
                part_obj_key = "where_col_list"

            elif token.last_keyword == "ON" or token.last_keyword == "USING":
                part_obj_key = "join_col_list"

            else:
                part_obj_key = "etc_col_list"

        elif token.is_potential_column_name and "row" in token.value:
            part_obj_key = DynamicSqlSearch.get_part_obj_by_keyword(token)

        elif (
            columns_aliases is not None
            and token.is_potential_alias
            and token.is_potential_column_name
            and token.value in columns_aliases.keys()
        ):
            part_obj_key = DynamicSqlSearch.get_part_obj_by_keyword(token)

        elif token.is_potential_column_name and token.is_wildcard:
            part_obj_key = DynamicSqlSearch.get_part_obj_by_keyword(token)

        elif token.token_type is TokenType.COLUMN_ALIAS:
            part_obj_key = DynamicSqlSearch.get_part_obj_by_keyword(token)

        if part_obj_key is not None:
            DynamicSqlSearch.set_token_value_by_part(token, obj, part_obj_key)

    @staticmethod
    def get_part_obj_key_by_keyword(token):
        """
        token last_keyword로 part_obj_key를 정하는 함수
        :param token: 분석된 token
        :return: part_obj_key
        """
        if token.last_keyword == "SELECT":
            part_obj_key = "select_col_list"

        elif token.last_keyword == "WHERE":
            part_obj_key = "where_col_list"

        return part_obj_key

    @staticmethod
    def set_token_value_by_part(token, part_obj, part_obj_key):
        """
        각 부분별 token값 세팅 함수
        :param token: 분석 token
        :param part_obj: 값 세팅 하려는 obj
        :param part_obj_key: 각 부분을 구별하는 part key
        """
        if f"{token.parenthesis_level}" not in part_obj[part_obj_key]:
            part_obj[part_obj_key][f"{token.parenthesis_level}"] = []

        part_obj[part_obj_key][f"{token.parenthesis_level}"].append(token.value)

    @staticmethod
    def compare_dicts(parquet_data_dict, obj_dict):
        """
        dict 값 비교 함수
        :param parquet_data_dict: parquet에 저장된 데이터 dict
        :param obj_dict: 원본 sql 분석 dict
        :return: 일치 결과 (bool)
        """
        if len(parquet_data_dict) != len(obj_dict):
            return False

        for key, value in parquet_data_dict.items():
            if key not in obj_dict or obj_dict[key] != value:
                return False

        return True

    @staticmethod
    def extract_first_key_and_value_in_obj(sql_fixed_part, part_key):
        """
        sql 고정 부분으로 분석된 데이터의 첫번째 키와 첫번째 값을 추출하기 위한 함수
        :param sql_fixed_part: 분석된 고정 부분 데이터
        :param part_key: 추출하려는 부분 key 값
        :return: 추출된 key (depth), value
        """
        key = "-1"
        value = "-1"
        if sql_fixed_part.get(part_key, {}):
            key = next(iter(sql_fixed_part.get(part_key, {})))

            if sql_fixed_part.get(part_key, {}).get(key):
                value = sql_fixed_part.get(part_key, {}).get(key, [])[0]
                # value_cnt = len(sql_fixed_part.get(part_key, {}).get(key))

        return key, value

    @staticmethod
    def analyze_each_idx(idx, sql_fixed_parts, sql_dynamic_parts, parser, where_token_idxs):
        """
        각 where idx 별로 분석하려는 함수
        :param idx: where idx
        :param sql_fixed_parts: sql fixed 부분
        :param sql_dynamic_parts: sql dynamic 부분
        :param parser: parser obj
        :param where_token_idxs: where token idx list
        """
        query_part_obj = AnalyzedQueryPartObj()

        sql_fixed_part = query_part_obj.sql_fixed_part
        sql_dynamic_part = query_part_obj.sql_dynamic_part

        for token in parser.tokens:
            if len(where_token_idxs) == 0 or token.position < where_token_idxs[idx]:
                DynamicSqlSearch.analyze_token_type(token, sql_fixed_part, parser.columns_aliases)
            else:
                DynamicSqlSearch.analyze_token_type(token, sql_dynamic_part, parser.columns_aliases)

        sql_fixed_parts[f"{idx}"] = sql_fixed_part
        sql_dynamic_parts[f"{idx}"] = sql_dynamic_part

    @staticmethod
    def extract_ratio_metric_column_idx(df, ratio_metrics):
        """
        비율을 구하는 지표들의 컬럼 인덱스를 추출하는 함수
        :param df: 대상 데이터프레임
        :param ratio_metrics: 비율을 구하려는 지표
        :return: 추출된 컬럼 인덱스
        """
        target_columns = []

        for ratio_metric in ratio_metrics:
            target_columns.append(df.columns.get_loc(ratio_metric) + 1)

        return target_columns

    @staticmethod
    def rename_export_excel_data_df(export_excel_data_df):
        """
        엑셀 추출시 표시되는 컬럼명 변경을 위한 함수
        :param export_excel_data_df: 엑셀 추출 데이터 프레임
        :return: 변경된 데이터 프레임
        """
        export_excel_data_df = export_excel_data_df.rename(
            columns={
                "ratio_elapsed_time": "Elapsed Time (%)",
                "ratio_cpu_time": "CPU Time (%)",
                "ratio_lio": "Logical Reads (%)",
                "ratio_pio": "Physical Reads (%)",
                "exec": "Executions",
                "sum_elapsed_time": "Elapsed Time (Sec)",
                "sum_cpu_time": "CPU Time (Sec)",
                "sum_lio": "Logical Reads (blocks)",
                "sum_pio": "Physical Reads (blocks)",
                "avg_elapsed_time": "Elapsed Time/exec (Sec)",
                "avg_cpu_time": "CPU Time/exec (Sec)",
                "avg_lio": "Logical Reads/exec (blocks)",
                "avg_pio": "Physical Reads/exec (blocks)",
            },
        )
        return export_excel_data_df

    @staticmethod
    def calc_ratio_each_metric(dynamic_source_sql, sum_metrics):
        """
        sum_metric으로 ratio_metric 구하기
        :param dynamic_source_sql: source sql object
        :param sum_metrics: sum_ 으로 시작되는 metrics
        """
        ratio_metrics = []
        for sum_metric in sum_metrics:
            ratio_metric = sum_metric.replace("sum", "ratio")
            dynamic_source_sql.valid_sql_df[ratio_metric] = dynamic_source_sql.valid_sql_df[sum_metric].div(
                dynamic_source_sql.valid_sql_df[sum_metric].sum(), fill_value=0
            )

            ratio_metrics.append(ratio_metric)

        dynamic_source_sql.valid_sql_df[ratio_metrics] = dynamic_source_sql.valid_sql_df[ratio_metrics].fillna(0)
        dynamic_source_sql.valid_sql_df[ratio_metrics] = dynamic_source_sql.valid_sql_df[ratio_metrics].round(3)

    @staticmethod
    def make_export_excel_target_data_by_df(tmp_df):
        """
        target sql 데이터 엑셀 추출을 위한 변환 함수
        :param tmp_df: DB에 저장한 유효한 target sql 데이터 프레임
        :return: 추출된 데이터 프레임
        """
        export_excel_data_df = tmp_df[
            [
                "target_sql_uid",
                "target_sql_id",
                "target_where_cols",
                "dynamic_pattern",
                "ratio_elapsed_time",
                "ratio_cpu_time",
                "ratio_lio",
                "ratio_pio",
                "exec",
                "sum_elapsed_time",
                "sum_cpu_time",
                "sum_lio",
                "sum_pio",
                "avg_elapsed_time",
                "avg_cpu_time",
                "avg_lio",
                "avg_pio",
            ]
        ]

        export_excel_data_df.sort_values(by=["ratio_elapsed_time"], axis=0, ascending=False, inplace=True)
        return export_excel_data_df

    @staticmethod
    def get_sql_metadata_obj(sql_text):
        """
        sql_metadata 라이브러리 Parser obj 생성 함수
        :param sql_text: 파싱하려는 sql text
        :return: Parser 객체
        :exception: False (bool)
        """
        try:
            parser = Parser(sql_text)
            parser.columns
            parser.tables
        except Exception:
            return False
        return parser

    @staticmethod
    def make_debug_value_by_df(df, target_column):
        """
        데이터 프레임 target_column에 "" 값을 제거 하기 위한 함수
        :param df: 변환하려는 데이터 프레임
        :param target_column: 타겟 컬럼
        :return: 제거된 값 리스트
        """
        value_list = df[target_column].apply(DynamicSqlSearch.make_debug_value).values.tolist()
        return [value for value in value_list if value != ""]

    @staticmethod
    def make_debug_value(x):
        """
        sql 조회 debug용 값을 만들어 주기 위한 함수
        :param x: sql where 절 str value
        :return: 변환된 str
        """
        if x != "":
            x = "'" + x + "'"
        return x
