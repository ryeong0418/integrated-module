import pandas as pd
import numpy as np
import psycopg2 as db
import psycopg2.extras
import sys

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from psycopg2 import errors
from psycopg2.errorcodes import DUPLICATE_TABLE

from datetime import datetime, timedelta

from src.decoder.intermax_decryption import Decoding
from src.common.utils import TargetUtils, SystemUtils, InterMaxUtils, MaxGaugeUtils, SqlUtils
from src.common.constants import TableConstants, SystemConstants
from src.common.enum_module import ModuleFactoryEnum
from src.common.timelogger import TimeLogger
from sql.common_sql import CommonSql, AeWasSqlTextSql, AeDbSqlTemplateMapSql, AeDbInfoSql, AeDbSqlTextSql
from sql.common_sql import AeWasDevMapSql, XapmTxnSqlDetail


class CommonTarget:

    def __init__(self, logger, config):
        self.logger = logger
        self.config = config

        self.analysis_conn_str = TargetUtils.get_db_conn_str(config['analysis_repo'])
        self.im_conn_str = TargetUtils.get_db_conn_str(config['intermax_repo'])
        self.mg_conn_str = TargetUtils.get_db_conn_str(config['maxgauge_repo'])

        self.analysis_engine_template = TargetUtils.get_engine_template(config['analysis_repo'])
        self.im_engine_template = TargetUtils.get_engine_template(config['intermax_repo'])
        self.mg_engine_template = TargetUtils.get_engine_template(config['maxgauge_repo'])

        self.analysis_engine = None
        self.im_engine = None
        self.mg_engine = None

        self.im_conn = None
        self.mg_conn = None
        self.sa_conn = None

        self.sa_cursor = None

        self.data_handling_chunksize = self.config.get('data_handling_chunksize', 10_000)
        self.extract_chunksize = self.data_handling_chunksize * 5
        self.sql_match_time = self.config.get('sql_match_time', 0)
        self.sql_file_root_path = f"{self.config['home']}/" \
                                  f"{SystemConstants.SQL}/" \
                                  f"{ModuleFactoryEnum[self.config['args']['proc']].value}"

        self.update_cluster_cnt = 0
        self.intermax_decoder = None
        self.sql_debug_flag = self.config.get('sql_debug_flag', False)

    def __del__(self):
        if self.im_conn:
            self.im_conn.close()
        if self.sa_conn:
            self.sa_conn.close()
        if self.mg_conn:
            self.mg_conn.close()
        if self.analysis_engine:
            self.analysis_engine.dispose()
        if self.im_engine:
            self.im_engine.dispose()
        if self.mg_engine:
            self.mg_engine.dispose()

    def _execute_insert_meta(self, query, table_name, target_conn):
        """
        InterMax, MaxGauge 메타 데이터를 분석 DB에 truncate후 insert하는 기능

        :param query : 메타 데이터 query문
        :param target_conn : InterMax, MaxGauge DB connection 정보
        """
        replace_dict = {'table_name': table_name}
        delete_table_query = SqlUtils.sql_replace_to_dict(CommonSql.TRUNCATE_TABLE_DEFAULT_QUERY, replace_dict)
        self._default_execute_query(self.sa_conn, delete_table_query)
        meta_df = self._get_target_data_by_query(target_conn, query, table_name, )
        self._insert_engine_by_df(self.analysis_engine, table_name, meta_df)

    def _create_engine(self, engine_template):
        self.logger.info(f"Create engine info : {engine_template}")
        return create_engine(
            engine_template,
            echo=self.sql_debug_flag,
            pool_size=20,
            max_overflow=20,
            echo_pool=False,
            pool_pre_ping=True,
            connect_args={
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
            },
        )

    def _get_target_data_by_query(self, target_conn, query, table_name="UNKNOWN TABLE"):
        """
        각 분석 대상의 DB에서 query 결과를 DataFrame 담아오는 함수
        :param target_conn: 각 타겟 connect object
        :param query: 각 타겟 호출 SQL
        :param table_name: 분석 모듈 DB에 저장될 테이블 명 (for logging)
        :return: 각 타겟의 query 결과 정보 (DataFrame)
        """

        with TimeLogger(f"[{CommonTarget.__name__}] {sys._getframe(0).f_code.co_name}(), {table_name} to extract ",
                        self.logger):
            df = pd.read_sql(query, target_conn)

        return df

    def _insert_engine_by_df(self, engine, table_name, df):
        """
        분석 모듈 DB에 DataFrame의 데이터 저장 함수
        :param engine: 저장 하려는 타겟 SqlAlchemy engine
        :param table_name: 저장 하려는 DB 테이블
        :param df: 저장하려는 DataFrame
        :return:
        """
        with TimeLogger(f"[{CommonTarget.__name__}] {sys._getframe(0).f_code.co_name}(), {table_name} to import ",
                        self.logger):
            df.to_sql(
                name=table_name,
                con=engine,
                schema='public',
                if_exists='append',
                index=False
            )
        return df

    def _default_execute_query(self, conn, query):
        """
        분석 모듈 DB 기본 sql 실행 쿼리
        :param conn: sql 실행하려는 타겟 DB Connection Object
        :param query: 실행 하려는 쿼리
        :return:
        """
        try:
            cursor = conn.cursor()
            cursor.execute(query)

        except errors.lookup(DUPLICATE_TABLE):
            self.logger.warn("This DDL Query DUPLICATE_TABLE.. SKIP")

        except Exception as e:
            self.logger.exception(e)
        finally:
            conn.commit()
            cursor.close()

    def _psql_insert_copy(self, table, engine, df):

        if not df.empty:
            self.logger.info(f"{table}  upsert data")

            metadata = MetaData()
            t = Table(table, metadata, autoload_with=engine)
            insert_values = df.replace({np.nan: None}).to_dict(orient='records')

            insert_stmt = insert(t).values(insert_values)
            update_stmt = {exc_k.key: exc_k for exc_k in insert_stmt.excluded}

            upsert_values = insert_stmt.on_conflict_do_update(
                index_elements=t.primary_key,
                set_=update_stmt
            ).returning(t)

            with engine.connect() as connection:
                connection.execute(upsert_values)
                connection.commit()

    def _get_df_by_chunk_size(self, engine: create_engine, query: str, chunk_size: int = 0, coerce=True):
        if chunk_size == 0:
            chunk_size = self.extract_chunksize
        conn = engine.connect().execution_options(stream_results=True)
        return pd.read_sql_query(text(query), conn, chunksize=chunk_size, coerce_float=coerce)


class InterMaxTarget(CommonTarget):

    def init_process(self):
        self.im_conn = db.connect(self.im_conn_str)
        self.sa_conn = db.connect(self.analysis_conn_str)
        self.im_engine = self._create_engine(self.im_engine_template)
        self.analysis_engine = self._create_engine(self.analysis_engine_template)

    def insert_intermax_meta_data(self):
        """
        InterMax 메타 데이터 테이블 query문 불러와 insert 또는 upsert 기능 함수
        upsert 실행 테이블 : ae_was_dev_map, ae_txn_name
        insert 실행 테이블 : ae_was_db_info, ae_host_info, ae_was_info
        """
        extractor_file_path = f"{self.sql_file_root_path}/was/meta/"
        extractor_files = SystemUtils.get_filenames_from_path(extractor_file_path)

        for extractor_file in extractor_files:

            with open(f"{extractor_file_path}{extractor_file}", mode='r', encoding='utf-8') as file:
                query = file.read()

            table_name = SystemUtils.extract_tablename_in_filename(extractor_file)

            if table_name == "ae_was_dev_map" or table_name == "ae_txn_name":
                self._excute_upsert_intermax_data(query, table_name)

            else:
                self._execute_insert_meta(query, table_name, self.im_conn)

    def insert_intermax_detail_data(self):
        """
        InterMax 분석 데이터 테이블 query를 날짜 별로 불러와 읽은 후, delete -> insert 또는 upsert 기능 함수

        upsert 실행 테이블: ae_was_sql_text
        delete -> insert (기본) 실행 테이블: ae_bind_sql_elapse, ae_was_os_stat_osm
        delete -> insert (ae_was_dev_map 테이블에서 was_id 제외) 실행 테이블: ae_jvm_stat_summary,ae_txn_detail,
                                                        ae_txn_sql_detail, ae_txn_sql_fetch, ae_was_stat_summary
        """
        extractor_file_path = f"{self.sql_file_root_path}/was/"
        extractor_files = SystemUtils.get_filenames_from_path(extractor_file_path, '', 'txt')

        delete_query = CommonSql.DELETE_TABLE_BY_DATE_QUERY

        date_conditions = InterMaxUtils.set_intermax_date(self.config['args']['s_date'],
                                                          self.config['args']['interval'])

        for extractor_file in extractor_files:

            with open(f"{extractor_file_path}{extractor_file}", mode='r', encoding='utf-8') as file:
                query = file.read()

            table_name = SystemUtils.extract_tablename_in_filename(extractor_file)

            for date in date_conditions:

                table_suffix_dict = {'table_suffix': date}
                detail_query = SqlUtils.sql_replace_to_dict(query, table_suffix_dict)
                delete_dict = {'table_name': table_name, 'date': date}

                try:

                    if table_name == "ae_was_sql_text":
                        self._excute_upsert_intermax_data(detail_query, table_name)

                    else:
                        im_delete_query = SqlUtils.sql_replace_to_dict(delete_query, delete_dict)
                        self.logger.info(f"delete query execute : {im_delete_query}")
                        self._default_execute_query(self.sa_conn, im_delete_query)

                        if table_name == "ae_bind_sql_elapse":
                            self._excute_insert_bind_value_date(detail_query, table_name)

                        elif table_name == "ae_was_os_stat_osm":
                            self._execute_insert_intermax_detail_data(detail_query, table_name)

                        else:
                            ae_dev_map_df = self._get_target_data_by_query(self.sa_conn,
                                                                                 AeWasDevMapSql.SELECT_AE_WAS_DEV_MAP,
                                                                                 table_name)
                            self._excute_insert_dev_except_data(detail_query, table_name, ae_dev_map_df)

                except Exception as e:
                    self.logger.exception(f"{table_name} table, {date} date detail data insert error")
                    self.logger.exception(e)

    def _get_intermax_data_by_chunksize(self, query):
        """
        InterMax SQL query문을 chunksize만큼 dataframe 형태로 읽어서 반환
        """
        return self._get_df_by_chunk_size(self.im_engine, query)

    def _excute_upsert_intermax_data(self, query, table_name):
        """
        분석 모듈 DB에 data upsert 기능 함수
        :param query: txt파일에 입력된 query문

        실행 테이블: (메타 테이블) ae_was_dev_map, ae_txn_name
                   (InterMax 테이블) ae_was_sql_text
        """
        for query_df in self._get_intermax_data_by_chunksize(query):
            df = InterMaxUtils.meta_table_value(table_name, query_df)
            self._psql_insert_copy(table_name, self.analysis_engine, df)

    def _excute_insert_bind_value_date(self,query,table_name):

        """
        분석 모듈 DB에 data insert 기능 함수
        :param query: txt파일에 입력된 query문

        실행 테이블: (InterMax 테이블) ae_bind_sql_elapse

        bind_list 컬럼값을 복호화하여 bind_value 컬럼에 insert
        """
        for df in self._get_intermax_data_by_chunksize(query):

            if self.config['extract_bind_value']:

                if self.intermax_decoder is None:
                    self.intermax_decoder = Decoding(self.config)
                    self.intermax_decoder.set_path()

                df['bind_value'] = df['bind_list'].apply(
                    self.intermax_decoder.execute_bind_list_decoding
                )
                df['bind_value'] = df['bind_value'].astype(str)

            self._insert_engine_by_df(self.analysis_engine, table_name, df)

    def _execute_insert_intermax_detail_data(self, query, table_name):

        """
        분석 모듈 DB에 data insert 기능 함수
        :param query: txt파일에 입력된 query문

        실행 테이블: (메타 테이블) ae_was_db_info, ae_host_info, ae_was_info,
                   (InterMax 테이블)  ae_was_os_stat_osm
        """

        for df in self._get_intermax_data_by_chunksize(query):
            self._insert_engine_by_df(self.analysis_engine, table_name, df)

    def _excute_insert_dev_except_data(self, query, table_name,ae_dev_map_df):
        """
        분석 모듈 DB ae_was_dev_map 테이블에서 was_id에 해당하는 값들을 제외하고 data insert 기능 함수
        :param query: txt파일에 입력된 query문

        실행 테이블: (WAS 테이블) ae_jvm_stat_summary,ae_txn_detail, ae_txn_sql_detail, ae_txn_sql_fetch, ae_was_stat_summary
        """

        for detail_df in self._get_intermax_data_by_chunksize(query):
            was_id_except_df = detail_df[~detail_df['was_id'].isin(ae_dev_map_df['was_id'])]
            self._insert_engine_by_df(self.analysis_engine, table_name, was_id_except_df)

    def get_xapm_txn_sql_detail(self, start_param, end_param):
        param_dict = {'start_param': start_param, 'end_param': end_param}
        query = SqlUtils.sql_replace_to_dict(XapmTxnSqlDetail.SELECT_XAPM_TXN_SQL_DETAIL, param_dict)

        return self._get_target_data_by_query(self.im_conn, query, "XAPM_SQL_SUMMARY")


class MaxGaugeTarget(CommonTarget):

    def init_process(self):
        self.mg_conn = db.connect(self.mg_conn_str)
        self.sa_conn = db.connect(self.analysis_conn_str)
        self.analysis_engine = self._create_engine(self.analysis_engine_template)
        self.mg_engine = self._create_engine(self.mg_engine_template)

    def insert_maxgauge_meta_data(self):
        """
        MaxGauge 메타 데이터 테이블 query문 불러와 insert 기능 함수
        insert 실행 테이블: ae_db_info
        """
        extractor_file_path = f"{self.sql_file_root_path}/db/meta/"
        extractor_files = SystemUtils.get_filenames_from_path(extractor_file_path)

        for extractor_file in extractor_files:
            with open(f"{extractor_file_path}{extractor_file}", mode='r', encoding='utf-8') as file:
                query = file.read()

            table_name = SystemUtils.extract_tablename_in_filename(extractor_file)
            self._execute_insert_meta(query, table_name, self.mg_conn)

    def insert_maxgauge_detail_data(self):
        """
        MaxGauge 메타 데이터 테이블 query를 날짜 별로 불러와 읽은 후, delete -> insert 기능 함수
        """
        date_conditions = MaxGaugeUtils.set_maxgauge_date(self.config['args']['s_date'],
                                                          self.config['args']['interval'])
        ae_db_info_query = AeDbInfoSql.SELECT_AE_DB_INFO

        ae_db_info_name = TableConstants.AE_DB_INFO
        db_info_df = self._get_target_data_by_query(self.sa_conn, ae_db_info_query, ae_db_info_name)

        delete_query = CommonSql.DELETE_TABLE_BY_PARTITION_KEY_QUERY
        extractor_file_path = f"{self.sql_file_root_path}/db/"
        extractor_files = SystemUtils.get_filenames_from_path(extractor_file_path, '', 'txt')

        for extractor_file in extractor_files:

            with open(f"{extractor_file_path}{extractor_file}", mode='r', encoding='utf-8') as file:
                query = file.read()

            table_name = SystemUtils.extract_tablename_in_filename(extractor_file)

            for _, row in db_info_df.iterrows():
                db_id = str(row["db_id"]).zfill(3)
                instance_name = str(row["instance_name"])

                for date in date_conditions:
                    table_suffix_dict = {'instance_name': instance_name, 'partition_key': date + db_id}
                    delete_suffix_dict = {'table_name': table_name, 'partition_key': date + db_id}

                    try:
                        mg_delete_query = SqlUtils.sql_replace_to_dict(delete_query, delete_suffix_dict)
                        self.logger.info(f"delete query execute : {mg_delete_query}")
                        self._default_execute_query(self.sa_conn, mg_delete_query)

                        detail_query = SqlUtils.sql_replace_to_dict(query, table_suffix_dict)
                        self._execute_insert_maxgauge_detail_data(detail_query, table_name)

                    except Exception as e:
                        self.logger.exception(f"{table_name} table, {date} date detail data insert error")
                        self.logger.exception(e)

    def _execute_insert_maxgauge_detail_data(self, query, table_name):
        """
        분석 모듈 DB에 data insert 기능 함수
        :param query: txt파일에 입력된 query문
        실행 테이블: ae_db_sql_text, ae_session_info, ae_session_stat, ae_sql_stat_10min, ae_sql_wait_10min
        """
        get_read_sql_query = self._get_df_by_chunk_size(self.mg_engine, query)

        for df in get_read_sql_query:
            self._insert_engine_by_df(self.analysis_engine, table_name, df)


class SaTarget(CommonTarget):

    def init_process(self):
        self.sa_conn = db.connect(self.analysis_conn_str)
        self.im_conn = db.connect(self.im_conn_str)
        self.analysis_engine = self._create_engine(self.analysis_engine_template)
        self.sa_cursor = self.sa_conn.cursor()

    def create_table(self):
        init_path = f"{self.sql_file_root_path}/{SystemConstants.DDL}/"
        init_files = SystemUtils.get_filenames_from_path(init_path)

        for init_file in init_files:
            with open(f"{init_path}{init_file}", mode='r', encoding='utf-8') as file:
                ddl = file.read()

            self._default_execute_query(self.sa_conn, ddl)

    def get_ae_was_sql_text(self, extract_cnt=0):
        query = AeWasSqlTextSql.SELECT_AE_WAS_SQL_TEXT
        table_name = TableConstants.AE_WAS_SQL_TEXT

        if extract_cnt > 0:
            query += f" limit {extract_cnt}"

        try:
            result_df = self._get_target_data_by_query(self.sa_conn, query, table_name)
        except Exception as e:
            self.logger.exception(e)
        return result_df

    def ae_was_sql_text_meta(self):
        """
        InterMax DB xapm_sql_text 테이블에서 데이터 추출하여 분석 DB ae_was_sql_text 테이블에 insert 기능 함수
        """
        init_path = f"{self.sql_file_root_path}/{SystemConstants.META}/"
        init_files = SystemUtils.get_filenames_from_path(init_path)

        for init_file in init_files:
            with open(f"{init_path}{init_file}", mode='r', encoding='utf-8') as file:
                meta_query = file.read()

            table_name = SystemUtils.extract_tablename_in_filename(init_file)
            self._execute_insert_meta(meta_query, table_name, self.im_conn)

    def get_ae_db_sql_text_by_1seq(self, partition_key, chunksize):
        replace_dict = {'partition_key': partition_key}
        query = SqlUtils.sql_replace_to_dict(AeDbSqlTextSql.SELECT_AE_DB_SQL_TEXT_1SEQ, replace_dict)

        return pd.read_sql_query(query, self.sa_conn, chunksize=chunksize)

    def get_all_ae_db_sql_text_by_1seq(self, df, chunksize):
        query_with_data = AeDbSqlTextSql.SELECT_AE_DB_SQL_TEXT_WITH_DATA

        params = tuple(df.itertuples(index=False, name=None))

        psycopg2.extras.execute_values(self.sa_cursor, query_with_data, params, page_size=chunksize)
        results = self.sa_cursor.fetchall()

        return results

    def excute_summarizer(self):
        """
        분석 모듈 DB에 data summarizer 기능 함수
        ae_txn_detail_summary_temp, ae_txn_sql_detail_summary_temp 에 1일치씩 데이터 delete -> insert 실행
        날짜 별로 두 개의 temp table을 join하여 ae_txn_sql_summary에 데이터 insert
        """

        date_conditions = InterMaxUtils.set_intermax_date(self.config['args']['s_date'], self.config['args']['interval'])

        summarizer_temp_path = f"{self.sql_file_root_path}/temp/"
        summarizer_temp_files = SystemUtils.get_filenames_from_path(summarizer_temp_path)

        for date in date_conditions:
            start_date, end_date = SaTarget.summarizer_set_date(date)
            date_dict = {'StartDate': start_date, 'EndDate': end_date}

            for summarizer_temp_file in summarizer_temp_files:

                table_name = summarizer_temp_file.split('.')[0]

                replace_dict = {'table_name': table_name}
                delete_table_query = SqlUtils.sql_replace_to_dict(CommonSql.DELETE_TABLE_DEFAULT_QUERY,
                                                                     replace_dict)

                with open(f"{summarizer_temp_path}{summarizer_temp_file}", mode='r', encoding='utf-8') as file:
                    query = file.read()
                    temp_query = SqlUtils.sql_replace_to_dict(query, date_dict)

                    try:
                        self._default_execute_query(self.sa_conn, delete_table_query)
                        temp_df = self._get_target_data_by_query(self.sa_conn, temp_query, table_name)
                        self._insert_engine_by_df(self.analysis_engine, table_name, temp_df)

                    except Exception as e:
                        self.logger.exception(
                            f"{summarizer_temp_file.split('.')[0]} table, create_temp_table execute error")
                        self.logger.exception(e)

            self.summary_join(date_dict)

    @staticmethod
    def summarizer_set_date(input_date):
        start_date = datetime.strptime(input_date, '%Y%m%d')
        end_date = start_date + timedelta(days=1)
        start_date = start_date.strftime('%Y-%m-%d 00:00:00')
        end_date = end_date.strftime('%Y-%m-%d 00:00:00')
        return start_date, end_date

    def summary_join(self, date_dict):
        """
        ae_txn_detail_summary_temp, ae_txn_sql_detail_summary_temp 테이블 조인 기능 함수
        날짜 별로 delete -> join data insert 기능 수행
        """

        summary_path = f"{self.sql_file_root_path}/summary/"
        summary_files = SystemUtils.get_filenames_from_path(summary_path)

        delete_query = CommonSql.DELETE_SUMMARY_TABLE_BY_DATE_QUERY

        for summary_file in summary_files:

            with open(f"{summary_path}{summary_file}", mode='r', encoding='utf-8') as file:
                query = file.read()

                join_query = SqlUtils.sql_replace_to_dict(query, date_dict)
                table_name = summary_file.split('.')[0]
                datetime_format = datetime.strptime(date_dict['StartDate'].split()[0], '%Y-%m-%d')
                formatted_date = datetime_format.strftime('%Y%m%d')
                delete_dict = {'table_name': table_name, 'date': formatted_date}

                try:
                    sa_delete_query = SqlUtils.sql_replace_to_dict(delete_query, delete_dict)
                    self._default_execute_query(self.sa_conn, sa_delete_query)
                    self.logger.info(f"delete query execute : {sa_delete_query}")

                    for df in self._get_df_by_chunk_size(self.analysis_engine, join_query):
                        self._insert_engine_by_df(self.analysis_engine, table_name, df)

                except Exception as e:
                    self.logger.exception(f"{summary_file.split('.')[0]} table, summary insert execute error")
                    self.logger.exception(e)

    def sql_query_convert_df(self, sql_query):
        table_name = TableConstants.AE_TXN_SQL_SUMMARY
        df = self._get_target_data_by_query(self.sa_conn, sql_query, table_name)

        return df

    def get_maxgauge_date_conditions(self, ):
        return MaxGaugeUtils.set_maxgauge_date(self.config['args']['s_date'], self.config['args']['interval'])

    def insert_merged_result(self, merged_df):
        table_name = TableConstants.AE_SQL_TEXT
        total_len = 0

        for i in range(len(merged_df)):
            try:
                merged_df.iloc[i:i + 1].to_sql(table_name, if_exists='append', con=self.analysis_engine,
                                               schema='public', index=False)
                total_len += 1
            except IntegrityError:
                pass
            except Exception as e:
                self.logger.exception(e)

        self.logger.info(f"Matching Data except duplicate, Insert rows : {total_len}")

    def get_ae_db_info(self):
        query = AeDbInfoSql.SELECT_AE_DB_INFO
        table_name = TableConstants.AE_DB_INFO

        df = self._get_target_data_by_query(self.sa_conn, query, table_name, )
        df['lpad_db_id'] = df['db_id'].astype('str').str.pad(3, side='left', fillchar='0')
        return df

    def get_table_data_query(self, table):
        """
        분석 모듈 DB Dump시 parquet export 할 테이블의 쿼리를 만드는 함수
        parquet export 시 데이터의 타입을 체크하는데 export 할 데이터를 가져올때 int 타입 컬럼의 모든 값이 Nan일 경우 select 시
        float 타입으로 변경된다. 따라서 Nan일 경우를 대비해 DB에서 select 시 coalesce 함수를 씌어서 0으로 치환하기 위한 쿼리로 변경한다.
        timestamp가 Nan일 경우 parquet 파일 변환 시 빈값이고 DB에 Insert시 timestamp 타입에 빈값을 넣으면서 오류가 나서
        timestamp가 빈값이면 now()로 데이터를 치환 시킨다.
        :param table: export할 테이블 str
        :return: select query
        """
        query = SqlUtils.sql_replace_to_dict(CommonSql.SELECT_TABLE_COLUMN_TYPE, {'table': table})

        cols = pd.read_sql(query, self.sa_conn)

        num_col_type_list = ['int', 'double', 'float']
        time_col_type_list = ['time']
        sel_list = []

        for _, row in cols.iterrows():
            if any(num_str in row['data_type'] for num_str in num_col_type_list):
                sel_list.append(f"coalesce({row['column_name']}, 0) as {row['column_name']}")
            elif any(num_str in row['data_type'] for num_str in time_col_type_list):
                sel_list.append(f"coalesce({row['column_name']}, now()) as {row['column_name']}")
            else:
                sel_list.append(row['column_name'])

        sel = ",".join(sel_list)

        return f"SELECT {sel} FROM {table}"

    def get_table_data_by_chunksize(self, query, chunksize, coerce=True):
        return self._get_df_by_chunk_size(self.analysis_engine, query, chunksize, bool(coerce))

    def insert_target_table_by_dump(self, table, df):
        self._insert_engine_by_df(self.analysis_engine, table, df)

    def term_extract_sql_text(self, chunksize):

        s_date = datetime.strptime(str(self.config['args']['s_date']), '%Y%m%d')
        e_date = s_date + timedelta(days=int(self.config['args']['interval']))

        date_dict = {'StartDate': str(s_date), 'EndDate': str(e_date), 'seconds': str(self.sql_match_time)}
        query = AeWasSqlTextSql.SELECT_SQL_ID_AND_SQL_TEXT
        sql_id_and_sql_text = SqlUtils.sql_replace_to_dict(query, date_dict)

        return self._get_df_by_chunk_size(self.analysis_engine, sql_id_and_sql_text, chunksize,)

    def update_cluster_id_by_sql_id(self, df):
        self.logger.info(f"Execute update_cluster_id_by_sql_id query total : {len(df)}")
        self.update_cluster_cnt = 0

        [self._update_cluster_id_by_sql_id(row) for _, row in df.iterrows()]
        self.logger.info(f"Execute update_cluster_id_by_sql_id query end : {self.update_cluster_cnt}")

    def _update_cluster_id_by_sql_id(self, row):
        query = AeWasSqlTextSql.UPDATE_CLUSTER_ID_BY_SQL_ID

        try:
            exec_query = SqlUtils.sql_replace_to_dict(query,
                                                         {'cluster_id': row['cluster_id'], 'sql_id': row['sql_id']}
                                                         )
            self._default_execute_query(self.sa_conn, exec_query)

            self.update_cluster_cnt += 1
        except IntegrityError as ie:
            self.logger.exception(f"_update_cluster_id_by_sql_id(), update execute error. check sql_id {row['sql_id']}")
            self.logger.exception(ie)
            pass
        except Exception as e:
            self.logger.exception(f"_update_cluster_id_by_sql_id(), update execute error. check sql_id {row['sql_id']}")
            self.logger.exception(e)

    def upsert_cluster_id_by_sql_uid(self, df):
        self.logger.info(f"Execute upsert_cluster_id_by_sql_uid query total : {len(df)}")
        self.update_cluster_cnt = 0

        [self._upsert_cluster_id_by_sql_uid(row) for _, row in df.iterrows()]
        self.logger.info(f"Execute upsert_cluster_id_by_sql_uid query end : {self.update_cluster_cnt}")

    def _upsert_cluster_id_by_sql_uid(self, row):
        query = AeDbSqlTemplateMapSql.UPSERT_CLUSTER_ID_BY_SQL_UID

        try:
            exec_query = SqlUtils.sql_replace_to_dict(query,
                                                         {'cluster_id': row['cluster_id'], 'sql_uid': row['sql_uid']}
                                                         )
            self._default_execute_query(self.sa_conn, exec_query)

            self.update_cluster_cnt += 1
        except IntegrityError as ie:
            self.logger.exception(
                f"_update_cluster_id_by_sql_id(), update execute error. check sql_id {row['sql_uid']}")
            self.logger.exception(ie)
            pass
        except Exception as e:
            self.logger.exception(
                f"_update_cluster_id_by_sql_id(), update execute error. check sql_id {row['sql_uid']}")
            self.logger.exception(e)

    def insert_ae_sql_template(self, df):
        table_name = TableConstants.AE_SQL_TEMPLATE
        truncate_query = CommonSql.TRUNCATE_TABLE_DEFAULT_QUERY
        truncate_query = SqlUtils.sql_replace_to_dict(truncate_query, {'table_name': table_name})

        self._default_execute_query(self.sa_conn, truncate_query)
        self._insert_engine_by_df(self.analysis_engine, table_name, df)

    def get_cluster_cnt_by_grouping(self, extract_cnt):
        if self.config['intermax_repo']['use']:
            query = AeWasSqlTextSql.SELECT_CLUSTER_CNT_BY_GROUPING
            table_name = TableConstants.AE_WAS_SQL_TEXT

        elif self.config['maxgauge_repo']['use']:
            query = AeDbSqlTemplateMapSql.SELECT_CLUSTER_CNT_BY_GROUPING
            table_name = TableConstants.AE_DB_SQL_TEMPLATE_MAP

        if extract_cnt > 0:
            query += f"limit {extract_cnt}"

        try:
            result_df = self._get_target_data_by_query(self.sa_conn, query, table_name)
        except Exception as e:
            self.logger.exception(e)
        return result_df if result_df is not None else pd.DataFrame()

    def get_ae_was_sql_text_by_no_cluster(self, chunk_size):
        query = AeWasSqlTextSql.SELECT_BY_NO_CLUSTER_ID

        return self._get_df_by_chunk_size(self.analysis_engine, query, chunk_size,)

    def update_unanalyzed_was_sql_text(self):
        update_query = AeWasSqlTextSql.UPDATE_BY_NO_ANALYZED_TARGET

        self._default_execute_query(self.sa_conn, update_query)

    def insert_ae_txn_sql_similarity(self, result_valid_df):
        table_name = TableConstants.AE_TXN_SQL_SIMILARITY
        self._psql_insert_copy(table_name, self.analysis_engine, result_valid_df)

    def get_ae_was_sql_text_by_sql_id(self, sql_id):
        table_name = TableConstants.AE_WAS_SQL_TEXT
        query = SqlUtils.sql_replace_to_dict(AeWasSqlTextSql.SELECT_CLUSTER_ID_BY_SQL_ID, {'sql_id': sql_id})
        df = self._get_target_data_by_query(self.sa_conn, query, table_name)
        return df
