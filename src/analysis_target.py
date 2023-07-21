import pandas as pd
import numpy as np
import psycopg2.extras
import sys

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from psycopg2 import errors
from psycopg2.errorcodes import DUPLICATE_TABLE

from src.decoder.intermax_decryption import Decoding
from src.common.utils import TargetUtils, SqlUtils
from src.common.constants import TableConstants
from src.common.timelogger import TimeLogger
from sql.common_sql import CommonSql, AeWasSqlTextSql, AeDbSqlTemplateMapSql, AeDbInfoSql, AeDbSqlTextSql
from sql.common_sql import XapmTxnSqlDetail


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

    def _insert_table_by_df(self, engine, table_name, df):
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

        """sqlalchemy를 이용하여 데이터 upsert하는 함수"""

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
        """
        chunk size 만큼 테이블 데이터를 읽어 오는 함수
        :param engine: 각 target engine object
        :param query: 조회하려는 쿼리
        :param chunk_size: 로드하려는 chunk size
        :param coerce: float 데이터 처리
        :return: 조회한 데이터 데이터프레임
        """
        if chunk_size == 0:
            chunk_size = self.extract_chunksize
        conn = engine.connect().execution_options(stream_results=True)
        return pd.read_sql_query(text(query), conn, chunksize=chunk_size, coerce_float=coerce)

    def get_data_by_query(self, query):
        pass


class InterMaxTarget(CommonTarget):

    def init_process(self):
        self.im_engine = self._create_engine(self.im_engine_template)
        self.im_conn = self.im_engine.raw_connection()

    def get_data_by_query(self, query):
        return self._get_df_by_chunk_size(self.im_engine, query)

    def get_xapm_txn_sql_detail(self, start_param, end_param):
        param_dict = {'start_param': start_param, 'end_param': end_param}
        query = SqlUtils.sql_replace_to_dict(XapmTxnSqlDetail.SELECT_XAPM_TXN_SQL_DETAIL, param_dict)

        return self._get_target_data_by_query(self.im_conn, query, "XAPM_SQL_SUMMARY")


class MaxGaugeTarget(CommonTarget):

    def init_process(self):
        self.mg_engine = self._create_engine(self.mg_engine_template)
        self.mg_conn = self.mg_engine.raw_connection()

    def get_data_by_query(self, query):
        return self._get_df_by_chunk_size(self.mg_engine, query)


class SaTarget(CommonTarget):

    def init_process(self):
        self.analysis_engine = self._create_engine(self.analysis_engine_template)
        self.sa_conn = self.analysis_engine.raw_connection()
        self.sa_cursor = self.sa_conn.cursor()

    def create_table(self, ddl):
        self._default_execute_query(self.sa_conn, ddl)

    def insert_table_by_df(self, df, table_name):
        """
        분석 DB table에 데이터 프레임을 insert 하는 함수
        :param df: insert 하려는 데이터 프레임
        :param table_name: 저장하려는 table name
        """
        self._insert_table_by_df(self.analysis_engine, table_name, df)

    def delete_data(self, delete_query, delete_dict):
        delete_table_query = SqlUtils.sql_replace_to_dict(delete_query, delete_dict)
        self.logger.info(f"delete query execute : {delete_table_query}")
        self._default_execute_query(self.sa_conn, delete_table_query)

    def upsert_data(self, df, target_table_name):
        self._psql_insert_copy(target_table_name, self.analysis_engine, df)

    def insert_bind_value_date(self, df, table_name):
        """
        xapm_bind_sql_elapse의 bind_list 컬럼의 데이터들을 복호화하여
        bind_value 컬럼에 insert 기능 함수

        :param df : xapm_bind_sql_elapse의 data를 dataframe 형태로 불러옴
        :param table_name :ae_bind_sql_elapse
        """

        if self.config['extract_bind_value']:

            if self.intermax_decoder is None:
                self.intermax_decoder = Decoding(self.config)
                self.intermax_decoder.set_path()

            df['bind_value'] = df['bind_list'].apply(
                self.intermax_decoder.execute_bind_list_decoding
            )
            df['bind_value'] = df['bind_value'].astype(str)

        self._insert_table_by_df(self.analysis_engine, table_name, df)

    def insert_dev_except_data(self, detail_df, table_name, ae_dev_map_df):

        """
        분석 모듈 DB ae_was_dev_map 테이블에서 was_id에 해당하는 값들을 제외하고 data insert 기능 함수
        """
        was_id_except_df = detail_df[~detail_df['was_id'].isin(ae_dev_map_df['was_id'])]
        self._insert_table_by_df(self.analysis_engine, table_name, was_id_except_df)

    def get_data_by_query(self, query, chunksize=0, coerce=True):
        return self._get_df_by_chunk_size(self.analysis_engine, query, chunk_size=chunksize, coerce=coerce)

    def get_data_by_query_and_once(self, query, table_name="UNKNOWN TABLE"):
        return self._get_target_data_by_query(self.sa_conn, query, table_name)

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

    def get_ae_db_sql_text_by_1seq(self, partition_key, chunksize):
        """
        ae_db_sql_text 테이블에 seq가 1인 데이터 조회 함수
        :param partition_key: 조회하려는 partition_key
        :param chunksize: 조회하려는 chunksize
        :return:
        """
        replace_dict = {'partition_key': partition_key}
        query = SqlUtils.sql_replace_to_dict(AeDbSqlTextSql.SELECT_AE_DB_SQL_TEXT_1SEQ, replace_dict)

        return self._get_df_by_chunk_size(self.analysis_engine, query, chunk_size=chunksize)
        # return pd.read_sql_query(query, self.sa_conn, chunksize=chunksize)

    def get_all_ae_db_sql_text_by_1seq(self, df, chunksize):
        query_with_data = AeDbSqlTextSql.SELECT_AE_DB_SQL_TEXT_WITH_DATA
        params = tuple(df.itertuples(index=False, name=None))
        psycopg2.extras.execute_values(self.sa_cursor, query_with_data, params, page_size=chunksize)
        results = self.sa_cursor.fetchall()

        return results

    def sql_query_convert_df(self, sql_query):
        table_name = TableConstants.AE_TXN_SQL_SUMMARY
        df = self._get_target_data_by_query(self.sa_conn, sql_query, table_name)

        return df

    def insert_ae_sql_text_by_merged_df(self, merged_df):
        """
        ae_sql_text 테이블에 sql_text_merge 기능 수행 결과 저장 함수
        :param merged_df: sql text merge 된 sql text 데이터 프레임
        :return:
        """
        table_name = TableConstants.AE_SQL_TEXT
        total_len = 0

        for i in range(len(merged_df)):
            try:
                merged_df.iloc[i:i + 1].to_sql(
                    table_name,
                    if_exists='append',
                    con=self.analysis_engine,
                    schema='public',
                    index=False
                )
                total_len += 1
            except IntegrityError:
                pass
            except Exception as e:
                self.logger.exception(e)

        self.logger.info(f"Matching Data except duplicate, Insert rows : {total_len}")

    def get_ae_db_info(self):
        """
        ae_db_info table 조회 함수
        :return:
        """
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

    def get_ae_was_sql_text_by_term(self, s_date, e_date, chunksize):
        """
        ae_was_sql_text 테이블의 특정 기간 만큼 조회하려는 쿼리
        :param s_date: 조회 시작일
        :param e_date: 조회 마지막일
        :param chunksize: 한 트랜잭션에 조회하려는 chunksize
        :return: 조회한 결과
        """
        date_dict = {'StartDate': str(s_date), 'EndDate': str(e_date), 'seconds': str(self.sql_match_time)}
        query = AeWasSqlTextSql.SELECT_SQL_ID_AND_SQL_TEXT
        replaced_query = SqlUtils.sql_replace_to_dict(query, date_dict)

        return self._get_df_by_chunk_size(self.analysis_engine, replaced_query, chunksize,)

    def update_cluster_id_by_sql_id(self, df, target):
        """
        sql_text_template 수행 후 sql_id별 cluster_id를 업데이트 하는 함수
        was sql text의 경우 ae_was_sql_text에 update
        db sql text의 경우 ae_db_sql_template_map에 upsert
        :param df: sql_text_template 결과 데이터 프레임
        :param target: 대상 타겟 (was / db)
        :return:
        """
        self.logger.info(f"Execute update_cluster_id_by_sql_id query total : {len(df)}")
        self.update_cluster_cnt = 0

        [self._update_cluster_id_by_sql_id(row, target) for _, row in df.iterrows()]
        self.logger.info(f"Execute update_cluster_id_by_sql_id query end : {self.update_cluster_cnt}")

    def _update_cluster_id_by_sql_id(self, row, target):
        """
        sql_text_template 수행 후 sql_id별 cluster_id를 업데이트 하는 실제 함수
        :param row: sql_text_template 결과 데이터 프레임 row
        :param target: 대상 타겟 (was / db)
        :return:
        """
        sql_id = ''
        target_table = ''

        if target == 'was':
            exec_query = SqlUtils.sql_replace_to_dict(
                AeWasSqlTextSql.UPDATE_CLUSTER_ID_BY_SQL_ID,
                {'cluster_id': row['cluster_id'], 'sql_id': row['sql_id']}
            )
            sql_id = row['sql_id']
            target_table = 'ae_was_sql_text'
        elif target == 'db':
            exec_query = SqlUtils.sql_replace_to_dict(
                AeDbSqlTemplateMapSql.UPSERT_CLUSTER_ID_BY_SQL_UID,
                {'cluster_id': row['cluster_id'], 'sql_uid': row['sql_uid']}
            )
            sql_id = row['sql_uid']
            target_table = 'ae_db_sql_template_map'
        try:
            self._default_execute_query(self.sa_conn, exec_query)
            self.update_cluster_cnt += 1
        except IntegrityError as ie:
            self.logger.exception(
                f"_update_cluster_id_by_sql_id(), update execute error. check {target_table} table, sql_id {sql_id}"
            )
            self.logger.exception(ie)
            pass
        except Exception as e:
            self.logger.exception(
                f"_update_cluster_id_by_sql_id(), update execute error. check {target_table} table, sql_id {sql_id}"
            )
            self.logger.exception(e)

    def insert_ae_sql_template(self, df):
        """
        ae_sql_template 테이블 저장 함수
        :param df: 저장 하려는 데이터 프레임
        :return:
        """
        table_name = TableConstants.AE_SQL_TEMPLATE
        truncate_query = SqlUtils.sql_replace_to_dict(
            CommonSql.TRUNCATE_TABLE_DEFAULT_QUERY,
            {'table_name': table_name}
        )
        self._default_execute_query(self.sa_conn, truncate_query)
        self._insert_table_by_df(self.analysis_engine, table_name, df)

    def get_cluster_cnt_by_grouping(self, extract_cnt):
        """
        cluster_id로 그룹핑된 총 갯수 조회 함수
        :param extract_cnt: 추출 건수
        :return: 조회한 결과
        """
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
        """
        ae_was_sql_text에 cluster_id가 null인 데이터 조회 함수
        :param chunk_size: 한 트랜잭션에 조회하려는 chunksize
        :return: 조회한 결과
        """
        query = AeWasSqlTextSql.SELECT_BY_NO_CLUSTER_ID
        return self._get_df_by_chunk_size(self.analysis_engine, query, chunk_size,)

    def update_unanalyzed_was_sql_text(self):
        """
        미분석 ae_was_sql_text 데이터 0으로 업데이트 처리 함수
        :return:
        """
        update_query = AeWasSqlTextSql.UPDATE_BY_NO_ANALYZED_TARGET
        self._default_execute_query(self.sa_conn, update_query)

    def insert_ae_txn_sql_similarity(self, result_valid_df):
        """
        ae_txn_sql_similarity 에 결과 upsert 함수
        :param result_valid_df: 유사도 유효한 데이터 프레임
        :return:
        """
        table_name = TableConstants.AE_TXN_SQL_SIMILARITY
        self._psql_insert_copy(table_name, self.analysis_engine, result_valid_df)

    def get_ae_was_sql_text_by_sql_id(self, sql_id):
        """
        ae_was_sql_text 테이블에 sql_id 별 cluster_id 조회 함수
        :param sql_id: 조회하려는 sql_id
        :return: 조회한 결과
        """
        table_name = TableConstants.AE_WAS_SQL_TEXT
        query = SqlUtils.sql_replace_to_dict(AeWasSqlTextSql.SELECT_CLUSTER_ID_BY_SQL_ID, {'sql_id': sql_id})
        df = self._get_target_data_by_query(self.sa_conn, query, table_name)
        return df
