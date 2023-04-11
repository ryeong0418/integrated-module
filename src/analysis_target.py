import pandas as pd
import psycopg2 as db
import psycopg2.extras

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from src.common.utils import TargetUtils, SystemUtils
from src.common.constants import TableConstants, SystemConstants
from src.common.enum_module import ModuleFactoryEnum
from sql.sql_text_merge_sql import InterMaxSqlTextMergeQuery, SaSqlTextMergeQuery
from sql.extract_sql import InterMaxExtractQuery, MaxGaugeExtractorQuery
from sql.summarizer_sql import SummarizerQuery,InterMaxGaugeSummarizerQuery
from sql.common_sql import CommonSql
from datetime import datetime, timedelta


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

        self.chunksize = self.config['data_handling_chunksize']
        self.sql_match_time = self.config['sql_match_time']

    def _insert_meta_data(self, target_infra):
        meta_path = f"{self.config['home']}/" \
                    f"{SystemConstants.SQL_PATH}/" \
                    f"{ModuleFactoryEnum[self.config['args']['proc']].value}/" \
                    f"{SystemConstants.META_PATH}/" \
                    f"{target_infra}/"
        meta_files = SystemUtils.get_filenames_from_path(meta_path)

        if target_infra == 'was':
            target_conn = self.im_conn
        if target_infra == 'db':
            target_conn = self.mg_conn

        for meta_file in meta_files:

            with open(f"{meta_path}{meta_file}", mode='r', encoding='utf-8') as file:
                query = file.read()

            table_name = meta_file.split(".")[0].split('-')[1]
            self._execute_insert_meta(query, table_name, target_conn)

    def _execute_insert_meta(self, query, table_name, target_conn):
        replace_dict = {'table_name': table_name}
        delete_table_query = SystemUtils.sql_replace_to_dict(CommonSql.DELETE_TABLE_DEFAULT_QUERY, replace_dict)
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, delete_table_query)
        meta_df = TargetUtils.get_target_data_by_query(self.logger, target_conn, query, table_name,)
        TargetUtils.insert_analysis_by_df(self.logger, self.analysis_engine, table_name, meta_df)


class InterMaxTarget(CommonTarget):

    def init_process(self):
        self.im_conn = db.connect(self.im_conn_str)
        self.analysis_engine = create_engine(self.analysis_engine_template)
        self.im_engine = create_engine(self.im_engine_template)

    def create_im_engine(self):
        self.im_engine = create_engine(self.im_engine_template)

    def __del__(self):
        if self.im_conn:
            self.im_conn.close()
        if self.sa_conn:
            self.sa_conn.close()
        if self.analysis_engine:
            self.analysis_engine.dispose()

    def insert_intermax_meta(self):
        self.sa_conn = db.connect(self.analysis_conn_str)

        self._create_dblink_query()

        self._insert_meta_data('was')

    def get_xapm_sql_text(self, chunksize):
        query = InterMaxSqlTextMergeQuery.SELECT_XAPM_SQL_TEXT

        conn = self.im_engine.connect().execution_options(stream_results=True,)
        return pd.read_sql_query(text(query), conn, chunksize=chunksize)

    def insert_ae_sql_text(self, filtered_df):
        table_name = TableConstants.AE_SQL_TEXT
        TargetUtils.default_insert_data(self.logger, self.analysis_engine, table_name, filtered_df)

    def insert_intermax_detail_data(self):
        self.sa_conn = db.connect(self.analysis_conn_str)

        date_conditions = TargetUtils.set_intermax_date(self.config['args']['s_date'], self.config['args']['interval'])
        for date in date_conditions:
            table_suffix_dict = {'table_suffix': date}
            delete_query = InterMaxExtractQuery.DELETE_INTERMAX_QUERY

            self._set_insert_xapm_txn_detail(table_suffix_dict,delete_query,date)

            self._set_insert_xapm_txn_sql_detail(table_suffix_dict,delete_query,date)

            self._set_insert_xapm_txn_sql_fetch(table_suffix_dict,delete_query,date)

            self._set_insert_xapm_was_stat_summary(table_suffix_dict,delete_query,date)

            self._set_insert_xapm_jvm_stat_summmary(table_suffix_dict,delete_query,date)

            self._set_insert_xapm_os_stat_summary(table_suffix_dict,delete_query,date)

        self._set_insert_xapm_sql_text()

    def _set_insert_xapm_sql_text(self):

        self._dblink_connect()

        self._insert_new_xapm_sql_text()

    def _set_insert_xapm_txn_detail(self, table_suffix_dict, delete_query, date, table_name=TableConstants.AE_TXN_DETAIL):
        query = InterMaxExtractQuery.SELECT_XAPM_TXN_DETAIL
        detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)

        delete_dict = {'table_name': table_name, 'date': date}
        im_delete_query = SystemUtils.sql_replace_to_dict(delete_query,delete_dict)
        TargetUtils.default_sa_execute_query(self.logger,self.sa_conn,im_delete_query)

        try:
            self._excute_insert_intermax_detail_data(detail_query, table_name)

        except Exception as e:
            self.logger.exception(e)

    def _set_insert_xapm_txn_sql_detail(self, table_suffix_dict, delete_query, date, table_name=TableConstants.AE_TXN_SQL_DETAIL):
        query = InterMaxExtractQuery.SELECT_XAPM_TXN_SQL_DETAIL
        detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)

        delete_dict = {'table_name': table_name, 'date': date}
        im_delete_query = SystemUtils.sql_replace_to_dict(delete_query,delete_dict)
        TargetUtils.default_sa_execute_query(self.logger,self.sa_conn,im_delete_query)

        try:
            self._excute_insert_intermax_detail_data(detail_query, table_name)
        except Exception as e:
            self.logger.exception(e)

    def _set_insert_xapm_txn_sql_fetch(self,table_suffix_dict,delete_query,date,table_name=TableConstants.AE_TXN_SQL_FETCH):
        query = InterMaxExtractQuery.SELECT_XAPM_TXN_SQL_FETCH
        detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)

        delete_dict = {'table_name': table_name, 'date': date}
        im_delete_query = SystemUtils.sql_replace_to_dict(delete_query, delete_dict)
        TargetUtils.default_sa_execute_query(self.logger,self.sa_conn, im_delete_query)

        try:
            self._excute_insert_intermax_detail_data(detail_query, table_name)
        except Exception as e:
            self.logger.exception(e)

    def _set_insert_xapm_was_stat_summary(self,table_suffix_dict,delete_query,date, table_name=TableConstants.AE_WAS_STAT_SUMMARY):
        query = InterMaxExtractQuery.SELECT_XAPM_WAS_STAT_SUMMARY
        detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)

        delete_dict = {'table_name': table_name, 'date': date}
        im_delete_query = SystemUtils.sql_replace_to_dict(delete_query, delete_dict)
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, im_delete_query)

        try:
            self._excute_insert_intermax_detail_data(detail_query, table_name)
        except Exception as e:
            self.logger.exception(e)

    def _set_insert_xapm_jvm_stat_summmary(self, table_suffix_dict, delete_query, date, table_name=TableConstants.AE_JVM_STAT_SUMMARY):
        query = InterMaxExtractQuery.SELECT_XAPM_JVM_STAT_SUMMARY
        detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)

        delete_dict = {'table_name': table_name, 'date': date}
        im_delete_query = SystemUtils.sql_replace_to_dict(delete_query, delete_dict)
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, im_delete_query)

        try:
            self._excute_insert_intermax_detail_data(detail_query, table_name)
        except Exception as e:
            self.logger.exception(e)

    def _set_insert_xapm_os_stat_summary(self, table_suffix_dict, delete_query, date, table_name=TableConstants.AE_WAS_OS_STAT_OSM):
        query = InterMaxExtractQuery.SELECT_XAPM_OS_STAT_OSM
        detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)

        delete_dict = {'table_name': table_name, 'date': date}
        im_delete_query = SystemUtils.sql_replace_to_dict(delete_query, delete_dict)
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, im_delete_query)

        try:
            self._excute_insert_intermax_detail_data(detail_query, table_name)
        except Exception as e:
            self.logger.exception(e)

    def _excute_insert_intermax_detail_data(self, query, table_name):
        im_conn = self.im_engine.connect().execution_options(stream_results=True)
        get_read_sql_query = pd.read_sql_query(text(query),im_conn,chunksize=self.chunksize*10)
        for df in get_read_sql_query:
            TargetUtils.insert_analysis_by_df(self.logger, self.analysis_engine, table_name, df)

    def _create_dblink_query(self):
        create_dblink_query = CommonSql.CREATE_DBLINK
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, create_dblink_query)

    def _dblink_connect(self):
        dblink_connect_query = CommonSql.DBLINK_CONNECT
        intermax_db_info = {'intermax_db_info':self.im_conn_str}
        dblink_query = SystemUtils.sql_replace_to_dict(dblink_connect_query,intermax_db_info)
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, dblink_query)

    def _insert_new_xapm_sql_text(self):
        sql_id_query = CommonSql.SELECT_SQL_ID
        intermax_db_info = {'intermax_db_info': self.im_conn_str}
        select_sql_id_query = SystemUtils.sql_replace_to_dict(sql_id_query, intermax_db_info)
        sa_conn = self.analysis_engine.connect().execution_options(stream_results=True)
        get_read_sql = pd.read_sql_query(text(select_sql_id_query),sa_conn,chunksize=self.chunksize*10)
        table_name = TableConstants.AE_WAS_SQL_TEXT

        for df in get_read_sql:
            TargetUtils.insert_analysis_by_df(self.logger, self.analysis_engine, table_name, df)


class MaxGaugeTarget(CommonTarget):

    def init_process(self):
        self.mg_conn = db.connect(self.mg_conn_str)
        self.analysis_engine = create_engine(self.analysis_engine_template)
        self.mg_engine = create_engine(self.mg_engine_template)

    def __del__(self):
        if self.mg_conn:
            self.mg_conn.close()
        if self.sa_conn:
            self.sa_conn.close()
        if self.analysis_engine:
            self.analysis_engine.dispose()

    def insert_maxgauge_meta(self):
        self.sa_conn = db.connect(self.analysis_conn_str)

        self._insert_meta_data('db')

    def insert_maxgauge_detail_data(self):
        self.sa_conn = db.connect(self.analysis_conn_str)

        date_conditions = TargetUtils.set_maxgauge_date(self.config['args']['s_date'], self.config['args']['interval'])
        ae_db_info_query = CommonSql.SELECT_AE_DB_INFO
        ae_db_info_name = TableConstants.AE_DB_INFO
        delete_query = MaxGaugeExtractorQuery.DELETE_MAXGAUGE_QUERY
        df = TargetUtils.get_target_data_by_query(self.logger, self.sa_conn, ae_db_info_query, ae_db_info_name)

        for i, name in df.values:
            db_id = str(i).zfill(3)
            for date in date_conditions:
                table_suffix_dict = {'instance_name': name, 'partition_key': date + db_id}
                delete_suffix_dict = {'table_name': 'known', 'partition_key': date + db_id}

                self._set_insert_ora_session_info(table_suffix_dict,delete_query,delete_suffix_dict)

                self._set_insert_ora_session_stat(table_suffix_dict,delete_query,delete_suffix_dict)

                self._set_insert_apm_sql_list(table_suffix_dict,delete_query,delete_suffix_dict)

                self._set_insert_ora_sql_stat_10(table_suffix_dict,delete_query,delete_suffix_dict)

                self._set_insert_ora_sql_wait_10(table_suffix_dict,delete_query,delete_suffix_dict)

    def _set_insert_ora_session_info(self,table_suffix_dict,delete_query,delete_suffix_dict,table_name=TableConstants.AE_SESSION_INFO):

        query = MaxGaugeExtractorQuery.SELECT_ORA_SESSION_INFO
        detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)

        delete_suffix_dict['table_name'] = table_name

        mg_delete_query = SystemUtils.sql_replace_to_dict(delete_query, delete_suffix_dict)
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, mg_delete_query)

        try:
            self._excute_insert_maxgauge_detail_data(detail_query, table_name)
        except Exception as e:
            self.logger.exception(e)

    def _set_insert_ora_session_stat(self,table_suffix_dict,delete_query,delete_suffix_dict,table_name=TableConstants.AE_SESSION_STAT):

        query = MaxGaugeExtractorQuery.SELECT_ORA_SESSION_STAT
        detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)

        delete_suffix_dict['table_name'] = table_name

        mg_delete_query = SystemUtils.sql_replace_to_dict(delete_query, delete_suffix_dict)
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, mg_delete_query)

        try:
            self._excute_insert_maxgauge_detail_data(detail_query, table_name)
        except Exception as e:
            self.logger.exception(e)

    def _set_insert_apm_sql_list(self,table_suffix_dict,delete_query,delete_suffix_dict,table_name=TableConstants.AE_DB_SQL_TEXT):

        query = MaxGaugeExtractorQuery.SELECT_APM_SQL_LIST
        detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)

        delete_suffix_dict['table_name'] = table_name

        mg_delete_query = SystemUtils.sql_replace_to_dict(delete_query, delete_suffix_dict)
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, mg_delete_query)

        try:
            self._excute_insert_maxgauge_detail_data(detail_query, table_name)
        except Exception as e:
            self.logger.exception(e)

    def _set_insert_ora_sql_stat_10(self,table_suffix_dict,delete_query,delete_suffix_dict,table_name=TableConstants.AE_SQL_STAT_10MIN):

        query = MaxGaugeExtractorQuery.SELECT_ORA_SQL_STAT_10
        detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)

        delete_suffix_dict['table_name'] = table_name

        mg_delete_query = SystemUtils.sql_replace_to_dict(delete_query, delete_suffix_dict)
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, mg_delete_query)

        try:
            self._excute_insert_maxgauge_detail_data(detail_query, table_name)
        except Exception as e:
            self.logger.exception(e)

    def _set_insert_ora_sql_wait_10(self,table_suffix_dict,delete_query,delete_suffix_dict,table_name=TableConstants.AE_SQL_WAIT_10MIN):

        query = MaxGaugeExtractorQuery.SELECT_ORA_SQL_WAIT_10
        detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)

        delete_suffix_dict['table_name'] = table_name

        mg_delete_query = SystemUtils.sql_replace_to_dict(delete_query, delete_suffix_dict)
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, mg_delete_query)

        try:
            self._excute_insert_maxgauge_detail_data(detail_query, table_name)
        except Exception as e:
            self.logger.exception(e)

    def _excute_insert_maxgauge_detail_data(self, query, table_name):
        mg_conn = self.mg_engine.connect().execution_options(stream_results=True)
        get_read_sql_query = pd.read_sql_query(text(query),mg_conn,chunksize=self.chunksize*10)
        for df in get_read_sql_query:
            TargetUtils.insert_analysis_by_df(self.logger, self.analysis_engine, table_name, df)


class SaTarget(CommonTarget):

    def init_process(self):
        self.sa_conn = db.connect(self.analysis_conn_str)
        self.analysis_engine = create_engine(self.analysis_engine_template)

        self.sa_cursor = self.sa_conn.cursor()

        self.logger.info(f"analysis_repo DB 접속 정보 {self.analysis_conn_str}")
        self.logger.info(f"intermax_repo DB 접속 정보 {self.im_conn_str}")
        self.logger.info(f"maxgauge_repo DB 접속 정보 {self.mg_conn_str}")

        self.analysis_engine = create_engine(self.analysis_engine_template)

    def __del__(self):
        if self.sa_cursor:
            self.sa_cursor.close()
        if self.sa_conn:
            self.sa_conn.close()

    def create_table(self):
        init_path = f"{self.config['home']}/" \
                    f"{SystemConstants.SQL_PATH}/" \
                    f"{ModuleFactoryEnum[self.config['args']['proc']].value}/" \
                    f"{SystemConstants.DDL_PATH}"
        init_files = SystemUtils.get_filenames_from_path(init_path)

        for init_file in init_files:
            with open(f"{init_path}/{init_file}", mode='r', encoding='utf-8') as file:
                ddl = file.read()
                TargetUtils.create_table(self.logger, self.sa_conn, ddl)

    def get_ae_was_sql_text(self, chunksize):
        query = SaSqlTextMergeQuery.SELECT_AE_WAS_SQL_TEXT
        conn = self.analysis_engine.connect().execution_options(stream_results=True,)
        return pd.read_sql_query(text(query), conn, chunksize=chunksize)

    def get_ae_db_sql_text_by_1seq(self, partition_key, chunksize):
        replace_dict = {'partition_key': partition_key}
        query = SystemUtils.sql_replace_to_dict(SaSqlTextMergeQuery.SELECT_AE_DB_SQL_TEXT_1SEQ, replace_dict)

        return pd.read_sql_query(query, self.sa_conn, chunksize=chunksize)

    def get_all_ae_db_sql_text_by_1seq(self, df, chunksize):
        query_with_data = SaSqlTextMergeQuery.SELECT_AE_DB_SQL_TEXT_WITH_DATA

        params = tuple(df.itertuples(index=False, name=None))

        psycopg2.extras.execute_values(self.sa_cursor, query_with_data, params, page_size=chunksize)
        results = self.sa_cursor.fetchall()

        return results

    def ae_txn_detail_summary_temp_create_table(self):
        pairs = TargetUtils.summarizer_set_date(self.config['args']['s_date'], self.config['args']['interval'])
        for pair in pairs:
            date_dict = {'StartDate': pair[0], 'EndDate': pair[1]}
            query = SummarizerQuery.DDL_ae_txn_detail_summary_temp_SQL
            ae_txn_detail_summary_temp_SQL= SystemUtils.sql_replace_to_dict(query, date_dict)

            try:
                TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, ae_txn_detail_summary_temp_SQL)
            except Exception as e:
                self.logger.exception(e)

    def ae_txn_sql_detail_summary_temp_create_table(self):
        pairs = TargetUtils.summarizer_set_date(self.config['args']['s_date'], self.config['args']['interval'])
        for pair in pairs:
            date_dict = {'StartDate': pair[0], 'EndDate': pair[1]}
            query = SummarizerQuery.DDL_ae_txn_sql_detail_summary_temp_SQL
            ae_txn_sql_detail_summary_temp_SQL = SystemUtils.sql_replace_to_dict(query, date_dict)

            try:
                TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, ae_txn_sql_detail_summary_temp_SQL)
            except Exception as e:
                self.logger.exception(e)

    def summary_join(self):
        pairs = TargetUtils.summarizer_set_date(self.config['args']['s_date'], self.config['args']['interval'])
        for pair in pairs:
            date_dict = {'StartDate': pair[0], 'EndDate': pair[1]}
            query = InterMaxGaugeSummarizerQuery.WAS_DB_JOIN
            join_query = SystemUtils.sql_replace_to_dict(query, date_dict)

            try:
                table_name = TableConstants.AE_TXN_SQL_SUMMARY
                inter_df = TargetUtils.get_target_data_by_query(self.logger, self.sa_conn, join_query, table_name)
                TargetUtils.insert_analysis_by_df(self.logger, self.analysis_engine, table_name, inter_df)

            except Exception as e:
                self.logger.exception(e)

    def sql_query_convert_df(self, sql_query):

        table_name = TableConstants.AE_TXN_SQL_SUMMARY
        df = TargetUtils.get_target_data_by_query(self.logger, self.sa_conn, sql_query, table_name)

        return df

    def get_maxgauge_date_conditions(self,):
        return TargetUtils.set_maxgauge_date(self.config['args']['s_date'], self.config['args']['interval'])

    def insert_merged_result(self, merged_df):
        table_name = TableConstants.AE_SQL_TEXT
        total_len = 0

        for i in range(len(merged_df)):
            try:
                merged_df.iloc[i:i+1].to_sql(table_name, if_exists='append', con=self.analysis_engine,
                                             schema='public', index=False)
                total_len += 1
            except IntegrityError:
                pass
            except Exception as e:
                self.logger.exception(e)

        self.logger.info(f"Matching Data except duplicate, Insert rows : {total_len}")

    def get_ae_db_info(self):
        query = CommonSql.SELECT_AE_DB_INFO
        table_name = TableConstants.AE_DB_INFO

        df = TargetUtils.get_target_data_by_query(self.logger, self.sa_conn, query, table_name, )
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
        query = SystemUtils.sql_replace_to_dict(CommonSql.SELECT_TABLE_COLUMN_TYPE, {'table': table})

        cols = pd.read_sql(query, self.sa_conn)

        num_col_type_list = ['int', 'double', 'float']
        time_col_type_list = ['time']
        sel_list = []

        for idx, row in cols.iterrows():
            if any(num_str in row['data_type'] for num_str in num_col_type_list):
                sel_list.append(f"coalesce({row['column_name']}, 0) as {row['column_name']}")
            elif any(num_str in row['data_type'] for num_str in time_col_type_list):
                sel_list.append(f"coalesce({row['column_name']}, now()) as {row['column_name']}")
            else:
                sel_list.append(row['column_name'])

        sel = ",".join(sel_list)

        return f"SELECT {sel} FROM {table}"

    def get_table_data(self, query, chunksize):
        conn = self.analysis_engine.connect().execution_options(stream_results=True, )
        return pd.read_sql_query(text(query), conn, chunksize=chunksize, coerce_float=False)

    def insert_target_table_by_dump(self, table, df):
        TargetUtils.insert_analysis_by_df(self.logger, self.analysis_engine, table, df)

    def term_extract_sql_text(self, chunksize):

        s_date = datetime.strptime(str(self.config['args']['s_date']),'%Y%m%d')
        e_date = s_date + timedelta(days=int(self.config['args']['interval']))

        date_dict = {'StartDate': str(s_date), 'EndDate': str(e_date), 'seconds': str(self.sql_match_time)}
        query = SaSqlTextMergeQuery.SELECT_SQL_ID_AND_SQL_TEXT
        sql_id_and_sql_text = SystemUtils.sql_replace_to_dict(query, date_dict)

        sa_conn = self.analysis_engine.connect().execution_options(stream_results=True)
        get_read_sql = pd.read_sql_query(text(sql_id_and_sql_text),sa_conn,chunksize=chunksize)
        return get_read_sql












