import pandas as pd
import psycopg2 as db
import psycopg2.extras

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from src.common.utils import TargetUtils, SystemUtils
from src.common.constants import TableConstants, SystemConstants
from src.common.enum_module import ModuleFactoryEnum
from sql.sql_text_merge_sql import InterMaxSqlTextMergeQuery, SaSqlTextMergeQuery
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

        self.chunksize = self.config.get('data_handling_chunksize', 10_000)
        self.extract_chunksize = self.chunksize * 10
        self.sql_match_time = self.config.get('sql_match_time', 0)
        self.sql_file_root_path = f"{self.config['home']}/" \
                                  f"{SystemConstants.SQL}/" \
                                  f"{ModuleFactoryEnum[self.config['args']['proc']].value}"

        self.update_cluster_cnt = 0

    def _insert_meta_data(self, target_infra):
        meta_path = f"{self.sql_file_root_path}/{SystemConstants.META}/{target_infra}/"
        meta_files = SystemUtils.get_filenames_from_path(meta_path)

        if target_infra == 'was':
            target_conn = self.im_conn
        if target_infra == 'db':
            target_conn = self.mg_conn

        for meta_file in meta_files:

            with open(f"{meta_path}{meta_file}", mode='r', encoding='utf-8') as file:
                query = file.read()

            table_name = SystemUtils.extract_tablename_in_filename(meta_file)
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

        extractor_file_path = f"{self.sql_file_root_path}/was/"

        extractor_files = SystemUtils.get_filenames_from_path(extractor_file_path)
        delete_query = CommonSql.DELETE_TABLE_BY_DATE_QUERY

        date_conditions = TargetUtils.set_intermax_date(self.config['args']['s_date'], self.config['args']['interval'])

        for extractor_file in extractor_files:

            with open(f"{extractor_file_path}{extractor_file}", mode='r', encoding='utf-8') as file:
                query = file.read()

            table_name = SystemUtils.extract_tablename_in_filename(extractor_file)

            for date in date_conditions:
                table_suffix_dict = {'table_suffix': date}

                detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)
                delete_dict = {'table_name': table_name, 'date': date}

                try:
                    im_delete_query = SystemUtils.sql_replace_to_dict(delete_query, delete_dict)
                    self.logger.info(f"delete query execute : {im_delete_query}")

                    TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, im_delete_query)
                    self._execute_insert_intermax_detail_data(detail_query, table_name)

                except Exception as e:
                    self.logger.exception(f"{table_name} table, {date} date detail data insert error")
                    self.logger.exception(e)

        self._set_insert_xapm_sql_text()

    def _execute_insert_intermax_detail_data(self, query, table_name):
        im_conn = self.im_engine.connect().execution_options(stream_results=True)
        get_read_sql_query = pd.read_sql_query(text(query), im_conn, chunksize=self.extract_chunksize)
        for df in get_read_sql_query:
            TargetUtils.insert_analysis_by_df(self.logger, self.analysis_engine, table_name, df)

    def _set_insert_xapm_sql_text(self):

        self._dblink_connect()

        self._insert_new_xapm_sql_text()

    def _create_dblink_query(self):
        create_dblink_query = CommonSql.CREATE_DBLINK
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, create_dblink_query)

    def _dblink_connect(self):
        dblink_connect_query = CommonSql.DBLINK_CONNECT
        intermax_db_info = {'intermax_db_info': self.im_conn_str}
        dblink_query = SystemUtils.sql_replace_to_dict(dblink_connect_query, intermax_db_info)
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, dblink_query)

    def _insert_new_xapm_sql_text(self):
        sql_id_query = CommonSql.SELECT_SQL_ID
        intermax_db_info = {'intermax_db_info': self.im_conn_str}
        select_sql_id_query = SystemUtils.sql_replace_to_dict(sql_id_query, intermax_db_info)
        sa_conn = self.analysis_engine.connect().execution_options(stream_results=True)
        get_read_sql = pd.read_sql_query(text(select_sql_id_query),sa_conn,chunksize=self.extract_chunksize)
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

        db_info_df = TargetUtils.get_target_data_by_query(self.logger, self.sa_conn, ae_db_info_query, ae_db_info_name)

        delete_query = CommonSql.DELETE_TABLE_BY_PARTITION_KEY_QUERY
        extractor_file_path = f"{self.sql_file_root_path}/db/"
        extractor_files = SystemUtils.get_filenames_from_path(extractor_file_path)

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
                        mg_delete_query = SystemUtils.sql_replace_to_dict(delete_query, delete_suffix_dict)
                        self.logger.info(f"delete query execute : {mg_delete_query}")
                        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, mg_delete_query)

                        detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)
                        self._execute_insert_maxgauge_detail_data(detail_query, table_name)
                    except Exception as e:
                        self.logger.exception(f"{table_name} table, {date} date detail data insert error")
                        self.logger.exception(e)

    def _execute_insert_maxgauge_detail_data(self, query, table_name):
        mg_conn = self.mg_engine.connect().execution_options(stream_results=True)
        get_read_sql_query = pd.read_sql_query(text(query), mg_conn, chunksize=self.extract_chunksize)
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
        init_path = f"{self.sql_file_root_path}/{SystemConstants.DDL}/"
        init_files = SystemUtils.get_filenames_from_path(init_path)

        for init_file in init_files:
            with open(f"{init_path}{init_file}", mode='r', encoding='utf-8') as file:
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

    def create_temp_table(self):
        start_date, end_date = SaTarget.summarizer_set_date(self.config['args']['s_date'])
        date_dict = {'StartDate': start_date, 'EndDate': end_date}

        summarizer_temp_path = f"{self.sql_file_root_path}/temp/"
        summarizer_temp_files = SystemUtils.get_filenames_from_path(summarizer_temp_path)

        for summarizer_temp_file in summarizer_temp_files:

            with open(f"{summarizer_temp_path}{summarizer_temp_file}", mode='r', encoding='utf-8') as file:
                query = file.read()

            query = SystemUtils.sql_replace_to_dict(query, date_dict)

            try:
                TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, query)
            except Exception as e:
                self.logger.exception(f"{summarizer_temp_file.split('.')[0]} table, create_temp_table execute error")
                self.logger.exception(e)

    @staticmethod
    def summarizer_set_date(input_date):
        start_date = datetime.strptime(input_date, '%Y%m%d')
        end_date = start_date + timedelta(days=1)
        start_date = start_date.strftime('%Y-%m-%d 00:00:00')
        end_date = end_date.strftime('%Y-%m-%d 00:00:00')
        return start_date, end_date

    def summary_join(self):
        start_date, end_date = SaTarget.summarizer_set_date(self.config['args']['s_date'])
        date_dict = {'StartDate': start_date, 'EndDate': end_date}

        summary_path = f"{self.sql_file_root_path}/summary/"
        summary_files = SystemUtils.get_filenames_from_path(summary_path)

        delete_query = CommonSql.DELETE_SUMMARY_TABLE_BY_DATE_QUERY

        for summary_file in summary_files:

            with open(f"{summary_path}{summary_file}", mode='r', encoding='utf-8') as file:
                query = file.read()

            query = SystemUtils.sql_replace_to_dict(query, date_dict)
            table_name = summary_file.split('.')[0]

            delete_dict = {'table_name': table_name, 'date': self.config['args']['s_date']}

            try:
                sa_delete_query = SystemUtils.sql_replace_to_dict(delete_query, delete_dict)
                TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, sa_delete_query)
                self.logger.info(f"delete query execute : {sa_delete_query}")

                inter_df = TargetUtils.get_target_data_by_query(self.logger, self.sa_conn, query, table_name)
                TargetUtils.insert_analysis_by_df(self.logger, self.analysis_engine, table_name, inter_df)

            except Exception as e:
                self.logger.exception(f"{summary_file.split('.')[0]} table, summary insert execute error")
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

    def update_cluster_id_by_sql_id(self, df):
        self.logger.info(f"Execute query total : {len(df)}")
        self.update_cluster_cnt = 0

        [self._update_cluster_id_by_sql_id(row) for _, row in df.iterrows()]
        self.logger.info(f"Execute query end : {self.update_cluster_cnt}")

    def _update_cluster_id_by_sql_id(self, row):
        query = CommonSql.UPDATE_AE_WAS_SQL_TEXT_BY_CLUSTER_ID_QUERY

        try:
            exec_query = SystemUtils.sql_replace_to_dict(query,
                                                         {'cluster_id': row['cluster_id'], 'sql_id': row['sql_id']}
                                                         )
            TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, exec_query)

            self.update_cluster_cnt += 1
        except IntegrityError:
            pass
        except Exception as e:
            self.logger.exception(f"_update_cluster_id_by_sql_id(), update execute error. check sql_id {row['sql_id']}")
            self.logger.exception(e)
