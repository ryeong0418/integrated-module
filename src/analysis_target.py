import os
import pandas as pd
import psycopg2 as db
import psycopg2.extras

from sqlalchemy import create_engine, text

from src.common.utils import TargetUtils, SystemUtils
from src.common.constants import TableConstants,SystemConstants
from sql.initialize_sql import InterMaxInitializeQuery, MaxGaugeInitializeQuery, SaInitializeQuery
from sql.sql_text_merge_sql import InterMaxSqlTextMergeQuery, SaSqlTextMergeQuery
from sql.extract_sql import InterMaxExtractQuery, MaxGaugeExtractorQuery
from sql.summarizer_sql import SummarizerQuery,InterMaxGaugeSummarizerQuery


class CommonTarget:

    def __init__(self, logger, config):
        self.logger = logger
        self.config = config

        self.analysis_conn_str = TargetUtils.get_db_conn_str(config['analysis_repo'])
        self.im_conn_str = TargetUtils.get_db_conn_str(config['intermax_repo'])
        self.mg_conn_str = TargetUtils.get_db_conn_str(config['maxgauge_repo'])

        self.analysis_engine_template = TargetUtils.get_engine_template(config['analysis_repo'])

        self.analysis_engine = None
        self.im_conn = None
        self.mg_conn = None
        self.sa_conn = None

        self.sa_cursor = None


class InterMaxTarget(CommonTarget):

    def init_process(self):
        self.im_conn = db.connect(self.im_conn_str)
        self.analysis_engine = create_engine(self.analysis_engine_template)

    def __del__(self):
        if self.im_conn:
            self.im_conn.close()
        if self.sa_conn:
            self.sa_conn.close()
        if self.analysis_engine:
            self.analysis_engine.dispose()

    def create_table(self):
        conn = db.connect(self.analysis_conn_str)
        querys = InterMaxInitializeQuery.DDL_SQL
        check_query = InterMaxInitializeQuery.CHECK_SQL
        TargetUtils.create_and_check_table(self.logger, conn, querys, check_query)

    def insert_intermax_meta(self):
        self.sa_conn = db.connect(self.analysis_conn_str)

        self._set_insert_xapm_was_info()

        self._set_insert_xapm_txn_name()

        self._set_insert_xapm_sql_text()

        self._set_insert_xapm_db_info()

    def _set_insert_xapm_was_info(self):
        query = InterMaxInitializeQuery.SELECT_XAPM_WAS_INFO
        table_name = TableConstants.AE_WAS_INFO
        self._excute_insert_intermax_meta(query, table_name)

    def _set_insert_xapm_txn_name(self):
        query = InterMaxInitializeQuery.SELECT_XAPM_TXN_NAME
        table_name = TableConstants.AE_TXN_NAME
        self._excute_insert_intermax_meta(query, table_name)

    def _set_insert_xapm_sql_text(self):
        query = InterMaxInitializeQuery.SELECT_XAPM_SQL_TEXT
        table_name = TableConstants.AE_WAS_SQL_TEXT
        self._excute_insert_intermax_meta(query, table_name)

    def _set_insert_xapm_db_info(self):
        query = InterMaxInitializeQuery.SELECT_XAPM_DB_INFO
        table_name = TableConstants.AE_WAS_DB_INFO
        self._excute_insert_intermax_meta(query, table_name)

    def _excute_insert_intermax_meta(self, query, table_name):
        replace_dict = {'table_name': table_name}
        delete_table_query = SystemUtils.sql_replace_to_dict(SaInitializeQuery.DELETE_TABLE_DEFAULT_QUERY, replace_dict)
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, delete_table_query)
        meta_df = TargetUtils.get_target_data_by_query(self.logger, self.im_conn, query, table_name,)
        TargetUtils.insert_analysis_by_df(self.logger, self.analysis_engine, table_name, meta_df)

    def get_xapm_sql_text(self):
        query = InterMaxSqlTextMergeQuery.SELECT_XAPM_SQL_TEXT

        return pd.read_sql(query, self.im_conn)

    def insert_ae_sql_text(self, filtered_df):
        table_name = TableConstants.AE_SQL_TEXT

        TargetUtils.default_insert_data(self.logger, self.analysis_engine, table_name, filtered_df)

    def _insert_intermax_detail_data(self):
        self.sa_conn = db.connect(self.analysis_conn_str)
        
        self._set_insert_xapm_txn_detail()
        
        self._set_insert_xapm_txn_sql_detail()
        
        self._set_insert_xapm_txn_sql_fetch()

    def _set_insert_xapm_txn_detail(self):
        date_conditions = TargetUtils.set_intermax_date(self.config['args']['s_date'], self.config['args']['interval'])
        for date in date_conditions:
            table_suffix_dict = {'table_suffix': date}
            query = InterMaxExtractQuery.SELECT_XAPM_TXN_DETAIL
            detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)
            table_name = TableConstants.AE_TXN_DETAIL
            try:
                self._excute_insert_intermax_detail_data(detail_query, table_name)
            except Exception as e:
                self.logger.exception(e)

    def _set_insert_xapm_txn_sql_detail(self):
        date_conditions = TargetUtils.set_intermax_date(self.config['args']['s_date'], self.config['args']['interval'])
        for date in date_conditions:
            table_suffix_dict = {'table_suffix': date}
            query = InterMaxExtractQuery.SELECT_XAPM_TXN_SQL_DETAIL
            detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)
            table_name = TableConstants.AE_TXN_SQL_DETAIL
            try:
                self._excute_insert_intermax_detail_data(detail_query, table_name)
            except Exception as e:
                self.logger.exception(e)

    def _set_insert_xapm_txn_sql_fetch(self):
        date_conditions = TargetUtils.set_intermax_date(self.config['args']['s_date'], self.config['args']['interval'])
        for date in date_conditions:
            table_suffix_dict = {'table_suffix': date}
            query = InterMaxExtractQuery.SELECT_XAPM_TXN_SQL_FETCH
            detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)
            table_name = TableConstants.AE_TXN_SQL_FETCH
            try:
                self._excute_insert_intermax_detail_data(detail_query, table_name)
            except Exception as e:
                self.logger.exception(e)

    def _excute_insert_intermax_detail_data(self, query, table_name):
        df = TargetUtils.get_target_data_by_query(self.logger, self.im_conn, query, table_name)
        TargetUtils.insert_analysis_by_df(self.logger, self.analysis_engine, table_name, df)


class MaxGaugeTarget(CommonTarget):
    def init_process(self):
        self.mg_conn = db.connect(self.mg_conn_str)
        self.analysis_engine = create_engine(self.analysis_engine_template)

    def __del__(self):
        if self.mg_conn:
            self.mg_conn.close()
        if self.sa_conn:
            self.sa_conn.close()
        if self.analysis_engine:
            self.analysis_engine.dispose()

    def create_table(self):
        conn = db.connect(self.analysis_conn_str)
        querys = MaxGaugeInitializeQuery.DDL_SQL
        check_query = MaxGaugeInitializeQuery.CHECK_SQL

        TargetUtils.create_and_check_table(self.logger, conn, querys, check_query)

    def insert_maxgauge_meta(self):
        self.sa_conn = db.connect(self.analysis_conn_str)

        self._set_insert_ae_db_info()

    def _set_insert_ae_db_info(self):
        query = MaxGaugeInitializeQuery.SELECT_APM_DB_INFO
        table_name = TableConstants.AE_DB_INFO
        self._excute_insert_maxgauge_meta(query, table_name)

    def _excute_insert_maxgauge_meta(self, query, table_name):
        replace_dict = {'table_name': table_name}
        delete_table_query = SystemUtils.sql_replace_to_dict(SaInitializeQuery.DELETE_TABLE_DEFAULT_QUERY, replace_dict)
        TargetUtils.default_sa_execute_query(self.logger, self.sa_conn, delete_table_query)
        detail_df = TargetUtils.get_target_data_by_query(self.logger, self.mg_conn, query, table_name,)
        TargetUtils.insert_analysis_by_df(self.logger, self.analysis_engine, table_name, detail_df)

    def _insert_maxgauge_detail_data(self):
        self.sa_conn = db.connect(self.analysis_conn_str)

        self._set_insert_ora_session_info()

        self._set_insert_ora_session_stat()

        self._set_insert_apm_sql_list()

        self._set_insert_ora_sql_stat_10()

        self._set_insert_ora_sql_wait_10()

    def _set_insert_ora_session_info(self):
        date_conditions = TargetUtils.set_maxgauge_date(self.config['args']['s_date'], self.config['args']['interval'])
        for date in date_conditions:
            table_suffix_dict = {'table_suffix': date}
            query = MaxGaugeExtractorQuery.SELECT_ORA_SESSION_INFO
            detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)
            table_name = TableConstants.AE_SESSION_INFO
            try:
                self._excute_insert_maxgauge_detail_data(detail_query, table_name)
            except Exception as e:
                self.logger.exception(e)

    def _set_insert_ora_session_stat(self):
        date_conditions = TargetUtils.set_maxgauge_date(self.config['args']['s_date'], self.config['args']['interval'])
        for date in date_conditions:
            table_suffix_dict = {'table_suffix': date}
            query = MaxGaugeExtractorQuery.SELECT_ORA_SESSION_STAT
            detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)
            table_name = TableConstants.AE_SESSION_STAT
            try:
                self._excute_insert_maxgauge_detail_data(detail_query, table_name)
            except Exception as e:
                self.logger.exception(e)

    def _set_insert_apm_sql_list(self):
        date_conditions = TargetUtils.set_maxgauge_date(self.config['args']['s_date'], self.config['args']['interval'])
        for date in date_conditions:
            table_suffix_dict = {'table_suffix': date}
            query = MaxGaugeExtractorQuery.SELECT_APM_SQL_LIST
            detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)
            table_name = TableConstants.AE_DB_SQL_TEXT
            try:
                self._excute_insert_maxgauge_detail_data(detail_query, table_name)
            except Exception as e:
                self.logger.exception(e)

    def _set_insert_ora_sql_stat_10(self):
        date_conditions = TargetUtils.set_maxgauge_date(self.config['args']['s_date'], self.config['args']['interval'])
        for date in date_conditions:
            table_suffix_dict = {'table_suffix': date}
            query = MaxGaugeExtractorQuery.SELECT_ORA_SQL_STAT_10
            detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)
            table_name = TableConstants.AE_SQL_STAT_10MIN
            try:
                self._excute_insert_maxgauge_detail_data(detail_query, table_name)
            except Exception as e:
                self.logger.exception(e)

    def _set_insert_ora_sql_wait_10(self):
        date_conditions = TargetUtils.set_maxgauge_date(self.config['args']['s_date'], self.config['args']['interval'])
        for date in date_conditions:
            table_suffix_dict = {'table_suffix': date}
            query = MaxGaugeExtractorQuery.SELECT_ORA_SQL_WAIT_10
            detail_query = SystemUtils.sql_replace_to_dict(query, table_suffix_dict)
            table_name = TableConstants.AE_SQL_WAIT_10MIN
            try:
                self._excute_insert_maxgauge_detail_data(detail_query, table_name)
            except Exception as e:
                self.logger.exception(e)

    def _excute_insert_maxgauge_detail_data(self, query, table_name):
        df = TargetUtils.get_target_data_by_query(self.logger, self.mg_conn, query, table_name,)
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
        querys = SaInitializeQuery.DDL_SQL
        TargetUtils.create_and_check_table(self.logger, self.sa_conn, querys, None)

    def drop_table_for_sql_text_merge(self):
        query = SaSqlTextMergeQuery.DROP_TABLE_AE_SQL_TEXT

        try:
            cursor = self.sa_conn.cursor()
            cursor.execute(query)

        except Exception as e:
            self.logger.exception(e)
        finally:
            self.sa_conn.commit()

    def get_ae_was_sql_text(self, chunksize):
        query = SaSqlTextMergeQuery.SELECT_AE_WAS_SQL_TEXT
        conn = self.analysis_engine.connect().execution_options(stream_results=True,)
        return pd.read_sql_query(text(query), conn, chunksize=chunksize)

    def get_ae_db_sql_text_1seq(self, date, chunksize):
        replace_dict = {'s_date': date}
        query = SystemUtils.sql_replace_to_dict(SaSqlTextMergeQuery.SELECT_AE_DB_SQL_TEXT_1SEQ, replace_dict)

        return pd.read_sql_query(query, self.sa_conn, chunksize=chunksize)

    def get_ae_db_sql_text_by_1seq(self, df, chunksize):
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
        TargetUtils.insert_analysis_by_df(self.logger, self.analysis_engine, table_name, merged_df)

