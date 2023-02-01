import psycopg2 as db

from sqlalchemy import create_engine

from src.common.utils import TargetUtils
from src.common.constants import TableConstants
from sql.initialize_sql import InterMaxInitializeQuery, MaxGaugeInitializeQuery, SaInitializeQuery


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


class InterMaxTarget(CommonTarget):

    def init_process(self):
        self.im_conn = db.connect(self.im_conn_str)
        self.analysis_engine = create_engine(self.analysis_engine_template)

    def __del__(self):
        if self.im_conn:
            self.im_conn.close()
        if self.analysis_engine:
            self.analysis_engine.dispose()

    def create_table(self):
        conn = db.connect(self.analysis_conn_str)
        querys = InterMaxInitializeQuery.DDL_SQL
        check_query = InterMaxInitializeQuery.CHECK_SQL

        TargetUtils.create_and_check_table(self.logger, conn, querys, check_query)

    def insert_intermax_meta(self):
        self._insert_xapm_was_info()

        self._insert_xapm_txn_name()

        self._insert_xapm_sql_text()

        self._insert_xapm_db_info()

    def _insert_xapm_was_info(self):

        query = InterMaxInitializeQuery.SELECT_XAPM_WAS_INFO
        table_name = TableConstants.AE_WAS_INFO

        TargetUtils.insert_meta_data(self.logger, self.im_conn, self.analysis_engine, table_name, query,)

    def _insert_xapm_txn_name(self):
        query = InterMaxInitializeQuery.SELECT_XAPM_TXN_NAME
        table_name = TableConstants.AE_TXN_NAME

        TargetUtils.insert_meta_data(self.logger, self.im_conn, self.analysis_engine, table_name, query,)

    def _insert_xapm_sql_text(self):
        query = InterMaxInitializeQuery.SELECT_XAPM_SQL_TEXT
        table_name = TableConstants.AE_WAS_SQL_TEXT

        TargetUtils.insert_meta_data(self.logger, self.im_conn, self.analysis_engine, table_name, query,)

    def _insert_xapm_db_info(self):
        query = InterMaxInitializeQuery.SELECT_XAPM_DB_INFO
        table_name = TableConstants.AE_WAS_DB_INFO

        TargetUtils.insert_meta_data(self.logger, self.im_conn, self.analysis_engine, table_name, query,)


class MaxGaugeTarget(CommonTarget):

    def init_process(self):
        self.mg_conn = db.connect(self.mg_conn_str)
        self.analysis_engine = create_engine(self.analysis_engine_template)

    def __del__(self):
        if self.mg_conn:
            self.mg_conn.close()
        if self.analysis_engine:
            self.analysis_engine.dispose()

    def create_table(self):
        conn = db.connect(self.analysis_conn_str)
        querys = MaxGaugeInitializeQuery.DDL_SQL
        check_query = MaxGaugeInitializeQuery.CHECK_SQL

        TargetUtils.create_and_check_table(self.logger, conn, querys, check_query)

    def insert_maxgauge_meta(self):
        self._insert_ae_db_info()

    def _insert_ae_db_info(self):
        query = MaxGaugeInitializeQuery.SELECT_APM_DB_INFO
        table_name = TableConstants.AE_DB_INFO

        TargetUtils.insert_meta_data(self.logger, self.mg_conn, self.analysis_engine, table_name, query,)


class SaTarget(CommonTarget):

    def init_process(self):
        self.sa_conn = db.connect(self.analysis_conn_str)

        self.logger.info(f"analysis_repo DB 접속 정보 {self.analysis_conn_str}")
        self.logger.info(f"intermax_repo DB 접속 정보 {self.im_conn_str}")
        self.logger.info(f"maxgauge_repo DB 접속 정보 {self.mg_conn_str}")

    def __del__(self):
        if self.sa_conn:
            self.sa_conn.close()

    def create_table(self):
        querys = SaInitializeQuery.DDL_SQL

        TargetUtils.create_and_check_table(self.logger, self.sa_conn, querys, None)
