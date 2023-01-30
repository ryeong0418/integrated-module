import psycopg2 as db

from src.common.utils import TargetUtils
from sql.initialize_sql import InterMaxInitializeQuery, MaxGauseInitializeQuery, SaInitializeQuery


class CommonTarget:

    def __init__(self, logger, config):
        self.logger = logger
        self.config = config

        self.analysis_conn_str = TargetUtils.get_db_conn_str(config['analysis_repo'])
        self.im_conn_str = TargetUtils.get_db_conn_str(config['intermax_repo'])
        self.mg_conn_str = TargetUtils.get_db_conn_str(config['maxgauge_repo'])

        self.logger.info(f"analysis_repo DB 접속 정보 {self.analysis_conn_str}")
        self.logger.info(f"intermax_repo DB 접속 정보 {self.im_conn_str}")
        self.logger.info(f"maxgauge_repo DB 접속 정보 {self.mg_conn_str}")


class InterMaxTarget(CommonTarget):

    def create_table(self):
        conn = db.connect(self.analysis_conn_str)
        querys = InterMaxInitializeQuery.SQL
        check_query = InterMaxInitializeQuery.CHECK_SQL

        TargetUtils.create_and_check_table(self.logger, conn, querys, check_query)


class MaxGauseTarget(CommonTarget):

    def create_table(self):
        conn = db.connect(self.analysis_conn_str)
        querys = MaxGauseInitializeQuery.SQL
        check_query = MaxGauseInitializeQuery.CHECK_SQL

        TargetUtils.create_and_check_table(self.logger, conn, querys, check_query)


class SaTarget(CommonTarget):

    def create_table(self):
        conn = db.connect(self.analysis_conn_str)
        querys = SaInitializeQuery.SQL

        TargetUtils.create_and_check_table(self.logger, conn, querys, None)
