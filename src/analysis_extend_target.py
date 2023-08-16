import cx_Oracle

from cx_Oracle import ProgrammingError
from pathlib import Path

from src.analysis_target import CommonTarget
from src.common.utils import TargetUtils
from src.common.constants import SystemConstants


class OracleTarget(CommonTarget):
    """
    Extend 분석용 Oracle Target Class
    """

    def __init__(self, logger, config):
        super().__init__(logger=logger, config=config)

        self.oracle_engine = None
        self.oracle_conn = None
        self.ora_extend_url_object, self.ora_extend_conn_args = None, None
        self.identifier = None
        oracle_client_path = f"{Path(self.config['home']).parent}/{SystemConstants.ORACLE_CLIENT_PATH}"
        try:
            cx_Oracle.init_oracle_client(lib_dir=oracle_client_path)
        except ProgrammingError:
            self.logger.warn("Oracle init client exist")

    def set_extend_target_config(self, extend_target_repo):
        """
        extend target config 설정 함수.
        :param extend_target_repo: 확장 분석 타겟 repo 정보
        :return:
        """
        self.ora_extend_url_object, self.ora_extend_conn_args = TargetUtils.set_engine_param(extend_target_repo, True)
        self.identifier = (
            extend_target_repo["service_name"]
            if str(extend_target_repo["service_name"]).strip() != ""
            else str(extend_target_repo["sid"])
        )

    def init_process(self):
        """
        extend target db init 함수
        """
        self.oracle_engine = self._create_engine(self.ora_extend_url_object, self.ora_extend_conn_args)
        self.oracle_conn = self.oracle_engine.raw_connection()

    def get_data_by_query(self, query, chunksize=0, coerce=True):
        """
        테이블 조회 함수.
        :param query: 조회하려는 쿼리
        :param chunksize: 조회하려는 chunksize
        :param coerce: float 데이터 처리 flag
        :return: chunksize 만큼 조회된 데이터 데이터 프레임
        """
        return self._get_df_by_chunk_size(self.oracle_engine, query, chunk_size=chunksize, coerce=coerce)

    def teardown_oracle_extend_target(self):
        """
        extend target object close
        """
        if self.oracle_engine:
            self.oracle_engine.dispose()
        if self.oracle_conn:
            self.oracle_conn.close()
