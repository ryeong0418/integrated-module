from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from src.common.utils import TargetUtils


class DataBase:
    def __init__(self, config):
        self.config = config
        self.engine = None
        self.sql_debug_flag = config["sql_debug_flag"] if config["sql_debug_flag"] is not None else True

        self.analysis_url_object, self.analysis_conn_args = TargetUtils.set_engine_param(config["analysis_repo"])

    def create_engine(self):
        self.engine = create_engine(
            self.analysis_url_object,
            echo=self.sql_debug_flag,
            pool_size=20,
            max_overflow=20,
            echo_pool=self.sql_debug_flag,
            pool_pre_ping=True,
            connect_args=self.analysis_conn_args,
        )

    def engine_dispose(self):
        self.engine.dispose()

    @contextmanager
    def session_scope(self):
        session = sessionmaker(bind=self.engine)()
        session.expire_on_commit = False
        try:
            yield session
            session.commit()

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
