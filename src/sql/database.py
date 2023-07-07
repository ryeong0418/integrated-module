from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from src.common.utils import TargetUtils


class DataBase:

    def __init__(self, config):
        self.config = config
        self.engine = None
        self.sql_debug_flag = config['sql_debug_flag'] if config['sql_debug_flag'] is not None else True

    def create_engine(self):
        self.engine = create_engine(
            TargetUtils.get_engine_template(self.config['analysis_repo']),
            echo=self.sql_debug_flag,
            pool_size=20,
            max_overflow=20,
            echo_pool=self.sql_debug_flag,
            pool_pre_ping=True,
            connect_args={
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
            },
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
