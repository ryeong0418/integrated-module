from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from src.common.utils import TargetUtils

#https://stackoverflow.com/questions/55457069/how-to-fix-operationalerror-psycopg2-operationalerror-server-closed-the-conn


class DataBase:

    def __init__(self, config):
        sql_debug_flag = config['sql_debug_flag'] if config['sql_debug_flag'] is not None else True

        self.engine = create_engine(TargetUtils.get_engine_template(config['analysis_repo']), echo=sql_debug_flag,
                                    pool_size=20, max_overflow=20)

        self.Session = sessionmaker(bind=self.engine)

    @contextmanager
    def session_scope(self):
        session = self.Session()
        session.expire_on_commit = False
        try:
            yield session
            session.commit()

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
