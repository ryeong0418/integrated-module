from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from src.common.utils import TargetUtils


class DataBase:

    def __init__(self, config):
        self.engine = create_engine(TargetUtils.get_engine_template(config['analysis_repo']), echo=True)
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
