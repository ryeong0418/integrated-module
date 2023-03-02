from typing import Optional
from sqlalchemy import Integer, String, Sequence
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

from src.common.enum_module import MessageEnum


class Base(DeclarativeBase):
    pass


class ExecuteLogModel(Base):

    __tablename__ = 'ae_execute_log'

    seq: Mapped[int] = mapped_column(Sequence('seq_execute_log_id'), primary_key=True)
    execute_name: Mapped[str] = mapped_column(String(20))
    execute_start_dt: Mapped[str] = mapped_column(String(14))
    execute_end_dt: Mapped[Optional[str]] = mapped_column(String(14))
    execute_elapsed_time: Mapped[Optional[int]] = mapped_column(Integer)
    execute_args: Mapped[Optional[str]] = mapped_column(String(100))
    result: Mapped[str] = mapped_column(String(1))
    result_code: Mapped[str] = mapped_column(String(4))
    result_msg: Mapped[str] = mapped_column(String(100))
    create_id: Mapped[str] = mapped_column(String(20))

    def __init__(self, execute_name, execute_start_dt, execute_args=''):
        """
        Execute Log를 저장하기 위한 Model
        :param execute_name: 수행 기능 명
        :param execute_start_dt: 수행 동작 시작 시간
        :param execute_args: 수행 동작 외부 파라미터
        """
        self.execute_name = execute_name
        self.execute_start_dt = execute_start_dt
        self.execute_end_dt = None
        self.execute_elapsed_time = 0
        self.execute_args = execute_args
        self.result = 'P'
        self.result_code = 'W001'
        self.result_msg = MessageEnum[self.result_code].value
        self.create_id = 'system'

    def __repr__(self):
        return "<ExecuteLogModel(seq='{}', execute_name='{}', execute_start_dt='{}', execute_end_dt='{}', " \
               "execute_elapsed_time='{}', execute_args='{}', " \
               "result='{}', result_code='{}', result_msg='{}', create_id='{}')>"\
                .format(self.seq, self.execute_name, self.execute_start_dt, self.execute_end_dt,
                        self.execute_elapsed_time, self.execute_args,
                        self.result, self.result_code, self.result_msg, self.create_id)


if __name__ == '__main__':
    from src.sql.database import DataBase
    from resources.config_manager import Config

    config = Config('local').get_config()

    db = DataBase(config)
    elm = ExecuteLogModel('Initialize', '202302280955', 'test')

    with db.session_scope() as session:
        session.add(elm)

    with db.session_scope() as session:
        session.query(ExecuteLogModel).filter(ExecuteLogModel.seq == f'{elm.seq}').update({
            'result': 'Y',
            'execute_end_dt': '202302281005',
            'execute_elapsed_time': 80,
            'result_code': 'I000'
        })
        session.commit()
