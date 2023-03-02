import os
import sys
import time

from pprint import pformat
from pathlib import Path

from src.common.utils import SystemUtils
from src.common.constants import SystemConstants
from src.common.timelogger import TimeLogger
from src.common.enum_module import ModuleFactoryEnum
from src.common.enum_module import MessageEnum

from src.module_factory import ModuleFactory
from src.sql.database import DataBase
from src.sql.model import ExecuteLogModel

from resources.config_manager import Config
from resources.logger_manager import Logger


def main_process():
    sys.setrecursionlimit(10**7)

    home = os.path.dirname(os.path.abspath(__file__))

    env = SystemUtils.get_environment_variable()

    log_dir = str(Path(home) / SystemConstants.LOGGER_PATH)

    logger = Logger(env).get_default_logger(log_dir, SystemConstants.MASTER_LOG_FILE_NAME)
    args = SystemUtils.get_start_args()
    process = args.proc

    config = Config(env).get_config()

    start_tm = time.time()
    if process != 'i':
        db = DataBase(config)
        elm = ExecuteLogModel(ModuleFactoryEnum[process].value, SystemUtils.get_now_timestamp(), str(vars(args)))

        with db.session_scope() as session:
            session.add(elm)

    config['args'] = vars(args)

    config['log_dir'] = log_dir
    config['env'] = env
    config['home'] = home

    logger.info("*" * 79)
    logger.info(f"Module config :\n {pformat(config)}")
    logger.info("*" * 79)

    result = 'F'

    try:
        fmf = ModuleFactory(logger)
        instance = fmf.get_module_instance(ModuleFactoryEnum[process].value)
        instance.set_config(config)

        with TimeLogger(f'{instance.__class__.__name__}', logger):
            instance.main_process()

        result = 'S'
        result_code = 'I001'
        result_msg = MessageEnum[result_code].value

    except Exception as e:
        logger.exception(e)
        result = 'E'
        result_code = 'E999'
        result_msg = str(e)

    finally:
        result_dict = SystemUtils.set_update_execute_log(result, start_tm, result_code, result_msg)

        if process != 'i':
            with db.session_scope() as session:
                session.query(ExecuteLogModel).filter(ExecuteLogModel.seq == f'{elm.seq}').update(result_dict)
                session.commit()


if __name__ == "__main__":

    main_process()
