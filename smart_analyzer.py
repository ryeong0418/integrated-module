import os
import sys
import time

from pprint import pformat
from pathlib import Path

from src.common.utils import SystemUtils
from src.common.constants import SystemConstants
from src.common.timelogger import TimeLogger
from src.common.enum import ModuleFactoryEnum

from src.module_factory import ModuleFactory
from src.sql.crud import DataBase
from src.sql.model import ExecuteLogModel

from resources.config_manager import Config
from resources.logger_manager import Logger


def main_process():
    start_tm = time.time()
    sys.setrecursionlimit(10**7)

    home = os.path.dirname(os.path.abspath(__file__))

    env = SystemUtils.get_environment_variable()

    log_dir = str(Path(home) / SystemConstants.LOGGER_PATH)

    logger = Logger(env).get_default_logger(log_dir, SystemConstants.MASTER_LOG_FILE_NAME)
    args = SystemUtils.get_start_args()
    process = args.proc

    config = Config(env).get_config()

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

    result = 'N'
    result_code = 'E999'

    try:
        fmf = ModuleFactory(logger)
        instance = fmf.get_module_instance(process)
        instance.set_config(config)

        with TimeLogger(f'{instance.__class__.__name__}', logger):
            instance.main_process()

        result = 'Y'
        result_code = 'I000'

    except Exception as e:
        logger.exception(e)

    finally:
        result_dict = dict()
        result_dict['result'] = result
        result_dict['execute_end_dt'] = SystemUtils.get_now_timestamp()
        result_dict['execute_elapsed_time'] = time.time() - start_tm
        result_dict['result_code'] = result_code

        if process != 'i':
            with db.session_scope() as session:
                session.query(ExecuteLogModel).filter(ExecuteLogModel.seq == f'{elm.seq}').update(result_dict)
                session.commit()


if __name__ == "__main__":

    main_process()
