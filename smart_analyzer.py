import os
import sys
import time

from pprint import pformat
from pathlib import Path

from src.common.utils import SystemUtils
from src.common.constants import SystemConstants, ResultConstants
from src.common.timelogger import TimeLogger
from src.common.enum_module import ModuleFactoryEnum, MessageEnum

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
    if process != 'i' and process != 'b':
        db = DataBase(config)
        elm = ExecuteLogModel(ModuleFactoryEnum[process].value, SystemUtils.get_now_timestamp(), str(vars(args)))

        with db.session_scope() as session:
            session.add(elm)
    elif process == 'b':
        Path(f"{home}/{SystemConstants.TMP_PATH}").mkdir(exist_ok=True, parents=True)
        pid_tmp_file = f"{home}/{SystemConstants.TMP_PATH}/{SystemConstants.PID_TMP_FILE_NAME}"
        Path(pid_tmp_file).touch(exist_ok=True)

        with open(pid_tmp_file, 'w') as f:
            f.write(str(os.getpid()))

    config['args'] = vars(args)

    config['log_dir'] = log_dir
    config['env'] = env
    config['home'] = home

    logger.info("*" * 79)
    logger.info(f"Module config :\n {pformat(config)}")
    logger.info("*" * 79)

    result = ResultConstants.FAIL

    try:
        fmf = ModuleFactory(logger)
        instance = fmf.get_module_instance(ModuleFactoryEnum[process].value)
        instance.set_config(config)

        with TimeLogger(f'{instance.__class__.__name__}', logger):
            instance.main_process()

        result = ResultConstants.SUCCESS
        result_code = 'I001'
        result_msg = MessageEnum[result_code].value

    except Exception as e:
        logger.exception(e)
        result = ResultConstants.ERROR
        result_code = 'E999'
        result_msg = str(e)[:2000]

    finally:
        result_dict = SystemUtils.set_update_execute_log(result, start_tm, result_code, result_msg)

        if process != 'i' and process != 'b':
            with db.session_scope() as session:
                session.query(ExecuteLogModel).filter(ExecuteLogModel.seq == f'{elm.seq}').update(result_dict)
                session.commit()


if __name__ == "__main__":

    main_process()
