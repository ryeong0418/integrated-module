import os

from pprint import pformat
from pathlib import Path

from src.common.utils import SystemUtils
from src.common.constants import SystemConstants
from src.common.timelogger import TimeLogger
from src.module_factory import ModuleFactory
from resources.config_manager import Config
from resources.logger_manager import Logger


def main_process():
    home = os.path.dirname(os.path.abspath(__file__))

    env = SystemUtils.get_environment_variable()

    log_dir = str(Path(home) / SystemConstants.LOGGER_PATH)

    logger = Logger(env).get_default_logger(log_dir, SystemConstants.MASTER_LOG_FILE_NAME)
    args = SystemUtils.get_start_args()
    process = args.proc

    config = Config(env).get_config()
    config['args'] = vars(args)

    logger.info("*" * 79)
    logger.info(f"Module config :\n {pformat(config)}")
    logger.info("*" * 79)

    try:
        fmf = ModuleFactory(logger)
        instance = fmf.get_module_instance(process)
        instance.set_config(config)

        with TimeLogger(f'{instance.__class__.__name__}', logger):
            instance.main_process()

    except Exception as e:
        logger.exception(e)

    finally:
        pass


if __name__ == "__main__":

    main_process()
