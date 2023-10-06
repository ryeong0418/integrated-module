import os
import logging.config
import json
from pathlib import Path

from src.common.constants import SystemConstants as sc
from concurrent_log_handler import ConcurrentTimedRotatingFileHandler


class Logger(ConcurrentTimedRotatingFileHandler):
    def __init__(self, env="prod"):
        self.resources_path = os.path.dirname(os.path.abspath(__file__))
        self.env = env

    def get_default_logger(self, log_dir, log_file_name, error_log_dict=None):
        """
        logger를 할당 받기 위한 함수
        :param log_dir: 로그 파일 생성 path
        :param log_file_name: 로그 파일 이름
        :param error_log_dict: 해당 로그에서 에러 레벨 이상만 추출하기 위한 dict (optional)
        :return: logger
        """

        Path(log_dir).mkdir(exist_ok=True, parents=True)

        logger_path = [self.resources_path, sc.LOGGER_FILE_PATH]

        with open(f"{os.path.join(*logger_path)}/{sc.LOGGER_FILE_PREFIX}{self.env}{sc.LOGGER_FILE_SUFFIX}", "r") as f:
            logger_dict = json.load(f)

        logger_dict["handlers"]["file"]["filename"] = str(Path(log_dir) / f"{log_file_name}.log")

        handlers = list()
        handlers.append("file")

        if self.env == "local":
            handlers.append("console")

        # error_log_dict 없으면 생성 안하게 설정
        # if error_log_dict is not None:
        #     Path(error_log_dict["log_dir"]).mkdir(exist_ok=True, parents=True)
        #     error_filename = str(Path(error_log_dict["log_dir"]) / f"{error_log_dict['file_name']}.log")
        #     self.logger_config["handlers"]["error"]["filename"] = error_filename
        #     handlers.append("error")

        logger_dict["loggers"][f"{log_file_name}"] = dict()
        logger_dict["loggers"][f"{log_file_name}"]["handlers"] = handlers

        logging.config.dictConfig(logger_dict)
        logger = logging.getLogger(log_file_name)

        return logger
