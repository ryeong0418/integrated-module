import os
import json

from src.common.constants import SystemConstants as sc


class Config:

    def __init__(self, env='prod'):
        self.resources_path = os.path.dirname(os.path.abspath(__file__))
        self.env = env

    def get_config(self) -> dict:
        """
        분석 데이터 타겟 DB 정보 및 분석 데이터 저장 DB 정보 설정값
        :return: config
        """
        config_path = [self.resources_path, sc.CONFIG_FILE_PATH]

        with open(f'{os.path.join(*config_path)}/{sc.CONFIG_FILE_PREFIX}{self.env}{sc.CONFIG_FILE_SUFFIX}', 'r') as f:
            config = json.load(f)

        return config
