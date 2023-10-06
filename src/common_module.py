from src.analysis_target import SaTarget, InterMaxTarget, MaxGaugeTarget
from src.common.constants import SystemConstants
from src.common.enum_module import ModuleFactoryEnum


class CommonModule:
    def __init__(self, logger):
        self.logger = logger
        self.config: dict = None
        self.st = None
        self.imt = None
        self.mgt = None
        self.sql_file_root_path = None

    def set_config(self, config: dict):
        self.config = config

        self.sql_file_root_path = (
            f"{self.config['home']}/"
            f"{SystemConstants.SQL_ROOT_PATH}"
            f"{ModuleFactoryEnum[self.config['args']['proc']].value}/"
        )

    def main_process(self):
        pass

    def _init_sa_target(self):
        self.st = SaTarget(self.logger, self.config)
        self.st.init_process()

    def _init_im_target(self):
        self.imt = InterMaxTarget(self.logger, self.config)
        self.imt.init_process()

    def _init_mg_target(self):
        self.mgt = MaxGaugeTarget(self.logger, self.config)
        self.mgt.init_process()

    def _teardown_sa_target(self):
        del self.st

    def _teardown_im_target(self):
        del self.imt

    def _teardown_mg_target(self):
        del self.mgt
