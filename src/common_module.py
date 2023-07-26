from src.analysis_target import CommonTarget,SaTarget, InterMaxTarget, MaxGaugeTarget
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

        self.sql_file_root_path = f"{self.config['home']}/" \
                                  f"{SystemConstants.SQL_ROOT_PATH}" \
                                  f"{ModuleFactoryEnum[self.config['args']['proc']].value}/"

    def main_process(self):
        pass

    def common_target(self):
        return CommonTarget(self.logger, self.config)

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

    def _get_im_engine(self):
        return self.imt.get_im_engine()

    def _get_mg_engine(self):
        return self.mgt.get_mg_engine()

    def _get_st_engine(self):
        return self.st.get_st_engine()

    def _get_im_conn(self):
        return self.imt.get_im_conn()

    def _get_mg_conn(self):
        return self.mgt.get_mg_conn()

    def _get_st_conn(self):
        return self.st.get_st_conn()
