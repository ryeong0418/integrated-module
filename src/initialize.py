import time

from src import common_module as cm
from src.analysis_target import InterMaxTarget, MaxGaugeTarget, SaTarget


class Initialize(cm.CommonModule):

    def __init__(self, logger):
        self.logger = logger
        self.st = None
        self.imt = None
        self.mgt = None

    def main_process(self):
        self.logger.debug("SaTarget init")
        self.st = SaTarget(self.logger, self.config)
        self.st.init_process()
        self.st.create_table()
        self.st.ae_was_sql_text_meta()
