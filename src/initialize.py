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

        if self.config['intermax_repo']['use']:
            self.logger.debug("InterMaxTarget init")
            self.imt = InterMaxTarget(self.logger, self.config)
            self.imt.create_table()
            time.sleep(1)

            self.imt.init_process()
            self.imt.insert_intermax_meta()

        if self.config['maxgauge_repo']['use']:
            self.logger.debug("MaxGaugeTarget init")
            self.mgt = MaxGaugeTarget(self.logger, self.config)
            self.mgt.create_table()
            time.sleep(1)

            self.mgt.init_process()
            self.mgt.insert_maxgauge_meta()
