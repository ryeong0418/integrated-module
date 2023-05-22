import time

from src import common_module as cm


class Initialize(cm.CommonModule):

    def __init__(self, logger):
        super().__init__(logger)

    def main_process(self):
        self.logger.debug("SaTarget init")
        self._init_sa_target()
        self.st.create_table()

        if self.config['intermax_repo']['use']:
            self.logger.debug("InterMaxTarget init")
            self._init_im_target()

            self.imt.insert_intermax_meta()
            time.sleep(1)

        if self.config['maxgauge_repo']['use']:
            self.logger.debug("MaxGaugeTarget init")
            self._init_mg_target()

            self.mgt.insert_maxgauge_meta()
