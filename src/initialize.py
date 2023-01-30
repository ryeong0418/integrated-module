from src import common_module as cm
from src.analysis_target import InterMaxTarget, MaxGauseTarget, SaTarget


class Initialize(cm.CommonModule):

    def __init__(self, logger):
        self.logger = logger
        self.st = None
        self.imt = None
        self.mgt = None

    def main_process(self):
        self.logger.debug("SaTarget init")
        self.st = SaTarget(self.logger, self.config)
        self.st.create_table()

        if self.config['intermax_repo']['use']:
            self.logger.debug("InterMaxTarget init")
            self.imt = InterMaxTarget(self.logger, self.config)
            self.imt.create_table()

            # self._insert_intermax_meta()

        if self.config['maxgauge_repo']['use']:
            self.logger.debug("MaxGauseTarget init")
            self.mgt = MaxGauseTarget(self.logger, self.config)
            self.mgt.create_table()

            # self._insert_maxgauge_meta()

    def _insert_intermax_meta(self):
        self.imt.insert_xapm_was_info()

        self.imt.insert_xapm_txn_name()

        self.imt.insert_xapm_sql_text()

        self.imt.insert_xapm_db_info()

    def _insert_maxgauge_meta(self):
        self.mgt.insert_ap_db_info()


