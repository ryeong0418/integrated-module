from src import common_module as cm
from src.analysis_target import SaTarget


class Summarizer(cm.CommonModule):

    def __init__(self, logger):
        self.logger = logger
        self.st = None

    def main_process(self):
        self.logger.debug('Summarizer')
        self.st = SaTarget(self.logger, self.config)
        self.st.init_process()
        self.st.ae_txn_detail_summary_temp_create_table()
        self.st.ae_txn_sql_detail_summary_temp_create_table()
        self.st.summary_join()
