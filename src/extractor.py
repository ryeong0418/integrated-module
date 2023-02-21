from src import common_module as cm
from src.analysis_target import InterMaxTarget, MaxGaugeTarget, SaTarget


class Extractor(cm.CommonModule):

    def __init__(self, logger):
        self.logger = logger
        self.imt = None
        self.mgt = None


    def main_process(self):
        self.logger.debug('extractor')

        if self.config['intermax_repo']['use']:
            self.logger.debug("Intermax extractor")
            self.imt = InterMaxTarget(self.logger, self.config)
            self.imt.init_process()
            self.imt.insert_intermax_detail_data()

        if self.config['maxgauge_repo']['use']:
            self.logger.debug("maxgauge extractor")
            self.mgt = MaxGaugeTarget(self.logger, self.config)
            self.mgt.init_process()
            self.mgt.insert_maxgauge_detail_data()



