from src import common_module as cm


class Extractor(cm.CommonModule):

    def __init__(self, logger):
        super().__init__(logger)

    def main_process(self):
        self.logger.debug('extractor')

        if self.config['intermax_repo']['use']:
            self.logger.debug("Intermax extractor")
            self._init_im_target()

            self.imt.insert_intermax_detail_data()

        if self.config['maxgauge_repo']['use']:
            self.logger.debug("maxgauge extractor")
            self._init_mg_target()

            self.mgt.insert_maxgauge_detail_data()
