from src import common_module as cm
from src.analysis_target import SaTarget


class Visualization(cm.CommonModule):

    def __init__(self, logger):
        self.logger = logger
        self.logger.info("visualization init")
        self.st = None

    def main_process(self):
        self.logger.debug('Visualization')
        self.st = SaTarget(self.logger, self.config)
        self.st.init_process()
        self.st.visualization_data()

