from src import common_module as cm


class Extractor(cm.CommonModule):

    def __init__(self, logger):
        self.logger = logger
        self.logger.info("ExtractMain init")

