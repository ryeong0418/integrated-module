from src import common_module as cm


class SqlTextMerge(cm.CommonModule):

    def __init__(self, logger):
        self.logger = logger

    def main_process(self):
        self.logger.debug("SqlTextMerge main")