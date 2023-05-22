from src import common_module as cm


class Summarizer(cm.CommonModule):

    def __init__(self, logger):
        super().__init__(logger)

    def main_process(self):
        self.logger.debug('Summarizer')
        self._init_sa_target()

        self.st.create_temp_table()
        self.st.summary_join()
