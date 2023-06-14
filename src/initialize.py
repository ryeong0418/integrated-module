from src import common_module as cm


class Initialize(cm.CommonModule):

    def __init__(self, logger):
        super().__init__(logger)

    def main_process(self):
        self.logger.debug("SaTarget init")
        self._init_sa_target()
        self.st.create_table()
        self.st.ae_was_sql_text_meta()
