from src.analysis_target import SaTarget


class CommonModule:

    def __init__(self, logger):
        self.config: dict = None
        self.st = None

    def set_config(self, config: dict):
        self.config = config

    def main_process(self):
        pass

    def _init_sa_target(self):
        self.st = SaTarget(self.logger, self.config)
        self.st.init_process()


