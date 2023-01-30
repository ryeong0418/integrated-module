

class CommonModule:

    def __init__(self, logger):
        self.config: dict = None

    def set_config(self, config: dict):
        self.config = config

    def main_process(self):
        pass

