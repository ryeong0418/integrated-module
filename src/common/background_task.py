import threading


class BackgroundTask(threading.Thread):

    def __init__(self, logger, func, **kwargs):
        super().__init__()
        self.logger = logger
        self.func = func
        self.kwargs = kwargs

    def run(self) -> None:
        self.logger.info(f"{self.func.__name__} func execute by thread. kwargs keys : {self.kwargs.keys()}")

        if self.func.__name__ == 'update_cluster_id_by_sql_id':
            self.func(self.kwargs['df'])

