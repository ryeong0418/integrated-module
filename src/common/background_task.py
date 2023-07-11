import threading


class BackgroundTask(threading.Thread):
    """
    Thread를 이용한 Background Task 수행 Class
    """

    def __init__(self, logger, func, **kwargs):
        super().__init__()
        self.logger = logger
        self.func = func
        self.kwargs = kwargs

    def run(self) -> None:
        """
        class 생성 시 받은 인자의 함수와 함수인자로 해당 func를 실행하는 함수
        :return: 
        """
        self.logger.info(f"{self.func.__name__} func execute by thread. kwargs keys : {self.kwargs.keys()}")

        if self.func.__name__ in ('update_cluster_id_by_sql_id', 'upsert_cluster_id_by_sql_uid'):
            self.func(self.kwargs['df'])
