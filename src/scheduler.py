import signal
import time
import os
import platform

from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler

from src import common_module as cm
from src.common.constants import SystemConstants
from resources.logger_manager import Logger


class Scheduler(cm.CommonModule):

    def __init__(self, logger):
        self.logger = logger
        self.scheduler_logger = None
        self.main_scheduler: BlockingScheduler = None
        self.bg_scheduler: BackgroundScheduler = None

    def __del__(self):
        if self.main_scheduler:
            self.main_scheduler.shutdown()
        if self.bg_scheduler:
            self.bg_scheduler.shutdown()

    def main_process(self):
        self._add_scheduler_logger()
        # self._set_signal()
        self._init_scheduler()

        self.logger.info(f"Background Scheduler job start")
        self._bg_scheduler_start()
        time.sleep(1)

        self.logger.info(f"Main Scheduler job start")

        try:
            self._main_scheduler_start()
        except Exception as e:
            self.logger.exception(e)

    def _bg_scheduler_start(self):
        self.bg_scheduler.add_job(
            self._is_alive_logging_job,
            'cron',
            # hour='*',
            minute='*',
            id='_is_alive_logging_job'
        )
        self.bg_scheduler.add_job(
            self._sql_text_merge_job,
            'cron',
            # hour='*',
            second='2',
            id='_sql_text_merge_job'
        )
        self.bg_scheduler.start()

    def _main_scheduler_start(self):
        self.main_scheduler.add_job(
            self._extract_summary_job,
            'cron',
            # hour='2',
            minute=20,
            id='_extract_summary_job'
        )
        self.main_scheduler.start()

    def _add_scheduler_logger(self):
        self.scheduler_logger = Logger(self.config['env']).\
            get_default_logger(self.config['log_dir'], SystemConstants.SCHEDULER_LOG_FILE_NAME)

    def _init_scheduler(self):
        self.main_scheduler = BlockingScheduler(timezone='Asia/Seoul')
        self.bg_scheduler = BackgroundScheduler(timezone='Asia/Seoul')

    def _set_signal(self):
        if platform.system().lower() in 'windows':
            signal.signal(signal.SIGBREAK, self._terminate)
            signal.signal(signal.SIGINT, self._terminate)

    def _terminate(self):
        self.logger.info("terminated")

    def _extract_summary_job(self):
        self.scheduler_logger.info(f"extract_summary_job start")
        time.sleep(5)
        self.scheduler_logger.info(f"extract_summary_job start")

    def _is_alive_logging_job(self):
        for job in self.main_scheduler.get_jobs():
            self.scheduler_logger.info("name: {}, trigger: {}, next run: {}".format(
                job.id,
                job.trigger,
                job.next_run_time,
            ))

        for job in self.bg_scheduler.get_jobs():
            if job.id == '_is_alive_logging_job':
                continue

            self.scheduler_logger.info("name: {}, trigger: {}, next run: {} ".format(
                job.id,
                job.trigger,
                job.next_run_time,
            ))

        self.scheduler_logger.info(f"This Module Scheduler is Alive.. PID : {os.getpid()} \n")

    def _sql_text_merge_job(self):
        self.scheduler_logger.info(f"_sql_text_merge_job start")
        time.sleep(5)
        self.scheduler_logger.info(f"_sql_text_merge_job end")
