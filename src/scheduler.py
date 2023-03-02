import signal
import time
import os
import platform

from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler

from src import common_module as cm
from src.sql.database import DataBase
from src.sql.model import ExecuteLogModel
from src.extractor import Extractor
from src.summarizer import Summarizer
from src.sql_text_merge import SqlTextMerge
from src.common.constants import SystemConstants, ResultConstants
from src.common.utils import SystemUtils
from src.common.enum_module import ModuleFactoryEnum, MessageEnum
from resources.logger_manager import Logger

CRON = 'cron'


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

        try:
            self.logger.info(f"Background Scheduler job start")
            self._bg_scheduler_start()
            time.sleep(1)

            self.logger.info(f"Main Scheduler job start")
            self._main_scheduler_start()
        except Exception as e:
            self.logger.exception(e)

    def _bg_scheduler_start(self):
        self.bg_scheduler.add_job(
            self._is_alive_logging_job,
            CRON,
            hour=self.config['scheduler']['is_alive_sched']['hour'],
            minute=self.config['scheduler']['is_alive_sched']['minute'],
            id='_is_alive_logging_job'
        )
        # self.bg_scheduler.add_job(
        #     self._sql_text_merge_job,
        #     CRON,
        #     # hour='*',
        #     second='2',
        #     id='_sql_text_merge_job'
        # )
        self.bg_scheduler.start()

    def _main_scheduler_start(self):
        self.main_scheduler.add_job(
            self._main_job,
            CRON,
            hour=self.config['scheduler']['main_sched']['hour'],
            minute=self.config['scheduler']['main_sched']['minute'],
            id='_extract_summary_job'
        )
        self.scheduler_logger.info(f"Main scheduler start and set config cron expression - "
                                   f"{self.config['scheduler']['main_sched']['hour']} hour "
                                   f"{self.config['scheduler']['main_sched']['minute']} minute")
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
        self.bg_scheduler.shutdown()
        self.main_scheduler.shutdown()

    def _main_job(self):
        start_tm = time.time()

        db = DataBase(self.config)
        elm = ExecuteLogModel(ModuleFactoryEnum[self.config['args']['proc']].value,
                              SystemUtils.get_now_timestamp(), str(self.config['args']), 'batch')

        with db.session_scope() as session:
            session.add(elm)

        result = ResultConstants.FAIL
        self._update_config_custom_values()

        try:
            self._extractor_job()

            self._summarizer_job()

            self._sql_text_merge_job()

            result = ResultConstants.SUCCESS
            result_code = 'I001'
            result_msg = MessageEnum[result_code].value
        except Exception as e:
            self.logger.exception(e)
            result = ResultConstants.ERROR
            result_code = 'E999'
            result_msg = str(e)
        finally:
            result_dict = SystemUtils.set_update_execute_log(result, start_tm, result_code, result_msg)

            with db.session_scope() as session:
                session.query(ExecuteLogModel).filter(ExecuteLogModel.seq == f'{elm.seq}').update(result_dict)
                session.commit()

    def _extractor_job(self):
        self.scheduler_logger.info(f"_extractor_job start")

        extractor = Extractor(self.scheduler_logger)
        extractor.set_config(self.config)
        extractor.main_process()

        self.scheduler_logger.info(f"_extractor_job end")

    def _summarizer_job(self):
        self.scheduler_logger.info(f"_summarizer_job start")

        summarizer = Summarizer(self.scheduler_logger)
        summarizer.set_config(self.config)
        summarizer.main_process()

        self.scheduler_logger.info(f"_summarizer_job end")

    def _sql_text_merge_job(self):
        self.scheduler_logger.info(f"_sql_text_merge_job start")

        stm = SqlTextMerge(self.scheduler_logger)
        stm.set_config(self.config)
        stm.main_process()

        self.scheduler_logger.info(f"_sql_text_merge_job end")

    def _update_config_custom_values(self):
        custom_values = dict()
        custom_values['args'] = {'s_date': SystemUtils.get_date_by_interval(-1, fmt="%Y%m%d"), 'interval': 1}
        self.config.update(custom_values)

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

        self.scheduler_logger.info(f"This Analysis Module Scheduler is Alive.. PID : {os.getpid()} \n")
