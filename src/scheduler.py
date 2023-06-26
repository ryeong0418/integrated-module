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
from src.common.module_exception import ModuleException
from resources.logger_manager import Logger

CRON = 'cron'


class Scheduler(cm.CommonModule):

    def __init__(self, logger):
        super().__init__(logger)
        self.scheduler_logger = None
        self.block_scheduler: BlockingScheduler = None
        self.bg_scheduler: BackgroundScheduler = None

    def __del__(self):
        if self.block_scheduler:
            self.block_scheduler.shutdown()
        if self.bg_scheduler:
            self.bg_scheduler.shutdown()

    def main_process(self):
        self._add_scheduler_logger()
        # self._set_signal()
        self._init_scheduler()

        try:
            self.logger.info(f"Background Scheduler job start")
            # self._bg_scheduler_start()
            time.sleep(1)

            self.logger.info(f"Blocking Scheduler job start")
            self._block_scheduler_start()
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
        self.bg_scheduler.add_job(
            self._main_job,
            CRON,
            hour=self.config['scheduler']['main_sched']['hour'],
            minute=self.config['scheduler']['main_sched']['minute'],
            id='_extract_summary_job'
        )
        self.bg_scheduler.start()

    def _block_scheduler_start(self):
        self.block_scheduler.add_job(
            self._block_scheduler_job,
            CRON,
            hour=self.config['scheduler']['main_sched']['hour'],
            minute=self.config['scheduler']['main_sched']['minute'],
            id='_block_scheduler_job'
        )
        self.scheduler_logger.info(f"Main scheduler start and set config cron expression - "
                                   f"{self.config['scheduler']['main_sched']['hour']} hour "
                                   f"{self.config['scheduler']['main_sched']['minute']} minute")

        self.block_scheduler.start()
        self.logger.info(f"End of Scheduler start")

    def _block_scheduler_job(self):
        self.scheduler_logger.info("_block_scheduler_job")

    def _add_scheduler_logger(self):
        self.scheduler_logger = Logger(self.config['env']).\
            get_default_logger(self.config['log_dir'], SystemConstants.SCHEDULER_LOG_FILE_NAME)

    def _init_scheduler(self):
        self.block_scheduler = BlockingScheduler(timezone='Asia/Seoul')
        self.bg_scheduler = BackgroundScheduler(timezone='Asia/Seoul')

    def _set_signal(self):
        if platform.system().lower() in 'windows':
            signal.signal(signal.SIGBREAK, self._terminate)
            signal.signal(signal.SIGINT, self._terminate)

    def _terminate(self):
        self.logger.info("terminated")
        self.bg_scheduler.shutdown()
        self.block_scheduler.shutdown()

    def _main_job(self):
        self.scheduler_logger.info("main_job start")
        start_tm = time.time()

        result = ResultConstants.FAIL
        result_code = "E001"
        result_msg = MessageEnum[result_code].value

        try:
            db = DataBase(self.config)
            elm = ExecuteLogModel(ModuleFactoryEnum[self.config['args']['proc']].value,
                                  SystemUtils.get_now_timestamp(), str(self.config['args']), 'batch')

            with db.session_scope() as session:
                session.add(elm)

            self._extractor_job()

            self._summarizer_job()

            self._sql_text_merge_job()

            self._update_config_custom_values(proc='b')

            result = ResultConstants.SUCCESS
            result_code = 'I001'
            result_msg = MessageEnum[result_code].value

        except ModuleException as me:
            self.logger.error(me.error_msg)
            result = ResultConstants.ERROR
            result_code = me.error_code
            result_msg = me.error_msg

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

        self.scheduler_logger.info("main_job end")
        return

    def _extractor_job(self):
        self.scheduler_logger.info(f"_extractor_job start")

        self._update_config_custom_values(proc='e')

        extractor = Extractor(self.scheduler_logger)
        extractor.set_config(self.config)
        extractor.main_process()

        self.scheduler_logger.info(f"_extractor_job end")

    def _summarizer_job(self):
        self.scheduler_logger.info(f"_summarizer_job start")

        self._update_config_custom_values(proc='s')

        summarizer = Summarizer(self.scheduler_logger)
        summarizer.set_config(self.config)
        summarizer.main_process()

        self.scheduler_logger.info(f"_summarizer_job end")

    def _sql_text_merge_job(self):
        self.scheduler_logger.info(f"_sql_text_merge_job start")

        self._update_config_custom_values(proc='m')

        stm = SqlTextMerge(self.scheduler_logger)
        stm.set_config(self.config)
        stm.main_process()

        self.scheduler_logger.info(f"_sql_text_merge_job end")

    def _update_config_custom_values(self, proc):
        custom_values = dict()
        custom_values['args'] = {'s_date': SystemUtils.get_date_by_interval(-1, fmt="%Y%m%d"), 'interval': 1, 'proc': proc}
        self.config.update(custom_values)

    def _is_alive_logging_job(self):
        # for job in self.block_scheduler.get_jobs():
        #     self.scheduler_logger.info("name: {}, trigger: {}, next run: {}".format(
        #         job.id,
        #         job.trigger,
        #         job.next_run_time,
        #     ))

        for job in self.bg_scheduler.get_jobs():
            if job.id == '_is_alive_logging_job':
                continue

            self.scheduler_logger.info("name: {}, trigger: {}, next run: {} ".format(
                job.id,
                job.trigger,
                job.next_run_time,
            ))

        self.scheduler_logger.info(f"This Analysis Module Scheduler is Alive.. PID : {os.getpid()} \n")
