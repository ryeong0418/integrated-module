import copy
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
from src.dynamic_sql_search import DynamicSqlSearch
from src.sql_text_similar import SqlTextSimilar
from src.common.constants import SystemConstants, ResultConstants
from src.common.utils import SystemUtils, DateUtils
from src.common.enum_module import ModuleFactoryEnum, MessageEnum
from src.common.module_exception import ModuleException
from resources.logger_manager import Logger

CRON = "cron"


class Scheduler(cm.CommonModule):
    """
    Scheduler Class

    python 스케쥴러 기능을 활용하기 위한 Class
    """

    def __init__(self, logger):
        super().__init__(logger)
        self.scheduler_logger = None
        self.block_scheduler: BlockingScheduler = None
        self.bg_scheduler: BackgroundScheduler = None
        self.sts: SqlTextSimilar = None
        self.static_config = None

    def __del__(self):
        """
        스케쥴러 종료 함수.
        """
        if self.block_scheduler:
            self.block_scheduler.shutdown()
        if self.bg_scheduler:
            self.bg_scheduler.shutdown()

    def main_process(self):
        """
        Main process 함수
        """
        self._add_scheduler_logger()
        # self._set_signal()
        self._init_scheduler()

        try:
            self.logger.info("Background Scheduler job start")
            self._bg_scheduler_start()
            time.sleep(1)

            self.logger.info("Blocking Scheduler job start")
            self._block_scheduler_start()
        except Exception as e:
            self.logger.exception(e)

    def _bg_scheduler_start(self):
        """
        백그라운드 스케쥴러 등록 및 시작 하기 위한 함수
        :return:
        """

        if self.config["scheduler"]["is_alive_sched"]["use"]:
            self.bg_scheduler.add_job(
                self._is_alive_logging_job,
                CRON,
                hour=self.config["scheduler"]["is_alive_sched"]["hour"],
                minute=self.config["scheduler"]["is_alive_sched"]["minute"],
                id="_is_alive_logging_job",
            )

        if self.config["scheduler"]["main_sched"]["use"]:
            self.bg_scheduler.add_job(
                self._main_job,
                CRON,
                hour=self.config["scheduler"]["main_sched"]["hour"],
                minute=self.config["scheduler"]["main_sched"]["minute"],
                id="_extract_summary_job",
            )

        if self.config["scheduler"]["sql_text_similarity_sched"]["use"]:
            if self.config["intermax_repo"]["use"]:
                self.static_config = copy.deepcopy(self.config)

                custom_config_dict = self._set_custom_dict("r")

                self.static_config.update(custom_config_dict)

                self.sts = SqlTextSimilar(self.scheduler_logger)
                self.sts.set_config(self.static_config)
                self.sts.pre_load_tuning_sql_text()

                self.bg_scheduler.add_job(
                    self._sql_text_similar_job,
                    CRON,
                    hour=self.config["scheduler"]["sql_text_similarity_sched"]["hour"],
                    minute=self.config["scheduler"]["sql_text_similarity_sched"]["minute"],
                    id="_sql_text_similar_job",
                )
            else:
                self.scheduler_logger.info("SqlTextSimilar not activate, cause intermax_repo config use false")

        self.bg_scheduler.start()

    def _block_scheduler_start(self):
        """
        블로킹 스케쥴러 등록 및 시작 하기 위한 함수
        :return:
        """
        self.block_scheduler.add_job(
            self._block_scheduler_job,
            CRON,
            hour=self.config["scheduler"]["main_sched"]["hour"],
            minute=self.config["scheduler"]["main_sched"]["minute"],
            id="_block_scheduler_job",
        )
        self.scheduler_logger.info(
            f"Main scheduler start and set config cron expression - "
            f"{self.config['scheduler']['main_sched']['hour']} hour "
            f"{self.config['scheduler']['main_sched']['minute']} minute"
        )

        self.block_scheduler.start()

    def _block_scheduler_job(self):
        """
        블로킹 스케쥴러 job 함수 (Windows Service 등록 후 정상 동작 1회후 동작하지 않아서 백그라운드 스케쥴러로 job 이관)
        """
        self.scheduler_logger.info("_block_scheduler_job")

    def _add_scheduler_logger(self):
        """
        스케쥴러 전용 logger 생성 함수
        """
        self.scheduler_logger = Logger(self.config["env"]).get_default_logger(
            self.config["log_dir"], SystemConstants.SCHEDULER_LOG_FILE_NAME
        )

    def _init_scheduler(self):
        """
        스케쥴러 객체 생성 함수
        """
        self.block_scheduler = BlockingScheduler(timezone="Asia/Seoul")
        self.bg_scheduler = BackgroundScheduler(timezone="Asia/Seoul")

    def _set_signal(self):
        """
        signal 등록 함수
        """
        if platform.system().lower() in "windows":
            signal.signal(signal.SIGBREAK, self._terminate)
            signal.signal(signal.SIGINT, self._terminate)

    def _terminate(self):
        """
        스케쥴러 종료 함수
        """
        self.bg_scheduler.shutdown()
        self.block_scheduler.shutdown()

    def _main_job(self):
        """
        분석 모듈 Main 스케쥴러 job 함수
        """
        self.scheduler_logger.info("main_job start")
        start_tm = time.time()

        result = ResultConstants.FAIL
        result_code = "E001"
        result_msg = MessageEnum[result_code].value

        try:
            self._update_config_custom_values(proc="b")

            db = DataBase(self.config)
            db.create_engine()
            elm = ExecuteLogModel(
                ModuleFactoryEnum[self.config["args"]["proc"]].value,
                DateUtils.get_now_timestamp(),
                str(self.config["args"]),
                "batch",
            )

            with db.session_scope() as session:
                session.add(elm)

            db.engine_dispose()

            self._extractor_job()

            self._summarizer_job()

            self._sql_text_merge_job()

            self._dynamic_sql_search_job()

            result = ResultConstants.SUCCESS
            result_code = "I001"
            result_msg = MessageEnum[result_code].value

        except ModuleException as me:
            self.logger.error(me.error_msg)
            result = ResultConstants.ERROR
            result_code = me.error_code
            result_msg = me.error_msg

        except Exception as e:
            self.logger.exception(e)
            result = ResultConstants.ERROR
            result_code = "E999"
            result_msg = str(e)
        finally:
            result_dict = SystemUtils.set_update_execute_log(result, start_tm, result_code, result_msg)
            db.create_engine()
            with db.session_scope() as session:
                session.query(ExecuteLogModel).filter(ExecuteLogModel.seq == f"{elm.seq}").update(result_dict)
                session.commit()

            db.engine_dispose()

        self.scheduler_logger.info("main_job end")

    def _extractor_job(self):
        """
        extractor job 함수
        """
        self.scheduler_logger.info("_extractor_job start")

        self._update_config_custom_values(proc="e")

        extractor = Extractor(self.scheduler_logger)
        extractor.set_config(self.config)
        extractor.main_process()

        self.scheduler_logger.info("_extractor_job end")

    def _summarizer_job(self):
        """
        summarizer job 함수
        """
        self.scheduler_logger.info("_summarizer_job start")

        self._update_config_custom_values(proc="s")

        summarizer = Summarizer(self.scheduler_logger)
        summarizer.set_config(self.config)
        summarizer.main_process()

        self.scheduler_logger.info("_summarizer_job end")

    def _sql_text_merge_job(self):
        """
        sql_text_merge job 함수
        """
        self.scheduler_logger.info("_sql_text_merge_job start")

        self._update_config_custom_values(proc="m")

        stm = SqlTextMerge(self.scheduler_logger)
        stm.set_config(self.config)
        stm.main_process()

        self.scheduler_logger.info("_sql_text_merge_job end")

    def _dynamic_sql_search_job(self):
        """
        dynamic sql search job 함수
        """
        self.scheduler_logger.info("_dynamic_sql_search_job start")

        self._update_config_custom_values(proc="p")

        stm = DynamicSqlSearch(self.scheduler_logger)
        stm.set_config(self.config)
        stm.main_process()

        self.scheduler_logger.info("_dynamic_sql_search_job end")

    def _sql_text_similar_job(self):
        """
        sql_text_similar job 함수
        """
        self.scheduler_logger.info("_sql_text_similar_job start")

        self.sts.main_process()

        self.scheduler_logger.info("_sql_text_similar_job end")

    def _update_config_custom_values(self, proc):
        """
        스케쥴러 기능 별 config값 update 함수
        :param proc: 기능별 심볼 값
        """
        custom_config_dict = self._set_custom_dict(proc)

        self.config.update(custom_config_dict)

    @staticmethod
    def _set_custom_dict(proc):
        """
        proc 별 custom config 생성 함수
        :param proc: 각 proc 심볼
        :return: custom config
        """
        custom_value = {"s_date": DateUtils.get_date_by_interval(-1, fmt="%Y%m%d"), "interval": 1, "proc": proc}
        return {"args": custom_value}

    def _is_alive_logging_job(self):
        """
        is_alive_logging job 함수
        """
        for job in self.bg_scheduler.get_jobs():
            if job.id == "_is_alive_logging_job":
                continue

            self.scheduler_logger.info(f"name: {job.id}, trigger: {job.trigger}, next run: {job.next_run_time}")

        self.scheduler_logger.info(f"This analysis module scheduler is alive.. PID : {os.getpid()} \n")
