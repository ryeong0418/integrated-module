import importlib.util
import argparse
import os

from pathlib import Path
from psycopg2 import errors
from psycopg2.errorcodes import DUPLICATE_TABLE
import pandas as pd

from src.common.timelogger import TimeLogger

class SystemUtils:

    @staticmethod
    def to_camel_case(snake_str):
        """
        모듈명 이름으로 클래스명을 획득하는 함수
        :param snake_str: 모듈명 snake 형식
        :return: 클래스명
        """
        components = snake_str.split("_")
        return "".join(x.title() for x in components[0:])

    @staticmethod
    def get_module_class(module_name, class_name, path):
        """
        모듈의 클래스를 가져오는 함수
        :param module_name: 모듈 이름
        :param class_name: 클래스 이름
        :param path: 모듈 경로
        :return: 해당 클래스
        """
        m = SystemUtils.get_module_from_file(module_name, path)
        c = getattr(m, class_name)
        return c

    @staticmethod
    def get_module_from_file(module_name, file_path):
        spec = importlib.util.spec_from_file_location(
            module_name, str(Path(file_path) / f"{module_name}.py")
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    @staticmethod
    def get_start_args():
        """
        proc : 모듈 기능별 동작 수행을 위한 설정값
            (i : Initialize, e :  Extract, s : Summary, v : Visualization, b : batch) - required
        s_date : 분석 데이터 수집, 취합, 시각화 시작 날짜
        interval : 수행 날짜 기간
        모듈 기동 args 획독
        :return: args
        """
        parser = argparse.ArgumentParser(description="Smart Analyzer Start Args")

        parser.add_argument('--proc', required=True)
        parser.add_argument('--s_date')
        parser.add_argument('--interval')

        args = parser.parse_args()
        return args

    @staticmethod
    def get_environment_variable():
        """
        배포 환경 별 config, logger 설정을 위한 환경 변수
        모듈의 특성상 local, dev는 환경 변수로 설정하고 현장에서 특별한 설정이 없을 때 prod로 사용할 수 있게 구성
        :return: env
        """
        return 'prod' if os.environ.get("DEPLOY_ENV") is None else os.environ.get("DEPLOY_ENV")

    @staticmethod
    def byte_transform(bytes, to, bsize=1025):
        a = {'k': 1, 'm': 2, 'g': 3, 't': 4, 'p': 5, 'e': 6}
        r = float(bytes)
        for i in range(a[to]):
            r = r / bsize
        return round(r, 2)


class TargetUtils:

    @staticmethod
    def get_db_conn_str(repo_info):
        return "dbname={} host={} port={} user={} password={}".format(
            repo_info['sid'],
            repo_info['host'],
            repo_info['port'],
            repo_info['user'],
            repo_info['password']
        )

    @staticmethod
    def create_and_check_table(logger, conn, querys, check_query=None):
        cur = conn.cursor()

        for query in querys:
            try:
                cur.execute(query)

            except errors.lookup(DUPLICATE_TABLE) as e:
                logger.warn("This DDL Query DUPLICATE_TABLE.. SKIP")
            except Exception as e:
                logger.exception(f"{e}")
            finally:
                conn.commit()

        if check_query is not None:
            try:
                cur.execute(check_query)
                fetched_rows = cur.fetchall()
            except Exception as e:
                logger.exception(f"{e}")
            finally:
                cur.close()
                conn.close()

            logger.info(fetched_rows)

    @staticmethod
    def get_engine_template(repo_info):
        return "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
            repo_info['user'],
            repo_info['password'],
            repo_info['host'],
            repo_info['port'],
            repo_info['sid']
        )

    @staticmethod
    def insert_meta_data(logger, target_conn, analysis_engine, table_name, query):
        with TimeLogger(f"{table_name} to export", logger):
            df = pd.read_sql(query, target_conn)

        logger.info(f"{table_name} export pandas data memory (deep) : "
                    f"{SystemUtils.byte_transform(df.memory_usage(deep=True).sum(), 'm')} Mb")

        with TimeLogger(f"{table_name} to import", logger):
            df.to_sql(
                name=table_name,
                con=analysis_engine,
                schema='public',
                if_exists='append',
                index=False,
            )

