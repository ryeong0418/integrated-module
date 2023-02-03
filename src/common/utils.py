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
        """
        모듈 파일 이름으로 모듈을 임포트 할 모듈 반환 함수
        :param module_name: 해당 모듈 파일 이름
        :param file_path: 해당 모듈 파일 경로
        :return: 해당하는 모듈
        """
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
    def byte_transform(bytes, to, bsize=1024):
        """
        byte를 kb, mb, gb, tb, eb로 변환하는 함수
        :param bytes: 변환하려는 byte 값
        :param to: 변환 하려는 단위 (k : kb, m : mb, g : gb, t : tb, e : eb)
        :param bsize: 변환 상수
        :return: 변환 하려는 단위의 반올림 값
        """
        a = {'k': 1, 'm': 2, 'g': 3, 't': 4, 'p': 5, 'e': 6}
        r = float(bytes)
        for i in range(a[to]):
            r = r / bsize
        return round(r, 2)

    @staticmethod
    def sql_replace_to_dict(sql, replace_dict):
        """
        sql query에 동적 parameter를 set 하기 위한 함수
        :param sql: SQL 원본 쿼리
        :param replace_dict: 변경이 필요한 파라미터의 dict
                (ex) table_name를 치환하기 위해서는 원본 쿼리에 #(table_name) 형식으로 만들어 준다
        :return: 치환된 sql query
        :exception: 치환되지 않는 형태의 오류 발생시 원본 쿼리와 각 동적 parameter dict 키 매핑 확인
        """
        for key in replace_dict.keys():
            sql = sql.replace(f"#({key})", replace_dict[key])

        return sql


class TargetUtils:

    @staticmethod
    def get_db_conn_str(repo_info):
        """
        DB connection string를 만들기위한 함수
        :param repo_info: 대상 repository의 정보 (dict)
        :return: 접속 정보 str
        """
        return "dbname={} host={} port={} user={} password={}".format(
            repo_info['sid'],
            repo_info['host'],
            repo_info['port'],
            repo_info['user'],
            repo_info['password']
        )

    @staticmethod
    def create_and_check_table(logger, conn, querys, check_query=None):
        """
        분석 모듈에서 사용할 테이블 생성 함수
        :param logger: logger
        :param conn: connect object
        :param querys: 쿼리 (DDL)
        :param check_query: 테이블 생성 체크 쿼리
        :return:
        """
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
        """
        분석 모듈 DB 저장을 위한 SqlAlchemy engine 생성을 위한 string 생성 함수
        :param repo_info: 분석 모듈 DB repository 정보
        :return: engine 생성을 위한 str
        """
        return "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
            repo_info['user'],
            repo_info['password'],
            repo_info['host'],
            repo_info['port'],
            repo_info['sid']
        )

    @staticmethod
    def get_target_data_by_query(logger, target_conn, query, table_name="UNKNOWN TABLE"):
        """
        각 분석 대상의 DB에서 query 결과를 DataFrame 담아오는 함수
        :param logger: logger
        :param target_conn: 각 타겟 connect object
        :param query: 각 타겟 호출 SQL
        :param table_name: 분석 모듈 DB에 저장될 테이블 명 (for logging)
        :return: 각 타겟의 query 결과 정보 (DataFrame)
        """
        with TimeLogger(f"{table_name} to export", logger):
            df = pd.read_sql(query, target_conn)

        logger.info(f"{table_name} export pandas data memory (deep) : "
                    f"{SystemUtils.byte_transform(df.memory_usage(deep=True).sum(), 'm')} Mb")

        return df

    @staticmethod
    def insert_analysis_by_df(logger, analysis_engine, table_name, df):
        """
        분석 모듈 DB에 DataFrame의 데이터 저장 함수
        :param logger: logger
        :param analysis_engine: 분석 모듈 SqlAlchemy engine
        :param table_name: 분석 모듈 저장 DB 테이블
        :param df: 저장하려는 DataFrame
        :return:
        """
        with TimeLogger(f"{table_name} to import", logger):
            df.to_sql(
                name=table_name,
                con=analysis_engine,
                schema='public',
                if_exists='append',
                index=False,
            )

    @staticmethod
    def default_sa_execute_query(logger, sa_conn, query):
        """
        분석 모듈 DB 기본 sql 실행 쿼리
        :param logger: logger
        :param sa_conn: 분석 모듈 DB Connection Object
        :param query: 실행 하려는 쿼리
        :return:
        """
        try:
            cursor = sa_conn.cursor()
            cursor.execute(query)
        except Exception as e:
            logger.exception(e)
        finally:
            sa_conn.commit()
            cursor.close()

