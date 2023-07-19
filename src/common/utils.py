import importlib.util
import argparse
import os
import pandas as pd
import time
import re
import glob
from pathlib import Path
from datetime import datetime, timedelta


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
    def byte_transform(b, to, bsize=1024):
        """
        byte를 kb, mb, gb, tb, eb로 변환하는 함수
        :param b: 변환하려는 byte 값
        :param to: 변환 하려는 단위 (k : kb, m : mb, g : gb, t : tb, e : eb)
        :param bsize: 변환 상수
        :return: 변환 하려는 단위의 반올림 값
        """
        a = {'k': 1, 'm': 2, 'g': 3, 't': 4, 'p': 5, 'e': 6}
        r = float(b)
        for i in range(a[to]):
            r = r / bsize
        return round(r, 2)

    @staticmethod
    def get_file_in_path(query_folder, sql_name):
        with open(query_folder + "/" + sql_name, "r", encoding='utf-8') as file:
            sql_query = file.read()

        return sql_query



    @staticmethod
    def data_processing(df):
        df.columns = map(lambda x: str(x).upper(), df.columns)
        df = df.apply(pd.to_numeric, errors='ignore')

        if 'TIME' in df.columns:
            df['TIME'] = pd.to_datetime(df['TIME'])

        return df

    @staticmethod
    def excel_export(excel_file, sheet_name_txt, df):
        if not os.path.exists(excel_file):
            with pd.ExcelWriter(excel_file, mode='w', engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name_txt, index=False)
        else:
            with pd.ExcelWriter(excel_file, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
                df.to_excel(writer, sheet_name=sheet_name_txt, index=False)

    @staticmethod
    def get_date_by_interval(interval, fmt="%Y%m%d"):
        """
        interval에 따른 날짜를 구하기 위한 함수 
        :param interval: 필요한 날짜 interval (ex, 어제 -1, 내일 1)
        :param fmt: return 포맷 (기본 : yyyymmdd)
        :return: interval 날짜
        """
        now = datetime.now()
        return (now + timedelta(days=interval)).strftime(fmt)

    @staticmethod
    def get_filenames_from_path(path: str, prefix: str = '', suffix: str = ''):

        """
        path에 모든 파일 이름을 가져오는 함수
        :param path: 파일 이름을 가져오려는 절대 경로
        :param prefix: 시작이 prefix를 포함하는 파일 (optional)
        :param suffix: 끝이 suffix를 포함하는 파일 (optional)
        :return: 파일 이름 list
        """
        if not os.path.exists(path):
            os.makedirs(path)

        return [x for x in os.listdir(path) if str(x).startswith(prefix) and str(x).endswith(suffix)]

    @staticmethod
    def get_now_timestamp(fmt='%Y%m%d%H%M%S'):
        """
        현재 시간 timestamp 함수
        :param fmt: 포맷 optional (기본 : %Y%m%d%H%M%S)
        :return: 현재 시간 str (기본 14 자리)
        """
        return datetime.now().strftime(fmt)

    @staticmethod
    def set_update_execute_log(result, start_tm, result_code, result_msg) -> dict:
        """
        ae_execute_log 저장 하기 위한 함수
        :param result: 기능 동작 결과
        :param start_tm: 기능 시작 시간
        :param result_code: 기능 동작 결과 코드
        :param result_msg: 기능 동작 결과 메세지
        :return: 결과 dict
        """
        result_dict = dict()
        result_dict['result'] = result
        result_dict['execute_end_dt'] = SystemUtils.get_now_timestamp()
        result_dict['execute_elapsed_time'] = time.time() - start_tm
        result_dict['result_code'] = result_code
        result_dict['result_msg'] = result_msg

        return result_dict

    @staticmethod
    def get_file_export_args():
        """
        파일 추출 및 적재 기능 args
        proc :  no value - export, 'insert' - db insert
        :return: args
        """
        parser = argparse.ArgumentParser(description="File Export Args")

        parser.add_argument('--proc')
        args = parser.parse_args()
        return args

    @staticmethod
    def extract_tablename_in_filename(filename):
        return filename.split(".")[0].split('-')[1]

    @staticmethod
    def get_each_date_by_interval(s_date, interval):
        s_date = datetime.strptime(str(s_date), '%Y%m%d')
        e_date = s_date + timedelta(days=int(interval))
        return s_date, e_date


class TargetUtils:

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


class InterMaxUtils:

    @staticmethod
    def set_intermax_date(input_date, input_interval):
        date_conditions = []

        for i in range(1, int(input_interval)+1):
            from_date = datetime.strptime(str(input_date), '%Y%m%d')
            date_condition = from_date + timedelta(days=i - 1)
            date_condition = date_condition.strftime('%Y%m%d')
            date_conditions.append(date_condition)

        return date_conditions

    @staticmethod
    def  meta_table_value(table_name, df):

        if table_name == 'ae_was_dev_map':
            df['isdev'] = 1

        return df


class MaxGaugeUtils:

    @staticmethod
    def reconstruct_by_grouping(results):
        """
        db sql text 재조합을 위한 함수
        :param results: seq가 동일한 데이터들의 전체 리스트
        :return: 재조합된 데이터프레임
        """
        results_df = pd.DataFrame(results, columns=['sql_text', 'partition_key', 'sql_uid', 'seq'])
        results_df = results_df.groupby(['sql_uid', 'partition_key'], as_index=False).agg({'sql_text': ''.join})
        results_df.drop(columns='partition_key', inplace=True)
        return results_df

    @staticmethod
    def set_maxgauge_date(input_date, input_interval):
        date_conditions = []

        for i in range(1, int(input_interval)+1):
            from_date = datetime.strptime(str(input_date), '%Y%m%d')
            date_condition = from_date + timedelta(days=i - 1)
            date_condition = date_condition.strftime('%y%m%d')
            date_conditions.append(date_condition)

        return date_conditions


class SummarizerUtils:

    @staticmethod
    def summarizer_set_date(input_date):
        start_date = datetime.strptime(input_date, '%Y%m%d')
        end_date = start_date + timedelta(days=1)
        start_date = start_date.strftime('%Y-%m-%d 00:00:00')
        end_date = end_date.strftime('%Y-%m-%d 00:00:00')
        return start_date, end_date


class SqlUtils:

    @staticmethod
    def remove_unnecess_char(df, target_c: str, des_c: str = None, contains_comma=False):
        """
        정규식을 이용한 /t, /n, /r, comma 치환 함수
        :param df: 원본 데이터프레임
        :param target_c: 대상 타겟 컬럼
        :param des_c: 목적지 컬럼 (optional) if None target_c
        :param contains_comma: comma 값 치환 여부
        :return: 치환된 데이터프레임
        """
        des_c = target_c if des_c is None else des_c

        comma_attr = [(',', ' ')]

        repls = {r'\\t': ' ', r'\\n': ' ', r'\\r': ' ', '\t': ' ', '\n': ' ', '\r': ' '}

        if contains_comma:
            repls.update(comma_attr)

        rep = dict((re.escape(k), v) for k, v in repls.items())
        pattern = re.compile("|".join(rep.keys()))

        df[des_c] = df[target_c].apply(
            lambda x: pattern.sub(lambda m: rep[re.escape(m.group(0))], x)
        )
        return df

    @staticmethod
    def rex_processing(df):
        """
        sql text를 정규식 처리를 위한 함수
        in 구문, values 구문, DateTime 형식, Date 형식
        :param df: 정규식 처리를 위한 DataFrame, 해당 컬럼은 sql_text
        :return: 정규식 변환 처리된 DataFrame
        """
        df['sql_text'] = df['sql_text'].str.replace(r'\s+in\s?\([^)]*\)', ' in(<:args:>)', regex=True)
        df['sql_text'] = df['sql_text'].str.replace(r'\s+values\s?\([^)]*\)', ' values(<:args:>)', regex=True)
        df['sql_text'] = df['sql_text'].str.replace(r"\d{2,4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}\.?\d{0,3}", '<DATETIME>',
                                                    regex=True)
        df['sql_text'] = df['sql_text'].str.replace(r"\d{2,4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.?\d{0,3}", '<DATETIME>',
                                                    regex=True)
        df['sql_text'] = df['sql_text'].str.replace(r"\d{4}\-\d{2}\-\d{2}", '<DATE>', regex=True)
        return df

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
