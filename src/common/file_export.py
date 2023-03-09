import os
import pyarrow.parquet as pq
import pyarrow as pa

from pathlib import Path
from src.common.utils import SystemUtils


class ParquetFile:
    """
    Parquet file를 생성하기 위한 모듈
    """

    def __init__(self, logger, config):
        self.logger = logger
        self.config = config
        self.DOT = "."
        self.index = 1

    def make_parquet_by_df(self, df, export_path, parquet_file_name):
        """
        DataFrame을 parquet 파일로 만들기 위한 함수
        연속으로 호출 시 첫번째 파일 생성 이후 계속 append 된다.
        :param df: 타겟 데이터 프레임
        :param export_path: 추출하려는 파일의 디렉토리
        :param parquet_file_name: 추출하려는 파일 이름
        :return:
        """
        Path(export_path).mkdir(exist_ok=True, parents=True)

        parquet_name = f"{export_path}/{parquet_file_name}"

        df_volume = f"{SystemUtils.byte_transform(df.memory_usage(deep=True).sum(), 'm')} Mb"

        self.index = 1 if self.index > 30 else self.index

        if not os.path.isfile(parquet_name):
            df.to_parquet(parquet_name, engine='fastparquet')
            self.logger.info(f"{parquet_file_name} file create and export ({df_volume}) {self.DOT * self.index}")
        else:
            df.to_parquet(parquet_name, engine='fastparquet', append=True)
            self.logger.info(f"{parquet_file_name} file append and export ({df_volume}) {self.DOT * self.index}")

        self.index += 1

    def make(self, df, export_path, parquet_file_name):
        export_path = f"{self.config['home']}/{export_path}"
        Path(export_path).mkdir(exist_ok=True, parents=True)

        parquet_name = f"{export_path}/{parquet_file_name}"

        table = pa.Table.from_pandas(df)

        pq.write_table(table, f'{parquet_name}')

    def remove_parquet(self, file_path, file_name):
        """
        parquet 파일 삭제 함수. 필요시 사용
        :param file_path: 삭제하려는 파일 path
        :param file_name: 삭제하려는 파일명
        :return:
        """
        file_name = f"{file_path}/{file_name}"

        if os.path.isfile(file_name):
            self.logger.info(f"{file_name} Deleting..")
            os.remove(file_name)
            self.logger.info(f"{file_name} Deleted OK")


if __name__ == "__main__":
    from src.common.constants import SystemConstants, TableConstants
    from resources.logger_manager import Logger
    from resources.config_manager import Config
    from src.analysis_target import SaTarget

    no_need_table = {
        'ae_db_info',
    }

    home = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent

    env = SystemUtils.get_environment_variable()

    log_dir = str(Path(home) / SystemConstants.LOGGER_PATH)

    logger = Logger(env).get_default_logger(log_dir, SystemConstants.ETC_LOG_FILE_NAME)

    config = Config(env).get_config()

    pf = ParquetFile(logger, config)

    table_list = [v for k, v in TableConstants().__class__.__dict__.items() if not k.startswith('_')]
    table_list = [table for table in table_list if table not in no_need_table]

    export_parquet_root_path = f'{home}/{SystemConstants.EXPORT_ETC_PATH}'

    st = SaTarget(logger, config)
    st.init_process()

    for table in table_list:

        file_name = f"{table}{SystemConstants.DB_SQL_TEXT_FILE_SUFFIX}"

        pf.remove_parquet(export_parquet_root_path, file_name)

        # 이부분은 for문 필요 chunksize 만큼
        df = st.get_table_data(table)

        pf.make_parquet_by_df(df, export_parquet_root_path, file_name)
