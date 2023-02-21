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
