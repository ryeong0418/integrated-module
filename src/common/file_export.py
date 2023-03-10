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
        self.target_file_name = None

    def make_parquet_by_df(self, df, pqwriter: pq.ParquetWriter):
        """
        DataFrame을 parquet 파일로 만들기 위한 함수
        같은 pqwriter로 호출 하게되면 계속 append 된다.
        다른 파일 생성 시 get_pqwriter 함수로 재 생성한 후 사용
        :param df: 타겟 데이터 프레임
        :param pqwriter: parquet 파일 writer 객체
        :param parquet_file_name: 추출하려는 파일 이름 Optinal (for log)
        :return:
        """
        self.index = 1 if self.index > 10 else self.index
        df_volume = f"{SystemUtils.byte_transform(df.memory_usage(deep=True).sum(), 'm')} Mb"

        pqwriter.write_table(pa.Table.from_pandas(df))

        self.logger.info(f"{self.target_file_name} file export ({df_volume}) {self.DOT * self.index}")
        self.index += 1

    def remove_parquet(self, file_path, file_name):
        """
        parquet 파일 삭제 함수. 필요시 사용
        :param file_path: 삭제하려는 파일 path
        :param file_name: 삭제하려는 파일명
        :return:
        """
        file_name = f"{file_path}/{file_name}"

        if os.path.isfile(file_name):
            self.logger.debug(f"{file_name} Deleting..")
            os.remove(file_name)
            self.logger.info(f"{file_name} Deleted OK")

    def get_pqwriter(self, file_path, file_name, df):
        """
        parquet writer 객체 생성 함수
        :param file_path: parquet 파일 생성 path
        :param file_name: parquet 파일 생성 이름
        :param df: schema를 위한 target df
        :return: pqwriter
        """
        Path(file_path).mkdir(exist_ok=True, parents=True)
        self.target_file_name = f"{file_path}/{file_name}"
        self.index = 1
        return pq.ParquetWriter(self.target_file_name, pa.Table.from_pandas(df).schema, compression="gzip")
