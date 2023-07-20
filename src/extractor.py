from src import common_module as cm
from src.common.constants import TableConstants, SystemConstants

from src.common.utils import SystemUtils, InterMaxUtils, MaxGaugeUtils, SqlUtils
from sql.common_sql import CommonSql, AeWasDevMapSql, AeDbInfoSql


class Extractor(cm.CommonModule):

    """
    InterMax, MaxGauge DB에 있는 데이터 추출하여
    AE DB에 데이터 인서트
    """

    def __init__(self, logger):
        super().__init__(logger=logger)

    def main_process(self):

        self.logger.debug('extractor')
        self._init_sa_target()

        if self.config['intermax_repo']['use']:
            self.logger.debug("Intermax extractor")
            self._init_im_target()

            self._insert_meta_data(SystemConstants.WAS_PATH, self.imt)
            self._insert_intermax_detail_data()

        if self.config['maxgauge_repo']['use']:
            self.logger.debug("maxgauge extractor")
            self._init_mg_target()

            self._insert_meta_data(SystemConstants.DB_PATH, self.mgt)
            self._insert_maxgauge_detail_data()

    def _insert_meta_data(self, target, target_instance):

        """
        :param target : WAS, DB
        :param target_instance : WAS instance, DB instance
        """

        extractor_file_path = self.sql_file_root_path+target+SystemConstants.META_PATH
        extractor_meta_file_list = SystemUtils.get_filenames_from_path(extractor_file_path)

        for meta_file in extractor_meta_file_list:
            with open(extractor_file_path + meta_file, mode='r', encoding='utf-8') as file:
                meta_query = file.read()

            target_table_name = SystemUtils.extract_tablename_in_filename(meta_file)

            for meta_df in target_instance.get_data_by_query(meta_query):
                if target_table_name in ("ae_was_dev_map", "ae_txn_name"):
                    df = InterMaxUtils.meta_table_value(target_table_name, meta_df)
                    self.st.upsert_data(df, target_table_name)

                else:
                    self.st.insert_init_meta(meta_df, target_table_name)

    def _insert_intermax_detail_data(self):

        """
        InterMax 분석 데이터 테이블 query를 날짜 별로 불러와 읽은 후, delete -> insert 또는 upsert 기능 함수

        upsert 실행 테이블: ae_was_sql_text
        delete -> insert (기본) 실행 테이블: ae_bind_sql_elapse, ae_was_os_stat_osm
        delete -> insert (ae_was_dev_map 테이블에서 was_id 제외) 실행 테이블: ae_jvm_stat_summary,ae_txn_detail,
                                                        ae_txn_sql_detail, ae_txn_sql_fetch, ae_was_stat_summary
        """

        extractor_file_path = self.sql_file_root_path+SystemConstants.WAS_PATH
        extractor_detail_file_list = SystemUtils.get_filenames_from_path(extractor_file_path, '', 'txt')

        date_conditions = InterMaxUtils.set_intermax_date(self.config['args']['s_date'],
                                                          self.config['args']['interval'])

        ae_dev_map_df = self.st.get_data_by_query_and_once(
            AeWasDevMapSql.SELECT_AE_WAS_DEV_MAP,
            TableConstants.AE_WAS_DEV_MAP
        )
        delete_query = CommonSql.DELETE_TABLE_BY_DATE_QUERY

        for detail_file in extractor_detail_file_list:

            with open(extractor_file_path + detail_file, mode='r', encoding='utf-8') as file:
                query = file.read()

            target_table_name = SystemUtils.extract_tablename_in_filename(detail_file)

            for date in date_conditions:
                table_suffix_dict = {'table_suffix': date}
                detail_query = SqlUtils.sql_replace_to_dict(query, table_suffix_dict)
                delete_dict = {'table_name': target_table_name, 'date': date}

                try:

                    if target_table_name == "ae_was_sql_text":
                        for df in self.imt.get_data_by_query(detail_query):
                            self.st.upsert_data(df, target_table_name)

                    else:
                        self.st.delete_data(delete_query, delete_dict)
                        self.insert_intermax_common_detail_data(detail_query, target_table_name, ae_dev_map_df)

                except Exception as e:
                    self.logger.exception(f"{target_table_name} table, {date} date detail data insert error")
                    self.logger.exception(e)

    def insert_intermax_common_detail_data(self,detail_query,ae_table_name,ae_dev_map_df):

        """
        InterMax detail data Insert
        """

        for df in self.imt.get_data_by_query(detail_query):

            if ae_table_name == "ae_bind_sql_elapse":
                self.st.insert_bind_value_date(df, ae_table_name)

            elif ae_table_name == "ae_was_os_stat_osm":
                self.st.insert_detail_data(df, ae_table_name)

            else:
                self.st.insert_dev_except_data(df, ae_table_name, ae_dev_map_df)

    def _insert_maxgauge_detail_data(self):

        """
        MaxGauge 메타 데이터 테이블 query를 날짜 별로 불러와 읽은 후, delete -> insert 기능 함수
        """

        date_conditions = MaxGaugeUtils.set_maxgauge_date(self.config['args']['s_date'],
                                                          self.config['args']['interval'])

        db_info_df = self.st.get_data_by_query_and_once(
            AeDbInfoSql.SELECT_AE_DB_INFO,
            TableConstants.AE_DB_INFO
        )

        delete_query = CommonSql.DELETE_TABLE_BY_PARTITION_KEY_QUERY

        extractor_file_path = self.sql_file_root_path+SystemConstants.DB_PATH
        extractor_detail_file_list = SystemUtils.get_filenames_from_path(extractor_file_path,'','txt')

        for detail_file in extractor_detail_file_list:
            with open(extractor_file_path+detail_file, mode='r', encoding='utf-8') as file:
                query = file.read()

            table_name = SystemUtils.extract_tablename_in_filename(detail_file)

            for _, row in db_info_df.iterrows():
                db_id = str(row["db_id"]).zfill(3)
                instance_name = str(row["instance_name"])

                for date in date_conditions:
                    table_suffix_dict = {'instance_name': instance_name, 'partition_key': date + db_id}
                    delete_suffix_dict = {'table_name': table_name, 'partition_key': date + db_id}
                    detail_query = SqlUtils.sql_replace_to_dict(query, table_suffix_dict)

                    try:
                        self.st.delete_data(delete_query, delete_suffix_dict)
                        for df in self.mgt.get_data_by_query(detail_query):
                            self.st.insert_detail_data(df, table_name)

                    except Exception as e:
                        self.logger.exception(f"{table_name} table, {date} date detail data insert error")
                        self.logger.exception(e)
