from src import common_module as cm
from src.common.constants import TableConstants, SystemConstants
from src.common.enum_module import ModuleFactoryEnum
import glob
from src.common.utils import SystemUtils, InterMaxUtils, MaxGaugeUtils, SqlUtils
from sql.common_sql import CommonSql, AeWasDevMapSql, AeDbInfoSql


class Extractor(cm.CommonModule):

    def __init__(self, logger):

        super().__init__(logger)

    def main_process(self):

        self.logger.debug('extractor')

        self._init_sa_target()
        self.st_engine = self._get_st_engine()
        self.st_conn = self._get_st_conn()

        if self.config['intermax_repo']['use']:
            self.logger.debug("Intermax extractor")

            self._init_im_target()
            self.im_engine = self._get_im_engine()
            self.im_conn = self._get_im_conn()

            # self.insert_intermax_meta_data()
            self.insert_intermax_detail_data()

        # if self.config['maxgauge_repo']['use']:
        #     self.logger.debug("maxgauge extractor")
        #     self._init_mg_target()
        #
        #     self.mg_engine = self._get_mg_engine()
        #     self.mg_conn = self._get_mg_conn()
        #
        #     self.insert_maxgauge_meta_data()
        #     self.insert_maxgauge_detail_data()


    def insert_intermax_meta_data(self):

        """
        InterMax 메타 데이터 테이블 query문 불러와 insert 또는 upsert 기능 함수
        upsert 실행 테이블 : ae_was_dev_map, ae_txn_name
        insert 실행 테이블 : ae_was_db_info, ae_host_info, ae_was_info
        """

        extractor_meta_path = f'{self.sql_file_root_path}{SystemConstants.WAS_PATH}{SystemConstants.META_PATH}'
        extractor_meta_file_list = glob.glob(extractor_meta_path + '/*.txt')

        for meta_file_path in extractor_meta_file_list:
            with open(meta_file_path, mode='r', encoding='utf-8') as file:
                query = file.read()

            ae_table_name = SystemUtils.extract_tablename_in_filename(meta_file_path)

            if ae_table_name == "ae_was_dev_map" or ae_table_name == "ae_txn_name":
                self.imt.upsert_intermax_data(query, ae_table_name, self.st_engine)

            else:
                self.imt.insert_meta(query, ae_table_name, self.im_conn, self.st_engine)

    def insert_intermax_detail_data(self):

        """
        InterMax 분석 데이터 테이블 query를 날짜 별로 불러와 읽은 후, delete -> insert 또는 upsert 기능 함수

        upsert 실행 테이블: ae_was_sql_text
        delete -> insert (기본) 실행 테이블: ae_bind_sql_elapse, ae_was_os_stat_osm
        delete -> insert (ae_was_dev_map 테이블에서 was_id 제외) 실행 테이블: ae_jvm_stat_summary,ae_txn_detail,
                                                        ae_txn_sql_detail, ae_txn_sql_fetch, ae_was_stat_summary
        """

        extractor_detail_path = f'{self.sql_file_root_path}{SystemConstants.WAS_PATH}'
        extractor_detail_file_list = glob.glob(extractor_detail_path + '/*.txt')

        delete_query = CommonSql.DELETE_TABLE_BY_DATE_QUERY
        date_conditions = InterMaxUtils.set_intermax_date(self.config['args']['s_date'],
                                                          self.config['args']['interval'])

        for detail_file_path in extractor_detail_file_list:
            with open(detail_file_path, mode='r', encoding='utf-8') as file:
                query = file.read()

            ae_table_name = SystemUtils.extract_tablename_in_filename(detail_file_path)

            for date in date_conditions:
                table_suffix_dict = {'table_suffix': date}
                detail_query = SqlUtils.sql_replace_to_dict(query, table_suffix_dict)
                delete_dict = {'table_name': ae_table_name, 'date': date}

                try:

                    if ae_table_name == "ae_was_sql_text":
                        print(ae_table_name)
                        # self.imt.upsert_intermax_data(detail_query, ae_table_name, self.st_engine)

                    else:
                        ae_dev_map_df = self.imt._get_target_data_by_query(self.st_conn,
                                                                           AeWasDevMapSql.SELECT_AE_WAS_DEV_MAP,
                                                                           ae_table_name)

                        im_delete_query = SqlUtils.sql_replace_to_dict(delete_query, delete_dict)
                        self.logger.info(f"delete query execute : {im_delete_query}")
                        self.imt._default_execute_query(self.st_conn, im_delete_query)

                        self.execute_intermax_detail_function(detail_query, ae_table_name, ae_dev_map_df)

                except Exception as e:
                    self.logger.exception(f"{ae_table_name} table, {date} date detail data insert error")
                    self.logger.exception(e)

    def execute_intermax_detail_function(self,query,table_name,ae_dev_map_df=None):

        return{
            #'ae_bind_sql_elapse':self.imt._excute_insert_bind_value_date(query, table_name,self.st_engine),
            #'ae_was_os_stat_osm':self.imt._execute_insert_intermax_detail_data(query, table_name,self.st_engine),
            'ae_jvm_stat_summary':self.imt._excute_insert_dev_except_data(query, table_name, ae_dev_map_df,self.st_engine),
            'ae_txn_detail': self.imt._excute_insert_dev_except_data(query, table_name, ae_dev_map_df,self.st_engine),
            'ae_txn_sql_detail': self.imt._excute_insert_dev_except_data(query, table_name, ae_dev_map_df,self.st_engine),
            'ae_txn_sql_fetch': self.imt._excute_insert_dev_except_data(query, table_name, ae_dev_map_df,self.st_engine),
            'ae_was_stat_summary': self.imt._excute_insert_dev_except_data(query, table_name, ae_dev_map_df,self.st_engine)
        }.get(table_name)

    def insert_maxgauge_meta_data(self):

        extractor_meta_file_list = glob.glob(self.sql_file_root_path + '/'+SystemConstants.DB_PATH +
                                             SystemConstants.META_PATH + '/*.txt')

        for file_path in extractor_meta_file_list:
            with open(file_path, mode='r', encoding='utf-8') as file:
                query = file.read()

            ae_table_name = SystemUtils.extract_tablename_in_filename(file_path)
            self.mgt.insert_meta(query, ae_table_name, self.mg_conn, self.st_engine)

    def insert_maxgauge_detail_data(self):

        """
        MaxGauge 메타 데이터 테이블 query를 날짜 별로 불러와 읽은 후, delete -> insert 기능 함수
        """

        date_conditions = MaxGaugeUtils.set_maxgauge_date(self.config['args']['s_date'],
                                                          self.config['args']['interval'])

        ae_db_info_query = AeDbInfoSql.SELECT_AE_DB_INFO

        ae_db_info_name = TableConstants.AE_DB_INFO
        db_info_df = self.mgt._get_target_data_by_query(self.st_conn, ae_db_info_query, ae_db_info_name)

        delete_query = CommonSql.DELETE_TABLE_BY_PARTITION_KEY_QUERY

        extractor_detail_file_list = glob.glob(self.sql_file_root_path + '/' + SystemConstants.DB_PATH + '/*.txt')

        for file_path in extractor_detail_file_list:
            with open(file_path, mode='r', encoding='utf-8') as file:
                query = file.read()

            table_name = SystemUtils.extract_tablename_in_filename(file_path)

            for _, row in db_info_df.iterrows():
                db_id = str(row["db_id"]).zfill(3)
                instance_name = str(row["instance_name"])

                for date in date_conditions:
                    table_suffix_dict = {'instance_name': instance_name, 'partition_key': date + db_id}
                    delete_suffix_dict = {'table_name': table_name, 'partition_key': date + db_id}

                    try:
                        mg_delete_query = SqlUtils.sql_replace_to_dict(delete_query, delete_suffix_dict)
                        self.logger.info(f"delete query execute : {mg_delete_query}")

                        self.mgt._default_execute_query(self.st_conn, mg_delete_query)

                        detail_query = SqlUtils.sql_replace_to_dict(query, table_suffix_dict)
                        self.mgt._execute_insert_maxgauge_detail_data(detail_query, table_name,self.st_engine)

                    except Exception as e:
                        self.logger.exception(f"{table_name} table, {date} date detail data insert error")
                        self.logger.exception(e)


