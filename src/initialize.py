from src import common_module as cm
from src.common.constants import SystemConstants
from src.common.utils import SystemUtils
import os.path

class Initialize(cm.CommonModule):

    def __init__(self, logger):
        super().__init__(logger)

    def main_process(self):
        self.logger.debug("SaTarget init")

        self._init_sa_target()

        #self._create_table()

        self._insert_init_meta()

    def _create_table(self):
        init_ddl_path = f"{self.sql_file_root_path}{SystemConstants.DDL_PATH}"
        init_files = SystemUtils.get_filenames_from_path(init_ddl_path)

        for init_file in init_files:
            with open(f"{init_ddl_path}{init_file}", mode='r', encoding='utf-8') as file:
                ddl = file.read()

            self.st.create_table(ddl)

    def _insert_init_meta(self):
        if self.config['intermax_repo']['use']:
            self._init_im_target()

            self._insert_init_meta_by_target(SystemConstants.WAS_PATH, self.imt)
            self._teardown_im_target()

        if self.config['maxgauge_repo']['use']:
            self._init_mg_target()

            self._insert_init_meta_by_target(SystemConstants.DB_PATH, self.mgt)
            self._teardown_mg_target()

    def _insert_init_meta_by_target(self, target, target_instance):

        init_meta_path = f"{self.sql_file_root_path}{target}{SystemConstants.META_PATH}"
        init_files = SystemUtils.get_filenames_from_path(init_meta_path)

        for init_file in init_files:
            with open(f"{init_meta_path}{init_file}", mode='r', encoding='utf-8') as file:
                meta_query = file.read()

            target_table_name = SystemUtils.extract_tablename_in_filename(init_file)

            for meta_df in target_instance.get_data_by_meta_query(meta_query):
                self.st.insert_init_meta(meta_df, target_table_name)
