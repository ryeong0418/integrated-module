import os
import re
import pandas as pd

from pathlib import Path
from datetime import datetime, timedelta

from src import common_module as cm
from src.common.utils import SystemUtils, SqlUtils
from src.common.constants import DateFmtConstants
from src.common.constants import SystemConstants

INTERVAL_MINUTE = 10


class SqlTextSimilarity(cm.CommonModule):

    def __init__(self, logger):
        super().__init__(logger)
        self.origin_sql_id_dict = {}
        self.cluster_id_by_origin_sql_id_dict = {}
        self.last_execution_time = None
        self.last_execution_proc = "None"
        self.valid_sql_text_similarity = 0.6
        self.is_exist_tuning_sql = False

    def pre_load_tuning_sql_text(self):
        self._set_tuning_sql_text()
        self.valid_sql_text_similarity = self.config.get('valid_sql_text_similarity', 0.6)

    def main_process(self):
        self.logger.info("SqlTextSimilarity")

        self._set_tuning_sql_text()

        if self.is_exist_tuning_sql:
            xapm_sql_df = self._get_xapm_txn_sql_detail()
            xapm_sql_df = self._preprocessing_xapm_sql_df(xapm_sql_df)

            result_valid_df = self._analysis_sql_text_similarity(xapm_sql_df)
            self._insert_ae_txn_sql_similarity(result_valid_df)

    @staticmethod
    def _preprocessing_xapm_sql_df(xapm_sql_df):
        xapm_sql_df = xapm_sql_df[xapm_sql_df['sql_text'].str.contains('sql is too big') == False]

        xapm_sql_df = SqlUtils.remove_unnecess_char(xapm_sql_df, 'sql_text', contains_comma=True)
        xapm_sql_df['sql_text'] = xapm_sql_df['sql_text'].str.lower().str.replace(r'\s+', ' ', regex=True)
        xapm_sql_df = SqlUtils.rex_processing(xapm_sql_df)
        xapm_sql_df['sql_text_split'] = xapm_sql_df['sql_text'].str.split(r'\s+')
        xapm_sql_df.drop('sql_text', axis=1, inplace=True)
        return xapm_sql_df

    def _set_tuning_sql_text(self):
        home_parent_path = Path(self.config['home']).parent
        tuning_sql_path = os.path.join(home_parent_path, SystemConstants.TUNING_SQL_TEXT_PATH)

        if not os.path.isdir(tuning_sql_path):
            os.mkdir(tuning_sql_path)

        self.logger.debug(f"tuning sql child path exist {bool(os.listdir(tuning_sql_path))}")

        if not bool(os.listdir(tuning_sql_path)):
            self.logger.warn("tuning sql text path not exist")
            return
        else:
            self.is_exist_tuning_sql = True

        tuning_sql_list = os.listdir(tuning_sql_path)

        self.logger.info(f"tuning sql path {len(tuning_sql_list)} things exist")

        dict_keys_set = set(self.origin_sql_id_dict.keys())
        list_set = set(tuning_sql_list)

        new_sql_id_list = list(list_set - dict_keys_set)
        del_sql_id_list = list(dict_keys_set - list_set)

        if bool(del_sql_id_list):
            self.logger.info(f"{str(del_sql_id_list)} sql_id deleted.")
            self._remove_sql_id_in_object(del_sql_id_list)

        if not bool(new_sql_id_list):
            self.logger.info("new sql id path not detected.")
            return

        self.origin_sql_id_dict.update(dict.fromkeys(new_sql_id_list))

        self._set_tuning_sql_text_in_txt(tuning_sql_path)

    def _set_tuning_sql_text_in_txt(self, tuning_sql_path):
        self._init_sa_target()

        for sql_id, pre_sql_text in self.origin_sql_id_dict.items():

            if pre_sql_text is None:
                self.logger.info(f"new sql id : {sql_id}")

                tuning_sql_files = SystemUtils.get_filenames_from_path(
                    os.path.join(tuning_sql_path, sql_id), suffix=".sql"
                )

                if len(tuning_sql_files) > 1:
                    self.logger.warn(f"[{sql_id}] tuning sql file {len(tuning_sql_files)} exist. Check file in path")

                tuning_sql_file = tuning_sql_files[0]

                sql_text = self._get_sql_text_in_file(os.path.join(tuning_sql_path, sql_id, tuning_sql_file))

                self.origin_sql_id_dict[sql_id] = self._preprocessing_tuning_sql(sql_text)
                cluster_id_df = self.st.get_ae_was_sql_text_by_sql_id(sql_id)
                self.cluster_id_by_origin_sql_id_dict[sql_id] = cluster_id_df.iloc[0]['cluster_id'] \
                    if len(cluster_id_df) > 0 else ''

        self._teardown_sa_target()

    @staticmethod
    def _preprocessing_tuning_sql(sql_text):
        sql_text = sql_text.lower().strip()
        sql_text = sql_text.replace(',', ' ')
        sql_text = re.sub(r'\s+in\s?\([^)]*\)', ' in(<:args:>)', sql_text)
        sql_text = re.sub(r'\s+values\s?\([^)]*\)', ' values(<:args:>)', sql_text)
        sql_text = re.sub(r"\d{2,4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}\.?\d{0,3}", '<DATETIME>', sql_text)
        sql_text = re.sub(r"\d{2,4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.?\d{0,3}", '<DATETIME>', sql_text)
        sql_text = re.sub(r"\d{4}\-\d{2}\-\d{2}", '<DATE>', sql_text)
        return re.split(r"\s+", sql_text)

    @staticmethod
    def _get_sql_text_in_file(sql_file):
        sql = []
        with open(sql_file, 'r', encoding='utf-8') as file:
            while True:
                line = file.readline()
                if not line:
                    break

                sql.append(line.strip())
            file.close()

        return ' '.join(sql)

    def _get_xapm_txn_sql_detail(self):
        self._init_im_target()

        start_date, end_date = self._get_time_param()
        self.logger.info(f"get_xapm_txn_sql_detail start_date : {start_date}, end_date : {end_date}")

        xapm_sql_df = self.imt.get_xapm_txn_sql_detail(start_date, end_date)
        self._teardown_im_target()
        return xapm_sql_df

    @staticmethod
    def _get_time_param():
        now = datetime.now()

        start_date = \
            now.replace(minute=now.minute - now.minute % INTERVAL_MINUTE, second=0, microsecond=0) - \
            timedelta(minutes=INTERVAL_MINUTE)
        end_date = now.replace(minute=now.minute - now.minute % INTERVAL_MINUTE, second=0, microsecond=0)

        return \
            start_date.strftime(DateFmtConstants.DATETIME_FORMAT), \
            end_date.strftime(DateFmtConstants.DATETIME_FORMAT)

    def _analysis_sql_text_similarity(self, xapm_sql_df):
        valid_df_list = []

        for origin_sql_id, pre_sql_text in self.origin_sql_id_dict.items():
            tmp_df = xapm_sql_df.copy(deep=True)

            tmp_df['similarity'] = tmp_df['sql_text_split'].apply(lambda x: self.jaccard_similarity(x, pre_sql_text))
            tmp_df.drop('sql_text_split', axis=1, inplace=True)
            tmp_df = tmp_df[tmp_df['similarity'] >= self.valid_sql_text_similarity]
            tmp_df['origin_sql_id'] = origin_sql_id
            tmp_df['origin_cluster_id'] = self.cluster_id_by_origin_sql_id_dict[origin_sql_id]
            tmp_df = tmp_df.sort_values(by='similarity', ascending=False)

            valid_df_list.append(tmp_df)

        result_valid_df = pd.concat(valid_df_list)
        result_valid_df['similarity'] = result_valid_df['similarity'].round(decimals=3)

        self.logger.info(f"valid similarity sql id {len(result_valid_df)} times")

        for idx, row in result_valid_df.iterrows():
            self.logger.info(f"{row['origin_sql_id']} ({row['origin_cluster_id']}) "
                             f"-> {row['sql_id']} , {row['similarity']}")

        return result_valid_df

    def _insert_ae_txn_sql_similarity(self, result_valid_df):
        self._init_sa_target()
        self.st.insert_ae_txn_sql_similarity(result_valid_df)
        self._teardown_sa_target()

    def _remove_sql_id_in_object(self, del_sql_id_list):
        for del_sql_id in del_sql_id_list:
            if self.origin_sql_id_dict.get(del_sql_id):
                del self.origin_sql_id_dict[del_sql_id]
            if self.cluster_id_by_origin_sql_id_dict.get(del_sql_id):
                del self.cluster_id_by_origin_sql_id_dict[del_sql_id]

    @staticmethod
    def jaccard_similarity(list1, list2):
        s1 = set(list1)
        s2 = set(list2)
        return float(len(s1.intersection(s2)) / len(s1.union(s2)))
