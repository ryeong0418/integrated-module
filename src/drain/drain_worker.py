import pandas as pd

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.file_persistence import FilePersistence

from src.common.constants import SystemConstants, DrainConstants
from src.common.module_exception import ModuleException


class SelectConfig:
    MODEL_FILE_NAME = DrainConstants.DRAIN_SELECT_MODEL_FILE_NAME
    TEMPLATE_TREE_FILE_NAME = DrainConstants.DRAIN_SELECT_TEMPLATE_TREE_FILE_NAME
    CUSTOM_TAG = DrainConstants.SELECT_TEMPLATE_CLUSTER_ID_PREFIX


class EtcConfig:
    MODEL_FILE_NAME = DrainConstants.DRAIN_ETC_MODEL_FILE_NAME
    TEMPLATE_TREE_FILE_NAME = DrainConstants.DRAIN_ETC_TEMPLATE_TREE_FILE_NAME
    CUSTOM_TAG = DrainConstants.ETC_TEMPLATE_CLUSTER_ID_PREFIX


class DrainWorker:

    def __init__(self, config, drain_logger, target):
        self.config = config
        self.drain_logger = drain_logger
        self.model_file_name = None
        self.template_tree_file_name = None
        self.custom_tag = ''
        self.template_miner = None
        self.sql_text_template_tree = None
        self.line_count = 0
        self._init_config_by_target(target)

    def _init_config_by_target(self, target):
        if target == 'select':
            target_config = SelectConfig()
        elif target == 'etc':
            target_config = EtcConfig()
        else:
            raise ModuleException('E005')

        self.model_file_name = target_config.MODEL_FILE_NAME
        self.template_tree_file_name = target_config.TEMPLATE_TREE_FILE_NAME
        self.custom_tag = target_config.CUSTOM_TAG

    def init_drain(self):
        drain_config = TemplateMinerConfig()
        drain_config.load(f"{self.config['home']}"
                          f"{SystemConstants.DRAIN_CONF_PATH}"
                          f"{DrainConstants.DRAIN_INI_FILE_NAME}")

        self.template_miner = self._create_template_miner(drain_config)
        self.sql_text_template_tree = f"{self.config['home']}" \
                                      f"{SystemConstants.DRAIN_CONF_PATH}" \
                                      f"{self.template_tree_file_name}"

    def _create_template_miner(self, drain_config):
        persistence = FilePersistence(f"{self.config['home']}"
                                      f"{SystemConstants.DRAIN_CONF_PATH}"
                                      f"{self.model_file_name}")

        return TemplateMiner(persistence, drain_config)

    def match(self, df, target_col):
        df['cluster_id'] = '0'

        for idx, row in df.iterrows():
            cluster = self.template_miner.match(row[target_col])
            self.line_count += 1

            if cluster is None:
                self.drain_logger.info(f"No Match found")
                result = self.template_miner.add_log_message(row[target_col])
                cluster_id = self._add_custom_tag_in_cluster_id(result['cluster_id'])
                self.drain_logger.info(f"Create template id # {cluster_id}")

            else:
                cluster_id = self._add_custom_tag_in_cluster_id(cluster.cluster_id)
                self.drain_logger.info(f"Matched template # {cluster_id}")

            df.loc[idx, 'cluster_id'] = cluster_id

        return df

    def get_top_cluster_template(self):
        cluster_list = self._make_cluster_list()
        cluster_df = pd.DataFrame(cluster_list, columns=['cluster_id', 'sql_template', 'cluster_cnt'])
        return cluster_df

    def _add_custom_tag_in_cluster_id(self, cluster_id):
        return self.custom_tag + str(cluster_id)

    def print_drain_tree(self):
        self.template_miner.drain.print_tree(file=open(self.sql_text_template_tree, 'w', encoding="utf-8"))

        sorted_clusters = sorted(self.template_miner.drain.clusters, key=lambda it: it.size, reverse=True)

        self.drain_logger.info("*" * 79)
        for cluster in sorted_clusters:
            self.drain_logger.info(str(cluster).replace("ID=", f"ID={self.custom_tag}", 1))

        self.drain_logger.info("*" * 79)

    def _make_cluster_list(self):
        return [(self._add_custom_tag_in_cluster_id(cluster.cluster_id), ' '.join(cluster.log_template_tokens), 0)
                for cluster in self.template_miner.drain.clusters]
