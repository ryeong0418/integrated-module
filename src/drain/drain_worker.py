import pandas as pd
import os

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from drain3.file_persistence import FilePersistence

from src.common.constants import SystemConstants, DrainConstants
from src.common.module_exception import ModuleException


class SelectConfig:
    """
    select 타겟 Drain worker에서 사용할 설정값 class
    """

    MODEL_FILE_NAME = DrainConstants.DRAIN_SELECT_MODEL_FILE_NAME
    TEMPLATE_TREE_FILE_NAME = DrainConstants.DRAIN_SELECT_TEMPLATE_TREE_FILE_NAME
    CUSTOM_TAG = DrainConstants.SELECT_TEMPLATE_CLUSTER_ID_PREFIX


class EtcConfig:
    """
    etc 타겟 Drain worker에서 사용할 설정값 class
    """

    MODEL_FILE_NAME = DrainConstants.DRAIN_ETC_MODEL_FILE_NAME
    TEMPLATE_TREE_FILE_NAME = DrainConstants.DRAIN_ETC_TEMPLATE_TREE_FILE_NAME
    CUSTOM_TAG = DrainConstants.ETC_TEMPLATE_CLUSTER_ID_PREFIX


class DrainWorker:
    """
    Drain 알고리즘을 실제 수행하는 worker Class
    """

    def __init__(self, config, drain_logger, target, seq):
        self.config = config
        self.drain_logger = drain_logger
        self.model_file_name = None
        self.template_tree_file_name = None
        self.custom_tag = ""
        self.template_miner = None
        self.sql_text_template_tree = None
        self.line_count = 0
        self.seq = seq
        self._init_config_by_target(target)

    def _init_config_by_target(self, target):
        """
        타겟 별 init (config 설정) 함수
        :param target:
        :return:
        """
        if target == "select":
            target_config = SelectConfig()
        elif target == "etc":
            target_config = EtcConfig()
        else:
            raise ModuleException("E005")

        self.model_file_name = target_config.MODEL_FILE_NAME.replace("#(seq)", self.seq)
        self.template_tree_file_name = target_config.TEMPLATE_TREE_FILE_NAME.replace("#(seq)", self.seq)
        self.custom_tag = target_config.CUSTOM_TAG.replace("#(seq)", self.seq)

    def init_drain(self):
        """
        Drain 알고리즘에서 사용하눈 config 설정 및 인스턴스 생성
        :return:
        """
        drain_config = TemplateMinerConfig()
        drain_config.load(
            f"{self.config['home']}" f"{SystemConstants.DRAIN_CONF_PATH}" f"{DrainConstants.DRAIN_INI_FILE_NAME}"
        )

        drain_config.drain_depth = self.config["drain_config_depth"]

        self.template_miner = self._create_template_miner(drain_config)
        sql_text_template_tree_path = (
            f"{self.config['home']}" f"{SystemConstants.DRAIN_CONF_PATH}" f"{SystemConstants.DRAIN_TREE_PATH}"
        )

        if not os.path.exists(sql_text_template_tree_path):
            os.makedirs(sql_text_template_tree_path)

        self.sql_text_template_tree = f"{sql_text_template_tree_path}" f"{self.template_tree_file_name}"

    def _create_template_miner(self, drain_config):
        """
        Drain 인스턴스 생성 함수
        :param drain_config: drain config
        :return: 생성한 인스턴스 객체
        """
        sql_text_template_model_path = (
            f"{self.config['home']}" f"{SystemConstants.DRAIN_CONF_PATH}" f"{SystemConstants.DRAIN_MODEL_PATH}"
        )

        if not os.path.exists(sql_text_template_model_path):
            os.makedirs(sql_text_template_model_path)

        persistence = FilePersistence(f"{sql_text_template_model_path}" f"{self.model_file_name}")

        return TemplateMiner(persistence, drain_config)

    def match(self, df, target_col):
        """
        Drain template 분석 (cluster) 함수
        :param df: 분석 할 데이터 프레임
        :param target_col: 분석 할 데이터 프레임의 타겟 컬럼
        :return: 분석 완료된 데이터 프레임
        """
        df["cluster_id"] = "0"

        for idx, row in df.iterrows():
            cluster = self.template_miner.match(row[target_col])
            self.line_count += 1

            if cluster is None:
                self.drain_logger.info("No Match found")
                result = self.template_miner.add_log_message(row[target_col])
                cluster_id = self._add_custom_tag_in_cluster_id(result["cluster_id"])
                self.drain_logger.info(f"Create template id # {cluster_id}")

            else:
                cluster_id = self._add_custom_tag_in_cluster_id(cluster.cluster_id)
                self.drain_logger.info(f"Matched template # {cluster_id}")

            df.loc[idx, "cluster_id"] = cluster_id

        return df

    def get_top_cluster_template(self):
        """
        Drain 분석된 모델에서 top cluster template를 반환하는 함수
        :return: 분석된 cluster template 데이터 프레임
        """
        cluster_list = self._make_cluster_list()
        cluster_df = pd.DataFrame(cluster_list, columns=["cluster_id", "sql_template"])
        return cluster_df

    def _add_custom_tag_in_cluster_id(self, cluster_id):
        """
        Drain 모델에서 생성한 cluster_id에 타겟 별 custom tag를 붙이는 함수
        :param cluster_id:
        :return:
        """
        return self.custom_tag + str(cluster_id)

    def print_drain_tree(self):
        """
        분석된 drain cluster tree를 파일로 생성하고 로깅 하기 위한 함수
        :return:
        """
        with open(self.sql_text_template_tree, "w", encoding="utf-8") as file:
            self.template_miner.drain.print_tree(file=file)

        sorted_clusters = sorted(self.template_miner.drain.clusters, key=lambda it: it.size, reverse=True)

        self.drain_logger.info("*" * 79)
        for cluster in sorted_clusters:
            self.drain_logger.info(str(cluster).replace("ID=", f"ID={self.custom_tag}", 1))

        self.drain_logger.info("*" * 79)

    def _make_cluster_list(self):
        """
        분석된 drain cluster id와 template를 만드는 함수
        :return:
        """
        return [
            (self._add_custom_tag_in_cluster_id(cluster.cluster_id), " ".join(cluster.log_template_tokens))
            for cluster in self.template_miner.drain.clusters
        ]
