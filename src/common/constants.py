

class SystemConstants:
    CONFIG_FILE_PATH = 'config'
    LOGGER_FILE_PATH = 'logger'

    # env 별 config 파일 이름 prefix, suffix
    CONFIG_FILE_PREFIX = 'config-'
    CONFIG_FILE_SUFFIX = '.json'

    # env 별 logger 파일 이름 prefix, suffix
    LOGGER_FILE_PREFIX = 'logger-'
    LOGGER_FILE_SUFFIX = '.json'

    # logger 파일 이름
    MASTER_LOG_FILE_NAME = 'master'
    ETC_LOG_FILE_NAME = 'etc'
    SCHEDULER_LOG_FILE_NAME = 'scheduler'
    SQL_TEXT_TEMPLATE_LOG_FILE_NAME = 'sql_text_template'

    # logs 폴더명
    LOGGER_PATH = 'logs'

    # db sql text parquet 파일명
    DB_SQL_TEXT_FILE_NAME = 'ae_db_sql_text'

    # parquet 파일 확장자
    PARQUET_FILE_EXT = '.parquet'   

    # export 폴더명
    EXPORT_PARQUET_PATH = 'export/parquet'
    EXPORT_ETC_PATH = 'export/etc'
    SQL_PATH = 'export/sql_excel/sql'
    EXCEL_PATH = 'export/sql_excel/excel'

    # batch_pid_tmp
    TMP_PATH = 'tmp'
    PID_TMP_FILE_NAME = 'pid.tmp'

    # sql path
    SQL = 'sql'
    DDL = 'ddl'
    META = 'meta'

    # sql_text_template 관련 파일 경로
    DRAIN_CONF_PATH = '/resources/drain/'

    # decoding jar 파일
    DECODING_JAR_FILE_PATH = '/resources/intermax_decoding'

    # jar 파일 확장자
    JAR_FILE_EXT = '.jar'


class TableConstants:
    AE_WAS_INFO = "ae_was_info"
    AE_WAS_DB_INFO = "ae_was_db_info"
    AE_TXN_NAME = "ae_txn_name"
    AE_WAS_SQL_TEXT = "ae_was_sql_text"

    AE_DB_INFO = "ae_db_info"
    AE_DB_SQL_TEXT = "ae_db_sql_text"
    AE_SQL_TEXT = "ae_sql_text"

    AE_TXN_DETAIL = "ae_txn_detail"
    AE_TXN_SQL_DETAIL = "ae_txn_sql_detail"
    AE_TXN_SQL_FETCH = "ae_txn_sql_fetch"

    AE_SESSION_INFO = "ae_session_info"
    AE_SESSION_STAT = "ae_session_stat"
    AE_SQL_STAT_10MIN = "ae_sql_stat_10min"
    AE_SQL_WAIT_10MIN = "ae_sql_wait_10min"
    AE_TXN_SQL_SUMMARY = "ae_txn_sql_summary"

    AE_WAS_STAT_SUMMARY = "ae_was_stat_summary"
    AE_JVM_STAT_SUMMARY = "ae_jvm_stat_summary"
    AE_WAS_OS_STAT_OSM = "ae_was_os_stat_osm"
    AE_EXECUTE_LOG = "ae_execute_log"
    AE_SQL_TEMPLATE = "ae_sql_template"
    AE_DB_SQL_TEMPLATE_MAP = "ae_db_sql_template_map"


class ResultConstants:
    SUCCESS = 'S'
    FAIL = 'F'
    ERROR = 'E'
    PROGRESS = 'P'


class DrainConstants:
    # drain ini 파일 이름
    DRAIN_INI_FILE_NAME = 'drain3.ini'

    # sql_text_template model 파일 이름
    DRAIN_SELECT_MODEL_FILE_NAME = 'select_sql_text_template.bin'
    DRAIN_ETC_MODEL_FILE_NAME = 'etc_sql_text_template.bin'

    # sql_text_template tree 파일 이름 debug용
    DRAIN_SELECT_TEMPLATE_TREE_FILE_NAME = 'select_sql_text_template.tree'
    DRAIN_ETC_TEMPLATE_TREE_FILE_NAME = 'etc_sql_text_template.tree'

    # cluster_id custom tag
    SELECT_TEMPLATE_CLUSTER_ID_PREFIX = 'S_'
    ETC_TEMPLATE_CLUSTER_ID_PREFIX = 'E_'
