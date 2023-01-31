

class SystemConstants:
    CONFIG_FILE_PATH = 'config'
    LOGGER_FILE_PATH = 'logger'

    # env 별 config 파일 이름 prefix, suffix
    CONFIG_FILE_PREFIX = 'config-'
    CONFIG_FILE_SUFFIX = '.json'

    # env 별 logger 파일 이름 prefix, suffix
    LOGGER_FILE_PREFIX = 'logger-'
    LOGGER_FILE_SUFFIX = '.json'

    # logger master 파일 이름
    MASTER_LOG_FILE_NAME = 'master'

    # logs 폴더명
    LOGGER_PATH = 'logs'


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

    AE_EXCUTE_LOG = "ae_excute_log"
