
class CommonSql:

    SELECT_AE_DB_INFO = (
        """
        SELECT db_id, instance_name FROM ae_db_info
        ORDER BY db_id asc
        """
    )

    SELECT_TABLE_COLUMN_TYPE = (
        """
        SELECT column_name, data_type 
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = '#(table)'        
        """
    )

    SELECT_AE_WAS_DEV_MAP = (
        "select was_id from ae_was_dev_map"
    )


    DELETE_TABLE_DEFAULT_QUERY = (
        "delete from #(table_name)"
    )

    TRUNCATE_TABLE_DEFAULT_QUERY = (
        "truncate table #(table_name)"
    )

    DELETE_TABLE_BY_DATE_QUERY = (
        "delete from #(table_name) where to_char(time,'yyyymmdd')='#(date)'"
    )

    DELETE_TABLE_BY_PARTITION_KEY_QUERY = (
        "delete from #(table_name) where partition_key = #(partition_key)"
    )

    DELETE_SUMMARY_TABLE_BY_DATE_QUERY = (
        "delete from #(table_name) where to_char(ten_min_time,'yyyymmdd')='#(date)'"
    )

    DUPLICATE_CHECK_AE_WAS_SQL_TEXT = (
        """
        INSERT INTO ae_was_sql_text (sql_id, sql_text)
        VALUES ('#(sql_id)', '#(sql_value)')
        ON CONFLICT (sql_id) 
        DO NOTHING
        """
    )