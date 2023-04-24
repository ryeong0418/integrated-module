
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

    CREATE_DBLINK = (
        """CREATE extension if not exists dblink """
    )

    DBLINK_CONNECT = (
        """SELECT dblink_connect('#(intermax_db_info)')"""
    )

    SELECT_SQL_ID = (
        """
        select * from dblink('#(intermax_db_info)','select sql_id,sql_text_100,sql_text from xapm_sql_text') 
        AS t(sql_id varchar, sql_text_100 varchar, sql_text varchar) 
        where sql_id not in (select sql_id from ae_was_sql_text);
        """
    )

    DELETE_TABLE_DEFAULT_QUERY = (
        "delete from #(table_name)"
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

    UPDATE_AE_WAS_SQL_TEXT_BY_CLUSTER_ID_QUERY = (
        "UPDATE ae_was_sql_text set cluster_id = '#(cluster_id)' where sql_id = '#(sql_id)'"
    )
