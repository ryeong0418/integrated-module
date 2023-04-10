

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
