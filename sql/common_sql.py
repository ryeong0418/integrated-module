
class CommonSql:

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

    TRUNCATE_TABLE_DEFAULT_QUERY = (
        "truncate table #(table_name)"
    )


class AeWasSqlTextSql:

    SELECT_AE_WAS_SQL_TEXT = (
        "select sql_id, sql_text_100, sql_text from ae_was_sql_text"
    )

    SELECT_BY_NO_CLUSTER_ID = (
        "select sql_id, sql_text "
        "from ae_was_sql_text "
        "where cluster_id is null"
    )

    UPDATE_BY_NO_ANALYZED_TARGET = (
        "update ae_was_sql_text set cluster_id = '0' where cluster_id is null"
    )

    SELECT_CLUSTER_CNT_BY_GROUPING = (
        "select cluster_id, count(*) as cluster_cnt "
        "from ae_was_sql_text " 
        "where 1=1 "
        "and cluster_id != '0' "
        "and cluster_id is not null "
        "group by cluster_id "
        "order by count(*) desc "
    )

    UPDATE_CLUSTER_ID_BY_SQL_ID = (
        "UPDATE ae_was_sql_text set cluster_id = '#(cluster_id)' where sql_id = '#(sql_id)'"
    )

    SELECT_SQL_ID_AND_SQL_TEXT = (
        """
        select AWST.sql_id, AWST.sql_text, AWST.sql_text_100
        from ae_was_sql_text AWST
        where exists (select 'X'
                      from ae_txn_sql_detail ATSD
                      where AWST.sql_id = ATSD.sql_id
                      and ATSD.TIME >= '""" + "#(StartDate)" + """'::timestamp
                      and ATSD.TIME < '""" + "#(EndDate)" + """'::timestamp
                      and ATSD.elapsed_time >= #(seconds)
        )
        """
    )


class AeDbSqlTemplateMapSql:

    UPSERT_CLUSTER_ID_BY_SQL_UID = (
        "INSERT INTO ae_db_sql_template_map (sql_uid, cluster_id) "
        "VALUES ('#(sql_uid)', '#(cluster_id)') "
        "ON CONFLICT (sql_uid) "
        "DO UPDATE "
        "SET cluster_id = EXCLUDED.cluster_id"
    )

    SELECT_CLUSTER_CNT_BY_GROUPING = (
        "select cluster_id, count(*) as cluster_cnt "
        "from ae_db_sql_template_map " 
        "where 1=1 "
        "and cluster_id != '0' "
        "and cluster_id is not null "
        "group by cluster_id "
        "order by count(*) desc "
    )


class AeDbInfoSql:

    SELECT_AE_DB_INFO = (
        """
        SELECT db_id, instance_name FROM ae_db_info
        ORDER BY db_id asc
        """
    )


class AeDbSqlTextSql:

    SELECT_AE_DB_SQL_TEXT_1SEQ = (
        """
        select sql_uid,            
            partition_key             		   
        from ae_db_sql_text
        where 1=1 
        and partition_key = #(partition_key)
        and seq = 1
        """
    )

    SELECT_AE_DB_SQL_TEXT_WITH_DATA = (
        """
            with data(sql_uid, partition_key) as (
                values %s
            )
            select ae.sql_text
                 , ae.partition_key
                 , ae.sql_uid
                 , ae.seq             
            from ae_db_sql_text ae
            JOIN data d
            ON ae.sql_uid = d.sql_uid
            AND ae.partition_key = d.partition_key
            order by ae.partition_key asc, ae.sql_uid asc, ae.seq asc
        """
    )
