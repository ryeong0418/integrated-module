
class InterMaxSqlTextMergeQuery:

    SELECT_XAPM_SQL_TEXT = (
        "select sql_id, sql_text, sql_text_100 from xapm_sql_text"
    )


class SaSqlTextMergeQuery:

    DROP_TABLE_AE_SQL_TEXT = (
        "truncate table ae_sql_text"
    )

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

    SELECT_AE_WAS_SQL_TEXT = (
        "select sql_id, sql_text_100, sql_text from ae_was_sql_text"
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

    SELECT_SQL_ID_AND_SQL_TEXT = (
        """
        SELECT ATSD.sql_id, AWST.sql_text, AWST.sql_text_100
        FROM ae_txn_sql_detail ATSD, ae_was_sql_text AWST
        WHERE ATSD.sql_id = AWST.sql_id
        AND ATSD.TIME > '""" + "#(StartDate)" + """'::timestamp
        and ATSD.TIME < '""" + "#(EndDate)" + """'::timestamp
        and ATSD.elapsed_time >= #(seconds)
        GROUP BY ATSD.sql_id, AWST.sql_text,AWST.sql_text_100
        """
    )
