
class InterMaxSqlTextMergeQuery:

    SELECT_XAPM_SQL_TEXT = (
        "select sql_id, sql_text, sql_text_100 from xapm_sql_text"
    )


class SaSqlTextMergeQuery:

    DROP_TABLE_AE_SQL_TEXT = (
        "truncate table ae_sql_text"
    )

    SELECT_AE_DB_SQL_TEXT = (
        """
        select sql_uid,
            sql_full(a.db_id, a.partition_key, a.sql_uid) as sql_text
        from (	   
                select sql_uid,
                    db_id,
                    partition_key	   		   
                from ae_db_sql_text 
            ) a	 
        """
    )
