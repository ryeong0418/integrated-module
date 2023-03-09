

class CommonSql:

    SELECT_AE_DB_INFO = (
        """
        SELECT db_id, instance_name FROM ae_db_info
        ORDER BY db_id asc
        """
    )

    SELECT_TABLE = (
        """
        SELECT * FROM #(table)
        """
    )
