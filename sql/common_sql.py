class CommonSql:
    """
    CommonSql class (공통 sql)
    """

    SELECT_TABLE_COLUMN_TYPE = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = '#(table)'
    """

    DELETE_TABLE_DEFAULT_QUERY = """
        delete
        from #(table_name)
    """

    TRUNCATE_TABLE_DEFAULT_QUERY = """
        truncate
        table #(table_name)
    """

    DELETE_TABLE_BY_DATE_QUERY = """
        delete from #(table_name)
        where to_char(time,'yyyymmdd')='#(date)'
    """

    DELETE_TABLE_BY_PARTITION_KEY_QUERY = """
        delete from #(table_name)
        where partition_key = #(partition_key)
    """

    DELETE_SUMMARY_TABLE_BY_DATE_QUERY = """
        delete from #(table_name)
        where to_char(ten_min_time,'yyyymmdd')='#(date)'
    """

    DELETE_EXTEND_TABLE_BY_DATE_QUERY = """
        delete from #(table_name)
        where to_char(end_time,'yyyy-mm-dd') >= '#(StartDate)'
        and to_char(end_time, 'yyyy-mm-dd') < '#(EndDate)'
        and lower(identifier) = lower('#(identifier)')
    """


class AeWasSqlTextSql:
    """
    ae_was_sql_text table sql class
    """

    SELECT_AE_WAS_SQL_TEXT = """
        select sql_id, sql_text
        from ae_was_sql_text
    """

    SELECT_BY_NO_CLUSTER_ID = """
        select sql_id, sql_text
        from ae_was_sql_text
        where cluster_id is null
    """

    UPDATE_BY_NO_ANALYZED_TARGET = """
        update ae_was_sql_text
        set cluster_id = '0'
        where cluster_id is null
    """

    SELECT_CLUSTER_CNT_BY_GROUPING = """
        select cluster_id, count(*) as cluster_cnt
        from ae_was_sql_text
        where 1=1
        and cluster_id != '0'
        and cluster_id is not null
        group by cluster_id
        order by count(*) desc
    """

    UPDATE_CLUSTER_ID_BY_SQL_ID = """
        UPDATE ae_was_sql_text
        set cluster_id = '#(cluster_id)'
        where sql_id = '#(sql_id)'
    """

    SELECT_SQL_ID_AND_SQL_TEXT = """
        select AWST.sql_id, AWST.sql_text, AWST.sql_text_100
        from ae_was_sql_text AWST
        where exists (select 'X'
                      from ae_txn_sql_detail ATSD
                      where AWST.sql_id = ATSD.sql_id
                      and ATSD.TIME >= '#(StartDate)'::timestamp
                      and ATSD.TIME < '#(EndDate)'::timestamp
                      and ATSD.elapsed_time >= #(seconds)
                     )
    """

    SELECT_CLUSTER_ID_BY_SQL_ID = """
        select cluster_id
        from ae_was_sql_text
        where sql_id = '#(sql_id)'
    """


class AeDbSqlTemplateMapSql:
    """
    ae_db_sql_template_map table sql class.
    """

    UPSERT_CLUSTER_ID_BY_SQL_UID = """
        INSERT INTO ae_db_sql_template_map (sql_uid, cluster_id)
        VALUES ('#(sql_uid)', '#(cluster_id)')
        ON CONFLICT (sql_uid)
        DO UPDATE
        SET cluster_id = EXCLUDED.cluster_id
    """

    SELECT_CLUSTER_CNT_BY_GROUPING = """
        select cluster_id, count(*) as cluster_cnt
        from ae_db_sql_template_map
        where 1=1
        and cluster_id != '0'
        and cluster_id is not null
        group by cluster_id
        order by count(*) desc
    """


class AeDbInfoSql:
    """
    ae_db_info table sql class.
    """

    SELECT_AE_DB_INFO = """
        SELECT db_id, instance_name
        FROM ae_db_info
        ORDER BY db_id asc
    """


class AeDbSqlTextSql:
    """
    ae_db_sql_text table sql class.
    """

    SELECT_AE_DB_SQL_TEXT_1SEQ = """
        select sql_uid, partition_key
        from ae_db_sql_text
        where 1=1
        and partition_key = #(partition_key)
        and seq = 1
    """

    SELECT_AE_DB_SQL_TEXT_1SEQ_ORDERBY = """
        select sql_uid, partition_key
        from ae_db_sql_text
        where 1=1
        and partition_key = #(partition_key)
        and seq = 1
        group by sql_uid, partition_key
        order by count(seq) asc
    """

    SELECT_AE_DB_SQL_TEXT_WITH_DATA = """
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


class AeWasDevMapSql:
    """
    ae_was_dev_map table sql class.
    """

    SELECT_AE_WAS_DEV_MAP = """
        select was_id
        from ae_was_dev_map
    """


class AeSqlStat10min:
    """
    ae_sql_stat_10min table sql class.
    """

    SELECT_AE_SQL_STAT_10MIN_BY_SQL_UID_AND_PARTITION_KEY = """
        select
            a.sql_uid,
            a.sql_id,
            sum(a.execution_count) as exec,
            sum(a.elapsed_time) as "sum_elapsed_time",
            sum(a.cpu_time) as "sum_cpu_time",
            sum(a.logical_reads) as "sum_lio",
            sum(a.physical_reads) as "sum_pio",
            case when sum(a.execution_count) != 0
                 then sum(a.elapsed_time) / sum(a.execution_count)
                 else 0 end as "avg_elapsed_time",
            case when sum(a.execution_count) != 0
                 then sum(a.cpu_time) / sum(a.execution_count)
                 else 0 end as "avg_cpu_time",
            case when sum(a.execution_count) != 0
                 then sum(a.logical_reads) / sum(a.execution_count)
                 else 0 end as "avg_lio",
            case when sum(a.execution_count) != 0
                 then sum(a.physical_reads) / sum(a.execution_count)
                 else 0 end as "avg_pio"
        from (
            select sql_uid, sql_id, elapsed_time , logical_reads , physical_reads, execution_count, cpu_time
            from ae_sql_stat_10min assm
            where sql_uid in (#(sql_uids))
            and partition_key in (#(partition_keys))
        ) as a
        group by a.sql_uid, a.sql_id
    """

    SELECT_AE_SESSION_STAT_10MIN_BY_SQL_ID = """
        select distinct sql_uid, sql_id
        from ae_sql_stat_10min assm
        where sql_id = '#(sql_id)'
    """


class XapmTxnSqlDetailSql:
    """
    (intermax) xapm_txn_sql_detail table sql class
    """

    SELECT_XAPM_TXN_SQL_DETAIL = """
        select xts.txn_id, xts.sql_id, xst.sql_text
        from (
            select xtsd.txn_id, xtsd.sql_id
            from xapm_txn_sql_detail xtsd
            where xtsd.time >= '#(start_param)'
            and xtsd.time < '#(end_param)'
            group by xtsd.txn_id, xtsd.sql_id
        ) as xts
        join xapm_sql_text xst
        on xts.sql_id = xst.sql_id
    """
