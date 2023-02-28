
class InterMaxExtractQuery:

    SELECT_XAPM_TXN_DETAIL = (
        # 22.08.01 export txn_detail() 메소드 jdbc_fetch_count 컬럼 추가
            """
                select  tid,
                        time,
                        start_time,
                        start_time + txn_elapse * '1 millisecond'::interval as end_time,
                        txn_id, 
                        was_id,      
                        txn_elapse,
                        txn_cpu_time,
                        thread_memory,
                        sql_exec_count,
                        sql_elapse,
                        sql_elapse_max,
                        fetch_count as fetched_rows,
                        fetch_time,
                        total_fetch_count as jdbc_fetch_count,
                        exception,
                        remote_count,
                        remote_elapse
                from xapm_txn_detail_p#(table_suffix)
            """
    )

    SELECT_XAPM_TXN_SQL_DETAIL = (

            # 22.07.15 export txn_sql_detail() 메소드 db_id 컬럼 추가
            # 22.08.01 export txn_sql_detail() 메소드 cursor_id 컬럼 추가
            """
                select  tid,
                        time, 
                        txn_id,
                        was_id,
                        sql_id,
                        execute_count,
                        elapsed_time,
                        elapsed_time_max,
                        db_id,
                        sid,
                        sql_seq,
                        cursor_id
                from xapm_txn_sql_detail_p#(table_suffix) 
            """
    )

    SELECT_XAPM_TXN_SQL_FETCH = (
            """
                select  tid,
                        time,                        
                        txn_id, 
                        server_id as was_id,      
                        cursor_id,                        
                        fetch_count as fetched_rows,
                        fetch_time,
                        fetch_time_max,
                        internal_fetch_count as jdbc_fetch_count      
                from xapm_txn_sql_fetch_p#(table_suffix)  
            """
    )

    SELECT_XAPM_WAS_STAT_SUMMARY = (
            """
                select time,
                        was_id,
                        active_users,
                        max_active_users,
                        active_txns,
                        max_active_txns,
                        db_sessions,
                        max_db_sessions,
                        active_db_sessions,
                        max_active_db_sessions,
                        jvm_cpu_usage,
                        max_jvm_cpu_usage,
                        jvm_free_heap,
                        max_jvm_free_heap,
                        jvm_heap_size,
                        max_jvm_heap_size,
                        jvm_used_heap,
                        max_jvm_used_heap,
                        jvm_thread_count,
                        max_jvm_thread_count,
                        jvm_gc_count,
                        max_jvm_gc_count,
                        max_txn_end_count,
                        sum_txn_end_count,
                        txn_elapse,
                        max_txn_elapse,
                        sql_exec_count,
                        max_sql_exec_count,
                        sql_elapse,
                        max_sql_elapse,
                        sql_prepare_count,
                        max_sql_prepare_count,
                        sql_fetch_count,
                        max_sql_fetch_count
                from xapm_was_stat_summary_p#(table_suffix) 
            """
    )

    SELECT_XAPM_JVM_STAT_SUMMARY = (
            """
                select time,
                        was_id,
                        compiles,
                        max_compiles,
                        compile_time,
                        max_compile_time,
                        class_count,
                        max_class_count,
                        loaded,
                        max_loaded,
                        class_time,
                        max_class_time,
                        jvm_gc_count
                from xapm_jvm_stat_summary_p#(table_suffix)
            """
    )

    SELECT_XAPM_OS_STAT_OSM = (
            """
                select  time,
                        host_id,
                        host_ip,
                        host_name,
                        os_cpu_sys,
                        os_cpu_user,
                        os_cpu_io,
                        os_free_memory,
                        os_total_memory,
                        swap_free,
                        swap_total
                from xapm_os_stat_osm_p#(table_suffix)           
            """
    )

    DELETE_INTERMAX_QUERY = (
            """
                delete from #(table_name) where to_char(time,'yyyymmdd')='#(date)'
            """
    )


class MaxGaugeExtractorQuery:

    SELECT_ORA_SESSION_INFO = (
            # 22.07.18 db_id 조건 제거
            """
               select partition_key,
                      db_id,
                      sid,
                      logon_time,
                      serial,
                      "time",
                      spid,
                      audsid,
                      (select s.value from apm_string_data s where s.id = si.user_name_id and s.id_type='USER_NAME') as schema,
                      (select s.value from apm_string_data s where s.id = si.os_user_id   and s.id_type='OS_USER') as os_user,
                      (select s.value from apm_string_data s where s.id = si.machine_id   and s.id_type='MACHINE') as machine,
                      (select s.value from apm_string_data s where s.id = si.terminal_id  and s.id_type='TERMINAL') as terminal,
                      cpid,
                      (select s.value from apm_string_data s where s.id = si.program_id   and id_type='PROGRAM') as program,
                      session_type
               from #(instance_name).ora_session_info si
               where partition_key =  #(partition_key)
               --and db_id=2
            """
    )

    SELECT_ORA_SESSION_STAT = (
            # 22.07.13 SELECT 쿼리 수정 sid, serial, logon_time, module, action, was_id, tan_name 컬럼 추가 및 변경
            # 22.07.18 db_id 조건 제거
            """
                select  distinct
                        partition_key, 
                        db_id,
                        sid,
                        serial,
                        logon_time,
                        time,
                        sql_uid,
                        sql_id,
                        sql_plan_hash,
                        module,
                        action,
                        client_identifier,
                        was_id,
                        split_part(client_identifier, ',' , 4) txn_name,
                        tid
                from #(instance_name).ora_session_stat
                where partition_key = #(partition_key)
                --and db_id=2
                and sql_id is not null                
            """
    )

    SELECT_APM_SQL_LIST = (
            # 22.07.18 db_id 조건 제거
            """
            SELECT  PARTITION_KEY
                    ,DB_ID
                    ,SQL_UID
                    ,SEQ
                    ,SQL_TEXT
            FROM #(instance_name).APM_SQL_LIST
            WHERE PARTITION_KEY = #(partition_key)
            --AND DB_ID=2
            """
    )

    SELECT_ORA_SQL_STAT_10 = (
            # 22.07.13 SELECT 쿼리 수정 ACTION 컬럼 추가
            # 22.07.18 db_id 조건 제거
            """
                SELECT  A.PARTITION_KEY 
                        ,A.DB_ID
                        ,A.TIME 
                        ,A.SQL_UID
                        ,A.SQL_ID 
                        ,A.SQL_PLAN_HASH 
                        ,(SELECT VALUE FROM APM_STRING_DATA WHERE ID = A.USER_NAME_ID AND ID_TYPE='USER_NAME') AS SCHEMA
                        ,(SELECT VALUE FROM APM_STRING_DATA WHERE ID = A.PROGRAM_ID AND ID_TYPE='PROGRAM') AS PROGRAM
                        ,A.MODULE 
                        ,A.ACTION 
                        ,(SELECT VALUE FROM APM_STRING_DATA WHERE ID = A.MACHINE_ID AND ID_TYPE='MACHINE') AS MACHINE
                        ,(SELECT VALUE FROM APM_STRING_DATA WHERE ID = A.OS_USER_ID AND ID_TYPE='OS_USER') AS OS_USER
                        ,ELAPSED_TIME
                        ,CPU_TIME
                        ,WAIT_TIME
                        ,LOGICAL_READS
                        ,PHYSICAL_READS
                        ,REDO_SIZE
                        ,EXECUTION_COUNT
                        --,SORT_DISK
                        --,SORT_ROWS
                        --,TABLE_FETCH_BY_ROWID
                        --,TABLE_FETCH_CONTINUED_BY_ROWID
                        --,TABLE_SCAN_BLOCKS_GOTTEN
                        --,TABLE_SCAN_ROWS_GOTTEN
                        --,IO_CELL_OFFLOAD_ELIGIBLE_BYTES
                        --,IO_INTERCONNECT_BYTES
                        --,OPTIMIZED_PHY_READ_REQUESTS
                        --,IO_CELL_UNCOMPRESSED_BYTES
                        --,IO_CELL_OFFLOAD_RETURNED_BYTES
                FROM  #(instance_name).ORA_SQL_STAT_10MIN A
                WHERE  PARTITION_KEY = #(partition_key)
                --AND  A.DB_ID = 2
            """
    )

    SELECT_ORA_SQL_WAIT_10 = (
            # 22.07.13 SELECT 쿼리 수정 ACTION 컬럼 추가
            # 22.07.18 db_id 조건 제거

            """
                SELECT A.PARTITION_KEY ,
                        A.DB_ID ,
                        A.TIME ,
                        A.SQL_UID,
                        A.SQL_ID ,
                        A.SQL_PLAN_HASH ,          
                        (SELECT VALUE FROM APM_STRING_DATA WHERE ID = A.USER_NAME_ID AND ID_TYPE='USER_NAME') AS SCHEMA,
                        (SELECT VALUE FROM APM_STRING_DATA WHERE ID = A.PROGRAM_ID AND ID_TYPE='PROGRAM') AS PROGRAM,
                        A.MODULE ,
                        A.ACTION ,   
                        (SELECT VALUE FROM APM_STRING_DATA WHERE ID = A.MACHINE_ID AND ID_TYPE='MACHINE') AS MACHINE,
                        (SELECT VALUE FROM APM_STRING_DATA WHERE ID = A.OS_USER_ID AND ID_TYPE='OS_USER') AS OS_USER,
                        A.WAIT_TIME ,
                        A.EVENT_VERSION,
                        B.EVENT_NAME,
                        B.WAIT_CLASS
                FROM #(instance_name).ORA_SQL_WAIT_10MIN A ,
                    ORA_EVENT_NAME B
                WHERE PARTITION_KEY = #(partition_key)
                --AND A.DB_ID = 2
                AND A.DB_ID=B.DB_ID
                AND A.EVENT_ID=B.EVENT_ID
                AND A.EVENT_VERSION=B.EVENT_VERSION
            """
    )

    DELETE_MAXGAUGE_QUERY = (
            """
                delete from #(table_name) where partition_key = #(partition_key)
            """
    )