
class InterMaxInitializeQuery:

    ###############################################################
    # InterMax Table Lists
    ###############################################################
    # AE_TXN_NAME
    # AE_WAS_INFO
    # AE_WAS_SQL_TEXT
    # AE_WAS_DB_INFO 22.07.15 신규 추가
    #
    # AE_TXN_DETAIL
    # AE_TXN_SQL_DETAIL
    # AE_TXN_SQL_FETCH
    ###############################################################

    DDL_SQL = (
        """ 
            CREATE TABLE AE_TXN_NAME (
                txn_id varchar(40) NULL,
                txn_name varchar(256) NULL,
                business_id int4 NULL,
                business_name varchar(256) NULL,
                modified_time timestamp NULL,
                create_dt timestamp with time zone default current_timestamp,
                create_id varchar(20) default 'system' not NULL
            )
        """,
        """ 
            CREATE TABLE AE_WAS_INFO (
                was_id int4 NULL,
                was_name varchar(128) NULL,
                host_name varchar(64) NULL,
                create_dt timestamp with time zone default current_timestamp,
                create_id varchar(20) default 'system' not NULL
            )
        """,
        # 22.07.13 테이블 명칭 변경 AE_APM_SQL_TEXT -> AE_WAS_SQL_TEXT
        """ 
            CREATE TABLE AE_WAS_SQL_TEXT (
                sql_id varchar(40) NULL,
                sql_text_100 varchar(100) NULL,
                sql_text text NULL,
                create_dt timestamp with time zone default current_timestamp,
                create_id varchar(20) default 'system' not NULL       
            )
        """,
        # 22.07.15 테이블 신규 추가 AE_WAS_DB_INFO
        """ 
            CREATE TABLE AE_WAS_DB_INFO (
                db_id int4 NULL,                   
                instance_name varchar(64) NULL,
                instance_alias varchar(100) NULL,                    
                db_type varchar(16) NULL,
                host_name varchar(64) NULL,
                host_ip varchar(16) NULL,
                sid varchar(64) NULL,
                lsnr_port int8 NULL,
                create_dt timestamp with time zone default current_timestamp,
                create_id varchar(20) default 'system' not NULL
            )
        """,
        # 22.08.01 jdbc_fetch_count 컬럼 추가
        """
            CREATE TABLE AE_TXN_DETAIL (
                tid int8 NULL,
                time timestamp NULL,
                start_time timestamp NULL,
                end_time timestamp NULL,
                txn_id varchar(40) NULL,
                was_id int4 NULL,
                txn_elapse int4 NULL,
                txn_cpu_time int8 NULL,
                thread_memory int8 NULL,
                sql_exec_count int4 NULL,
                sql_elapse int4 NULL,
                sql_elapse_max int4 NULL,
                fetched_rows int4 NULL,
                fetch_time int4 NULL,
                jdbc_fetch_count int4 NULL,
                exception int4 NULL,
                remote_count int4 NULL,
                remote_elapse int4 NULL,
                create_dt timestamp with time zone default current_timestamp,
                create_id varchar(20) default 'system' not NULL
            )
        """,
        # 22.07.15 테이블 컬럼 추가 db_id
        # 22.08.01 테이블 컬럼 추가 cursor_id
        """ 
            CREATE TABLE AE_TXN_SQL_DETAIL (
                    tid int8 NULL,
                    "time" timestamp NULL,
                    txn_id varchar(40) NULL,
                    was_id int4 NULL,
                    sql_id varchar(40) NULL,
                    execute_count int4 NULL,
                    elapsed_time int4 NULL,
                    elapsed_time_max int4 NULL,
                    db_id int4 NULL,
                    sid int4 NULL,
                    sql_seq int4 NULL,
                    cursor_id int8 NULL,
                    create_dt timestamp with time zone default current_timestamp,
                    create_id varchar(20) default 'system' not NULL
            )
        """,
        # 22.08.01 테이블 신규 추가 AE_TXN_SQL_FETCH
        """ 
            CREATE TABLE AE_TXN_SQL_FETCH (
                    tid int8 NULL,
                    "time" timestamp NULL,
                    txn_id varchar(40) NULL,
                    was_id int4 NULL,
                    cursor_id int8 NULL,
                    fetched_rows int4 NULL,
                    fetch_time int4 NULL,
                    fetch_time_max int4 NULL,
                    jdbc_fetch_count int4 NULL,
                    create_dt timestamp with time zone default current_timestamp,
                    create_id varchar(20) default 'system' not NULL                     
            )
        """,
        # 23.02.27 테이블 신규 추가 AE_WAS_STAT_SUMMARY
        """
            CREATE TABLE AE_WAS_STAT_SUMMARY(
                "time" timestamp NULL,
                was_id int4 NULL,
                active_users float8 NULL,
                max_active_users int4 NULL,
                active_txns float8 NULL,
                max_active_txns int4 NULL,
                db_sessions float8 NULL,
                max_db_sessions int4 NULL,
                active_db_sessions float8 NULL,
                max_active_db_sessions int4 NULL,
                jvm_cpu_usage float8 NULL,
                max_jvm_cpu_usage int8 NULL,
                jvm_free_heap float8 NULL,
                max_jvm_free_heap int8 NULL,
                jvm_heap_size float8 NULL,
                max_jvm_heap_size int8 NULL,
                jvm_used_heap float8 NULL,
                max_jvm_used_heap int8 NULL,
                jvm_thread_count float8 NULL,
                max_jvm_thread_count int8 NULL,
                jvm_gc_count float8 NULL,
                max_jvm_gc_count int8 NULL,
                max_txn_end_count int8 NULL,
                sum_txn_end_count int8 NULL,
                txn_elapse float8 NULL,
                max_txn_elapse int4 NULL,
                sql_exec_count float8 NULL,
                max_sql_exec_count int8 NULL,
                sql_elapse float8 NULL,
                max_sql_elapse int4 NULL,
                sql_prepare_count float8 NULL,
                max_sql_prepare_count int8 NULL,
                sql_fetch_count float8 NULL,
                max_sql_fetch_count int8 NULL,
                create_dt timestamp default current_timestamp,
                create_id varchar(20) default 'system' not NULL 
            )
        """,
        # 23.02.27 테이블 신규 추가 AE_JVM_STAT_SUMMARY
        """
            CREATE TABLE AE_JVM_STAT_SUMMARY(
                time timestamp NULL,
                was_id int4 NULL,
                compiles float8 NULL,
                max_compiles int8 NULL,
                compile_time float8 NULL,
                max_compile_time int8 NULL,
                class_count	float8 NULL,
                max_class_count	int8 NULL,
                loaded float8 NULL,
                max_loaded int8 NULL,
                class_time float8 NULL,
                max_class_time int8 NULL,
                eden_size_avg float8 NULL,
                eden_size_max int8 NULL,
                eden_capacity_avg float8 NULL,
                eden_capacity_max int8 NULL,
                eden_used_avg float8 NULL,
                eden_used_max int8 NULL,
                old_size_avg float8 NULL,
                old_size_max int8 NULL,
                old_capacity_avg float8 NULL,
                old_capacity_max int8 NULL,
                old_used_avg float8 NULL,
                old_used_max int8 NULL,
                perm_size_avg float8 NULL,
                perm_size_max int8 NULL,
                perm_capacity_avg float8 NULL,
                perm_capacity_max int8 NULL,
                perm_used_avg float8 NULL,
                perm_used_max int8 NULL,
                jvm_gc_count float8 NULL,
                total_gc_count int8 NULL,
                tatal_gc_time int8 NULL,
                minor_gc_count int8 NULL,
                minor_gc_time int8 NULL,
                major_gc_count int8 NULL,
                major_gc_time int8 NULL,
                create_dt timestamp default current_timestamp,
                create_id varchar(20) default 'system' not NULL      
            )
        """,
        # 23.02.27 테이블 신규 추가 AE_WAS_OS_STAT_OSM
        """
            CREATE TABLE AE_WAS_OS_STAT_OSM(
                time timestamp NULL,
                host_id int8 NULL,
                host_ip	varchar NULL,
                host_name varchar NULL,
                os_cpu_sys int8 NULL,
                os_cpu_user int8 NULL,
                os_cpu_io int8 NULL,
                os_free_memory int8 NULL,
                os_total_memory int8 NULL,
                swap_free int8 NULL,
                swap_total int8 NULL,
                create_dt timestamp default current_timestamp,
                create_id varchar(20) default 'system' not NULL  
            )   
        """
    )

    CHECK_SQL = (
        """
            select n.nspname as schema_name,
                   c.relname as table_name
              from pg_class as c join pg_catalog.pg_namespace as n 
                                   on n.oid = c.relnamespace 
             where c.relname similar to '(ae_txn|ae_was)%'
            order by table_name
        """
    )

    SELECT_XAPM_WAS_INFO = (
        "select was_id, was_name, host_name from xapm_was_info"
    )

    SELECT_XAPM_TXN_NAME = (
        "select txn_id, txn_name, business_id, business_name  from xapm_txn_name"
    )

    SELECT_XAPM_SQL_TEXT = (
        "select sql_id, sql_text_100, sql_text from xapm_sql_text"
    )

    SELECT_XAPM_DB_INFO = (
        "select db_id, host_name, instance_name, instance_alias , db_type, host_ip, sid, lsnr_port from xapm_db_info"
    )

    # 23.02.27 신규 테이블 추가

    SELECT_XAPM_WAS_STAT_SUMMARY = (
        """ select   time, was_id, active_users, max_active_users, active_txns, max_active_txns, db_sessions, 
                     max_db_sessions, active_db_sessions, max_active_db_sessions, jvm_cpu_usage, max_jvm_cpu_usage, 
                     jvm_free_heap, max_jvm_free_heap, jvm_heap_size, max_jvm_heap_size, jvm_used_heap, 
                     max_jvm_used_heap, jvm_thread_count, max_jvm_thread_count, jvm_gc_count, max_jvm_gc_count,
                     max_txn_end_count, sum_txn_end_count, txn_elapse, max_txn_elapse, sql_exec_count, 
                     max_sql_exec_count, sql_elapse, max_sql_elapse, sql_prepare_count, max_sql_prepare_count, 
                     sql_fetch_count, max_sql_fetch_count 
            from xapm_was_stat_summary 
        """
    )

    # xapm_jvm_stat_summary column 일부 삭제

    SELECT_XAPM_JVM_STAT_SUMMARY = (
        """
            select  time, was_id, compiles, max_compiles, compile_time, max_compile_time, class_count, 
                    max_class_count, loaded, max_loaded, class_time, max_class_time, jvm_gc_count
            from xapm_jvm_stat_summary
        """
    )

    SELECT_XAPM_OS_STAT_OSM = (
        """
            select  time, host_id, host_ip, host_name, os_cpu_sys, os_cpu_user, os_cpu_io, os_free_memory,
                    os_total_memory, swap_free, swap_total
            from xapm_os_stat_osm        
        """
    )


class MaxGaugeInitializeQuery:

    ###############################################################
    # MaxGauge Table Lists
    ###############################################################
    # AE_DB_INFO
    # AE_DB_SQL_TEXT 22.07.13 테이블명 변경

    # AE_SESSION_STAT
    # AE_SQL_STAT_10MIN
    # AE_SQL_WAIT_10MIN
    # AE_SESSION_INFO 22.07.13 신규 추가
    ###############################################################

    DDL_SQL = (
        """ 
            CREATE TABLE AE_DB_INFO (
                DB_ID INT2 ,
                INSTANCE_NAME VARCHAR(64) ,
                HOST_IP VARCHAR(64) ,
                HOST_NAME VARCHAR(64) ,
                PORT INT8 ,
                DAEMON_USER VARCHAR(64) ,
                DAEMON_PASSWORD VARCHAR(256) ,
                DB_USER VARCHAR(64) ,
                DB_PASSWORD VARCHAR(256) ,
                JDBC_URL VARCHAR(512) ,
                CHAR_SET VARCHAR(64) ,
                ALERT_GROUP_NAME VARCHAR(64) ,
                RAC_GROUP_ID INT8 ,
                CELL_GROUP_ID INT8 ,
                DB_TYPE VARCHAR(16) ,
                SID VARCHAR(64) ,
                DB_NAME VARCHAR(64) ,
                LSNR_IP VARCHAR(64) ,
                LSNR_PORT INT8 ,
                OS_TYPE VARCHAR(64) ,
                ORACLE_VERSION VARCHAR(64) ,
                SQLNET INT8 ,
                PQ_INST_ID INT8 ,
                RAC_INST_NUMBER INT8 ,
                BUSINESS_NAME VARCHAR(64) ,
                IS_MASTER_RTS VARCHAR(1),
                BATCH_JOB_YN VARCHAR(1),
                create_dt timestamp with time zone default current_timestamp,
                create_id varchar(20) default 'system' not NULL  
            )
        """,
        # 22.07.13 테이블 명칭 변경 AE_SQL_LIST -> AE_DB_SQL_TEXT
        """ 
            CREATE TABLE AE_DB_SQL_TEXT (
                PARTITION_KEY INT4 NULL,
                DB_ID INT2 NULL,
                SQL_UID VARCHAR(48) NULL,	    
                SEQ INT2 NULL,
                SQL_TEXT VARCHAR(4000) NULL,
                create_dt timestamp with time zone default current_timestamp,
                create_id varchar(20) default 'system' not NULL
            )
        """,
        """
            CREATE TABLE AE_SESSION_STAT (
                PARTITION_KEY INT4 NULL,
                DB_ID INT2 NULL,
                SID INT8 NULL,
                SERIAL INT4 NULL,
                LOGON_TIME TIMESTAMP NULL,                    
                TIME TIMESTAMP NULL,
                SQL_UID VARCHAR(48) NULL,
                SQL_ID VARCHAR(13) NULL,
                SQL_PLAN_HASH INT8 NULL,
                MODULE VARCHAR(128) NULL,
                ACTION VARCHAR(128) NULL,		
                CLIENT_IDENTIFIER VARCHAR(64) NULL,
                WAS_ID INT8 NULL,
                TXN_NAME VARCHAR(64) NULL,
                TID INT8 NULL,
                create_dt timestamp with time zone default current_timestamp,
                create_id varchar(20) default 'system' not NULL
            )
        """,
        # 22.07.13 테이블 컬럼 추가 action
        """ 
            CREATE TABLE AE_SQL_STAT_10MIN (
                PARTITION_KEY INT4 NULL,
                DB_ID INT2 NULL,
                TIME TIMESTAMP NULL,	
                SQL_UID VARCHAR(48) NULL,
                SQL_ID VARCHAR(13) NULL,
                SQL_PLAN_HASH INT8 NULL,	
                SCHEMA VARCHAR(128) NULL,
                PROGRAM VARCHAR(128) NULL,
                MODULE VARCHAR(128) NULL,
                ACTION VARCHAR(128) NULL,
                MACHINE VARCHAR(128) NULL,
                OS_USER VARCHAR(128) NULL,
                ELAPSED_TIME INT8 NULL,
                CPU_TIME INT8 NULL,
                WAIT_TIME INT8 NULL,
                LOGICAL_READS INT8 NULL,
                PHYSICAL_READS INT8 NULL,
                REDO_SIZE INT8 NULL,
                EXECUTION_COUNT INT8 NULL,
                create_dt timestamp with time zone default current_timestamp,
                create_id varchar(20) default 'system' not NULL
            )
        """,
        # 22.07.13 테이블 컬럼 추가 action
        """ 
            CREATE TABLE AE_SQL_WAIT_10MIN (
                PARTITION_KEY INT4 NULL,
                DB_ID INT2 NULL,
                TIME TIMESTAMP NULL,
                SQL_UID VARCHAR(48) NULL,
                SQL_ID VARCHAR(13) NULL,
                SQL_PLAN_HASH INT8 NULL,
                SCHEMA VARCHAR(128) NULL,
                PROGRAM VARCHAR(128) NULL,
                MODULE VARCHAR(128) NULL,
                ACTION VARCHAR(128) NULL,                    
                MACHINE VARCHAR(128) NULL,
                OS_USER VARCHAR(128) NULL,
                WAIT_TIME INT8 NULL,
                EVENT_VERSION INT2 NULL,
                EVENT_NAME VARCHAR(64) NULL,
                WAIT_CLASS VARCHAR(64) NULL,
                create_dt timestamp with time zone default current_timestamp,
                create_id varchar(20) default 'system' not NULL
            )
        """,
        # 22.07.13 AE_SESSION_INFO 테이블 추가
        """
             CREATE TABLE AE_SESSION_INFO (
                    partition_key int4 NULL,
                    db_id int2 NULL,
                    sid int8 NULL,
                    logon_time timestamp NULL,
                    serial int8 NULL,
                    "time" timestamp NULL,
                    spid varchar(12) NULL,
                    audsid int8 NULL,
                    schema varchar(128) NULL,
                    os_user varchar(128) NULL,
                    machine varchar(128) NULL,
                    terminal varchar(128) NULL,
                    cpid varchar(24) NULL,
                    program varchar(128) NULL,
                    session_type int8 NULL,
                    create_dt timestamp with time zone default current_timestamp,
                    create_id varchar(20) default 'system' not NULL
                )            
        """
    )

    CHECK_SQL = (
        """
            select n.nspname as schema_name,
                   c.relname as table_name
              from pg_class as c join pg_catalog.pg_namespace as n 
                                   on n.oid = c.relnamespace 
             where c.relname similar to '(ae_db|ae_sql|ae_session)%'
            order by table_name
        """
    )

    SELECT_APM_DB_INFO = (
        "select * from APM_DB_INFO"
    )


class SaInitializeQuery:

    ###############################################################
    # Sa Table Lists
    ###############################################################
    # AE_SQL_TEXT
    # AE_TXN_SQL_SUMMARY
    #
    ###############################################################
    # Sa Sequence
    ###############################################################
    # seq_execute_log_id
    #
    ###############################################################

    DDL_SQL = (
        # 22.07.22  AE_SQL_TEXT 테이블 신규 추가
        """ 
            CREATE TABLE public.ae_sql_text (
                was_sql_id varchar(40) NULL,	
                db_sql_uid varchar(40) null,
                sql_text_100 varchar(100) null,
                state_code varchar(100) null,
                create_dt timestamp with time zone default current_timestamp,
                create_id varchar(20) default 'system' not NULL
            )
        """,
        # 22.07.28 ae_txn_sql_summary 테이블 신규 추가
        """
            CREATE TABLE AE_TXN_SQL_SUMMARY (
                ten_min_time timestamp NULL,
                was_name varchar(128) NULL,
                txn_name varchar(256) NULL,
                txn_exec_count int4 NULL,
                exception_sum int4 NULL,
                txn_elapse_sum int4 NULL,
                txn_elapse_avg int4 NULL,
                txn_elapse_max int4 NULL,
                txn_cpu_time_sum int8 NULL,
                txn_cpu_time_avg int8 NULL,
                txn_cpu_time_max int8 NULL,
                thread_memory_avg int8 NULL,
                thread_memory_max int8 NULL,
                fetched_rows_sum int4 NULL,
                fetched_rows_avg int4 NULL,
                fetched_rows_max int4 NULL,
                fetch_time_sum int4 NULL,
                fetch_time_avg int4 NULL,
                fetch_time_max int4 NULL,
                jdbc_fetch_count_sum int4 NULL,
                jdbc_fetch_count_avg int4 NULL,
                jdbc_fetch_count_max int4 NULL ,
                remote_count_sum int4 NULL,
                remote_count_avg int4 NULL,
                remote_count_max int4 NULL,
                remote_elapse_sum int4 NULL,
                remote_elapse_avg int4 NULL,
                remote_elapse_max int4 NULL,
                instance_name varchar(64) NULL,
                was_db_id int2 null, 
                db_type varchar(16) NULL,
                sid int4 NULL,                   
                was_sql_id varchar(40) NULL,
                sql_exec_count_sum int4 NULL,
                sql_elapse_sum int4 NULL,
                sql_elapse_avg int4 NULL,
                sql_elapse_max int4 NULL,
                create_dt timestamp with time zone default current_timestamp,
                create_id varchar(20) default 'system' not NULL                                
            )
        """,
        """
        CREATE SEQUENCE seq_execute_log_id
        INCREMENT 1
        START 1
        MINVALUE 1
        MAXVALUE 9223372036854775807
        CACHE 1;        
        """,
        """
        CREATE TABLE AE_EXECUTE_LOG (
            seq bigint not null,
            execute_name varchar(20) not null,
            execute_start_dt varchar(20) not null,
            execute_end_dt varchar(20) not null,
            execute_elapsed_time integer not null,
            execute_args varchar(100) null,
            result varchar(1) not null,
            result_msg varchar(100) not null,
            create_dt timestamp with time zone default current_timestamp,
            create_id varchar(20) default 'system' not null
        )
        """
    )

    DELETE_TABLE_DEFAULT_QUERY = (
        "delete from #(table_name)"
    )
