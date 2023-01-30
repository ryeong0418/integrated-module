
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

    SQL = (
        """ 
            CREATE TABLE AE_TXN_NAME (
                txn_id varchar(40) NULL,
                txn_name varchar(256) NULL,
                business_id int4 NULL,
                business_name varchar(256) NULL,
                modified_time timestamp NULL
            )
        """,
        """ 
            CREATE TABLE AE_WAS_INFO (
                was_id int4 NULL,
                was_name varchar(128) NULL,
                host_name varchar(64) NULL
            )
        """,
        # 22.07.13 테이블 명칭 변경 AE_APM_SQL_TEXT -> AE_WAS_SQL_TEXT
        """ 
            CREATE TABLE AE_WAS_SQL_TEXT (
                sql_id varchar(40) NULL,
                sql_text_100 varchar(100) NULL,
                sql_text text NULL                
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
                lsnr_port int8 NULL
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
                remote_elapse int4 NULL
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
                    cursor_id int8 NULL
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
                    jdbc_fetch_count int4 NULL                        
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


class MaxGauseInitializeQuery:

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

    SQL = (
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
                BATCH_JOB_YN VARCHAR(1)  
            )
        """,
        # 22.07.13 테이블 명칭 변경 AE_SQL_LIST -> AE_DB_SQL_TEXT
        """ 
            CREATE TABLE AE_DB_SQL_TEXT (
                PARTITION_KEY INT4 NULL,
                DB_ID INT2 NULL,
                SQL_UID VARCHAR(48) NULL,	    
                SEQ INT2 NULL,
                SQL_TEXT VARCHAR(4000) NULL
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
                TID INT8 NULL
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
                EXECUTION_COUNT INT8 NULL
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
                WAIT_CLASS VARCHAR(64) NULL
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
                    session_type int8 NULL
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


class SaInitializeQuery:

    ###############################################################
    # Sa Table Lists
    ###############################################################
    # AE_SQL_TEXT
    # AE_TXN_SQL_SUMMARY
    #
    ###############################################################
    # Sa Function Lists
    ###############################################################
    # sql_full(p_dbid integer, p_parti bigint, p_sql_uid character varying)
    #
    ###############################################################

    SQL = (
        # 22.07.22  AE_SQL_TEXT 테이블 신규 추가
        """ 
            CREATE TABLE public.ae_sql_text (
                was_sql_id varchar(40) NULL,	
                db_sql_uid varchar(40) null,
                sql_text_100 varchar(100) null,
                state_code varchar(100) null
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
                sql_elapse_max int4 NULL                
            )
        """,
        # 22.07.27  sql_full 펑션 추가
        """ 
            CREATE OR REPLACE FUNCTION sql_full(p_dbid integer, p_parti bigint, p_sql_uid character varying)
            RETURNS text AS
            $BODY$
                declare 
                rTab1 record;
                rText text;
                v_parti bigint;
                v_sql_uid character varying(48);
                v_sql_text varchar(4000);

                begin
                rText :=' ';
                for rTab1 in
                select substr(replace( x.sql_text, chr(12), chr(10) ), 1, length( x.sql_text ))  col1 
                from ae_db_sql_text x
                where x.db_id = $1
                and x.partition_key = $2
                and x.sql_uid = $3
                order by x.seq		
                loop
                rText :=rText||rTab1.col1||'';
                end loop;
                return rText;
                end;
            $BODY$
            LANGUAGE plpgsql VOLATILE
            COST 100;
            ALTER FUNCTION sql_full(integer, bigint, character varying)
            OWNER TO postgres;
        """
    )
