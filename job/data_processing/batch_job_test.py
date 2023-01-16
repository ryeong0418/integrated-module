#%%
import psycopg2 as db #postgreDB 연동 모듈 pip install psycopg2
import pandas as pd   #데이터 활용 모듈 pip install pandas
import xml.etree.ElementTree as ET #xml 관리 모듈
from datetime import datetime, timedelta
import log # 별도로 만든 log.py 클래스를 사용하기 위해 import
import traceback # Exception Stack Trace 내용을 출력하기 위한 모듈
from io import StringIO # DataFrame Info 정보를 출력하기 위해 사용한 모듈 
from sqlalchemy import create_engine # ORM 모듈 pip install sqlalchemy  
#%%
try:
    #log 파일을 생성하기 위한 logger 인스턴스 생성
    logger = log.get_logger("log1")         
    logger.info("##################################################")
    
    #DB Connection Information            
    #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
    db_info = ET.parse('..\\config\\db_info.xml')
    
    # local Repository DB Information
    root = db_info.getroot()        
    import_db = root.findall("import_db")

    im_dbname = [x.findtext("sid") for x in import_db]
    im_host = [x.findtext("conn_ip") for x in import_db]
    im_port = [x.findtext("conn_port") for x in import_db]
    im_user = [x.findtext("user") for x in import_db]
    im_password = [x.findtext("password") for x in import_db]
    
    conn_string="""dbname="""+im_dbname[0]+""" host="""+im_host[0]+""" port="""+im_port[0]+""" user="""+im_user[0]+""" password="""+im_password[0]
    logger.info("DB 접속 정보 : "+ conn_string)
    
    #WAS row Data Read 후 dataframe에 적재
    conn=db.connect(conn_string)          
    cur=conn.cursor()      
    logger.info("DB 접속 완료")                   
    

    commands = (
        """
            CREATE temp TABLE ae_txn_detail_summary_temp AS
                select date_trunc('hour', time) + (((date_part('minute', time)::integer / 10::integer) * 10::integer) || ' minutes')::interval as ten_min_time			   
                        ,atn.txn_name as txn_name
                        ,count(atd.tid) as txn_exec_count
                        ,ROUND(sum(atd.txn_elapse / 1000.0), 3) as txn_elapse_sum
                        ,ROUND(avg(atd.txn_elapse / 1000.0), 3) as txn_elapse_avg
                        ,ROUND(max(atd.txn_elapse / 1000.0), 3) as txn_elapse_max
                        ,ROUND(sum(atd.txn_cpu_time / 1000.0), 3) as txn_cpu_time_sum
                        ,ROUND(avg(atd.txn_cpu_time / 1000.0), 3) as txn_cpu_time_avg
                        ,ROUND(max(atd.txn_cpu_time / 1000.0), 3) as txn_cpu_time_max
                        ,ROUND(avg(atd.thread_memory / 1024.0), 3) as thread_memory_avg
                        ,ROUND(max(atd.thread_memory / 1024.0), 3) as thread_memory_max
                        --,sum(atd.sql_exec_count) as sql_exec_count_sum
                        --,ROUND(avg(atd.sql_exec_count),1) as sql_exec_count_avg
                        --,max(atd.sql_exec_count) as sql_exec_count_max
                        --,ROUND(sum(atd.sql_elapse / 1000.0), 3) as sql_elapse_sum
                        --,ROUND(avg(atd.sql_elapse / 1000.0), 3) as sql_elapse_avg
                        --,ROUND(max(atd.sql_elapse_max / 1000.0), 3) as sql_elapse_max
                        ,sum(atd.fetched_row) as fetched_row_sum
                        ,ROUND(avg(atd.fetched_row),1) as fetched_row_avg
                        ,max(atd.fetched_row) as fetched_row_max
                        ,ROUND(sum(atd.fetch_time / 1000.0), 3) as fetch_time_sum
                        ,ROUND(avg(atd.fetch_time / 1000.0), 3) as fetch_time_avg
                        ,ROUND(max(atd.fetch_time / 1000.0), 3) as fetch_time_max
                        ,sum(atd.exception) as exception_sum
                        ,sum(atd.remote_count) as remote_count_sum
                        ,ROUND(avg(atd.remote_count),1) as remote_count_avg
                        ,max(atd.remote_count) as remote_count_max
                        ,ROUND(sum(atd.remote_elapse / 1000.0), 3) as remote_elapse_sum
                        ,ROUND(avg(atd.remote_elapse / 1000.0), 3) as remote_elapse_avg
                        ,ROUND(max(atd.remote_elapse / 1000.0), 3) as remote_elapse_max
                from ae_txn_detail atd ,
                        ae_txn_name atn
                where 1 = 1
                and atn.txn_id=atd.txn_id
                and atd.time >= '2022-07-17 00:00:00'::timestamp
                and atd.time < '2022-07-18 00:00:00'::timestamp
                group by ten_min_time, 
                            txn_name                   
        """,
        """                     
            CREATE temp TABLE ae_txn_sql_detail_summary_temp as
                    SELECT date_trunc( 'hour' , TIME ) +((( date_part( 'minute' , TIME ) ::integer /10::integer ) * 10::integer ) || ' minutes' ) ::interval AS ten_min_time,
                            txn_name,
                            sql_text as was_sql_text,
                            SUM( atsd.execute_count ) AS sql_exec_count_sum ,
                            ROUND( SUM( atsd.elapsed_time /1000.0 ) , 3 ) AS sql_elapse_sum ,
                            ROUND( AVG( atsd.elapsed_time /1000.0 ) , 3 ) AS sql_elapse_avg ,
                            ROUND( MAX( atsd.elapsed_time_max /1000.0 ) , 3 ) AS sql_elapse_max ,
                            aei.instance_name ,
                            aei.db_type ,
                            atsd.sid 
                        FROM ae_txn_sql_detail atsd ,
                                ae_was_db_info aei,
                                ae_txn_name atn,
                                ae_was_sql_text st
                        WHERE  1=1
                        and atsd.db_id = aei.db_id
                        and atn.txn_id=atsd.txn_id
                        and st.sql_id=atsd.sql_id
                        and atsd.time >= '2022-07-17 00:00:00'::timestamp
                        and atsd.time < '2022-07-18 00:00:00'::timestamp
                        GROUP BY ten_min_time ,
                                txn_name ,
                                aei.instance_name ,
                                aei.db_type ,
                                atsd.sid, 
                                was_sql_text             
        """
    )            

    for command in commands:
        logger.info("####WAS Temp Table Select JOb####")

        start_tm = datetime.now() # Query 시작 시간 추출하기            
        logger.info("Temp Select 시작시간 : "+ str(start_tm))

        cur.execute(command)

        end_tm = datetime.now() # Query 종료 시간 추출하기
        logger.info("Select 종료시간 : "+ str(end_tm))            
        logger.info("Select 수행시간 : "+ str(end_tm - start_tm)+"\n") 

    query="""
        select tt.ten_min_time as time_key
                ,tt.txn_name
                ,tt.txn_exec_count
                ,tt.exception_sum
                ,tt.txn_elapse_sum
                ,tt.txn_elapse_avg
                ,tt.txn_elapse_max
                ,tt.txn_cpu_time_sum
                ,tt.txn_cpu_time_avg
                ,tt.txn_cpu_time_max
                ,tt.thread_memory_avg
                ,tt.thread_memory_max
                --,tt.sum_sql_exec_count as sum_sql_exec_count_sm 
                --,tt.avg_sql_exec_count
                --,tt.max_sql_exec_count
                --,tt.sum_sql_elapse as sum_sql_elapse_sm    
                --,tt.avg_sql_elapse as avg_sql_elapse_sm     
                --,tt.max_sql_elapse as max_sql_elapse_sm       	    
                ,st.instance_name
                ,st.db_type
                ,st.sid
                ,st.was_sql_text
                ,st.sql_exec_count_sum
                ,st.sql_elapse_sum
                ,st.sql_elapse_avg
                ,st.sql_elapse_max
                ,tt.fetched_row_sum
                ,tt.fetched_row_avg
                ,tt.fetched_row_max
                ,tt.fetch_time_sum
                ,tt.fetch_time_avg
                ,tt.fetch_time_max
                ,tt.remote_count_sum
                ,tt.remote_count_avg
                ,tt.remote_count_max
                ,tt.remote_elapse_sum
                ,tt.remote_elapse_avg
                ,tt.remote_elapse_max
            from ae_txn_detail_summary_temp	tt 
            left outer join ae_txn_sql_detail_summary_temp st
            on tt.txn_name = st.txn_name
            and tt.ten_min_time = st.ten_min_time
            where tt.ten_min_time >= '2022-07-17 00:00:00'::timestamp
            and tt.ten_min_time < '2022-07-18 00:00:00'::timestamp
            order by tt.ten_min_time asc
    """  

    start_tm = datetime.now() # Query 시작 시간 추출하기 
    logger.info("Select 시작시간 : "+ str(start_tm))
    
    # Export Data
    cur.execute(query) 
    rs_list = cur.fetchall()                      
    inter_df = pd.DataFrame(rs_list)
    inter_df.columns = [i_desc[0] for i_desc in cur.description] # Columns
    #inter_df = pd.read_sql(query, conn)
    #inter_df = df.astype({'sql_uid':'category'} | {'sql_id':'category'})
    
    end_tm = datetime.now() # Query 종료 시간 추출하기

    logger.info("Select 종료시간 : "+ str(end_tm))            
    logger.info("Select 수행시간 : "+ str(end_tm - start_tm)+"\n") 

    cur.close()
    conn.close()

    #DB row Data Read 후 dataframe에 적재
    conn1=db.connect(conn_string)          
    cur1=conn1.cursor()      
    logger.info("DB 접속 완료")                   
    
    command = (
        """
            create temp table session_main
            as
            select  si.partition_key,
                    si.db_id,
                    si.sid,
                    si.serial,
                    si.machine,
                    si.program,
                    st.module,
                    si.schema,
                    si.os_user,
                    st.txn_name,
                    --st.tid,
                    st.sql_uid,
                    st.sql_id,
                    count(distinct st.sql_plan_hash) as sql_plan_cnt,
                    min(st.time) as start_time,
                    max(st.time) as end_time,
                    to_timestamp(substr(to_char(min(st.time),'yyyymmddhh24mi'),1,11)||'0','yyyymmddhh24mi') as sum_start_time,
                    to_timestamp(substr(to_char(max(st.time),'yyyymmddhh24mi'),1,11)||'0','yyyymmddhh24mi') as sum_end_time
            from ae_session_info si,
                    ae_session_stat st
            where 1 = 1
            -------------- join condition --------------
            and si.partition_key = st.partition_key
            and si.db_id = st.db_id
            and si.sid = st.sid
            and si.serial = st.serial
            -------------- predicate --------------
            and si.partition_key = 220717002
            and si.db_id = 2
            --and si.session_type not in ('SYS')
            and st.sql_id is not null
            and (si.program = 'JDBC Thin Client' or st.txn_name is not null) -- 제품 연계 기능의 버그로 값이 설정되지 않는 경우에 대비하는 조건 추가 필요. ***
            group by si.partition_key,
                    si.db_id,
                    si.sid,
                    si.serial,
                    si.machine,
                    si.program,
                    st.module,
                    si.schema,
                    si.os_user,
                    st.txn_name,
                    --st.tid,
                    st.sql_uid,
                    st.sql_id
            order by st.txn_name, start_time                   
        """                
    )

    logger.info("####DB Temp Table Select JOB ####")
    start_tm = datetime.now() # Query 시작 시간 추출하기            
    logger.info("Select 시작시간 : "+ str(start_tm))         
    
    cur1.execute(command)
    
    query="""
        SELECT  STAT.* ,
                ST.SQL_TEXT as db_sql_text
            FROM   (
                    SELECT S.PARTITION_KEY ,
                            S.DB_ID ,
                            S.TXN_NAME ,
                            S.SID ,
                            S.SERIAL ,
                            S.MACHINE ,
                            S.PROGRAM ,
                            S.MODULE ,
                            S.SCHEMA ,
                            S.OS_USER ,
                            S.SQL_UID ,
                            S.SQL_ID ,
                            S.SQL_PLAN_CNT ,
                            S.START_TIME ,
                            S.END_TIME ,
                            S.time_key ,
                            S.EXECUTION_COUNT ,
                            S.LOGICAL_READS ,
                            S.PHYSICAL_READS ,
                            S.REDO_SIZE ,
                            S.ELAPSED_TIME ,
                            S.CPU_TIME ,
                            S.TOTAL_WAIT_TIME ,
                            STRING_AGG( S.WAIT_TIME::VARCHAR , ',' ) AS WAIT_TIME ,
                            STRING_AGG( S.EVENT_NAME , ',' ) AS EVENT_NAME
                    FROM   (
                            SELECT SS.* ,
                                    SW.WAIT_TIME ,
                                    SW.EVENT_NAME
                            FROM   (
                                    SELECT SM.PARTITION_KEY ,
                                            SM.DB_ID ,
                                            SM.TXN_NAME ,
                                            SM.SID ,
                                            SM.SERIAL ,
                                            SM.MACHINE ,
                                            SM.PROGRAM ,
                                            SM.MODULE , 
                                            SM.SCHEMA ,
                                            SM.OS_USER ,
                                            SM.SQL_UID ,
                                            SM.SQL_ID ,
                                            SM.SQL_PLAN_CNT ,
                                            SM.START_TIME ,
                                            SM.END_TIME ,
                                            SS.TIME AS time_key ,
                                            SS.EXECUTION_COUNT ,
                                            SS.LOGICAL_READS ,
                                            SS.PHYSICAL_READS ,
                                            SS.REDO_SIZE ,
                                            SS.ELAPSED_TIME ,
                                            SS.CPU_TIME ,
                                            SS.WAIT_TIME AS TOTAL_WAIT_TIME 					
                                    FROM SESSION_MAIN SM ,
                                            AE_SQL_STAT_10MIN SS
                                    WHERE 1 = 1 
                                        -------------- JOIN CONDITION --------------
                                    AND SM.PARTITION_KEY = SS.PARTITION_KEY
                                    AND SM.DB_ID = SS.DB_ID
                                    AND SM.PROGRAM = SS.PROGRAM
                                    AND COALESCE( SM.MODULE , 'NONE' ) = COALESCE( SS.MODULE , 'NONE' )
                                    AND SM.MACHINE = SS.MACHINE
                                    AND SM.SCHEMA = SS.SCHEMA
                                    AND SM.OS_USER = SS.OS_USER
                                    AND SM.SQL_UID = SS.SQL_UID
                                    AND SM.PARTITION_KEY = 220717002
                                    AND SM.DB_ID = 2
                                    AND TO_CHAR( SS.TIME , 'YYYY-MM-DD HH24:MI:SS' ) >= TO_CHAR( SM.SUM_START_TIME , 'YYYY-MM-DD HH24:MI:SS' )
                                    AND TO_CHAR( SS.TIME , 'YYYY-MM-DD HH24:MI:SS' ) <= TO_CHAR( SM.SUM_END_TIME , 'YYYY-MM-DD HH24:MI:SS' )
                                    --and SID=385
                                    ) SS LEFT JOIN (SELECT *
                                                    FROM (
                                                            SELECT PARTITION_KEY ,
                                                                    DB_ID ,
                                                                    TIME ,
                                                                    SQL_UID ,
                                                                    SQL_ID ,
                                                                    SCHEMA  ,
                                                                    SQL_PLAN_HASH ,
                                                                    PROGRAM ,
                                                                    MODULE ,
                                                                    MACHINE ,
                                                                    OS_USER ,
                                                                    EVENT_NAME ,
                                                                    WAIT_TIME ,
                                                                    ROW_NUMBER( ) OVER( PARTITION BY PARTITION_KEY , DB_ID , TIME , SQL_ID , SCHEMA , SQL_PLAN_HASH , PROGRAM , MODULE , MACHINE , OS_USER , EVENT_NAME
                                                                                            ORDER BY WAIT_TIME DESC ) RANK
                                                            FROM AE_SQL_WAIT_10MIN
                                                            WHERE PARTITION_KEY = 220717002
                                                            ) S
                                                    WHERE RANK <= 3 
                                                    ) SW 
                                                ON SS.PARTITION_KEY = SW.PARTITION_KEY
                                            AND SS.DB_ID = SW.DB_ID
                                            AND SS.PROGRAM = SW.PROGRAM
                                            AND COALESCE( SS.MODULE , 'NONE' ) = COALESCE( SW.MODULE , 'NONE' )
                                            AND SS.MACHINE = SW.MACHINE
                                            AND SS.SCHEMA = SW.SCHEMA
                                            AND SS.OS_USER = SW.OS_USER
                                            AND SS.SQL_UID = SW.SQL_UID
                                            AND SS.time_key = SW.TIME
                            ) S
                    GROUP  BY S.PARTITION_KEY ,
                                S.DB_ID ,
                                S.TXN_NAME ,
                                S.SID ,
                                S.SERIAL ,
                                S.MACHINE ,
                                S.PROGRAM ,
                                S.MODULE ,
                                S.SCHEMA ,
                                S.OS_USER ,
                                S.SQL_UID ,
                                S.SQL_ID ,
                                S.SQL_PLAN_CNT ,
                                S.START_TIME ,
                                S.END_TIME ,
                                S.time_key ,
                                S.EXECUTION_COUNT ,
                                S.LOGICAL_READS ,
                                S.PHYSICAL_READS ,
                                S.REDO_SIZE ,
                                S.ELAPSED_TIME ,
                                S.CPU_TIME ,
                                S.TOTAL_WAIT_TIME
                    ) STAT LEFT JOIN AE_DB_SQL_TEXT ST 
                        ON STAT.PARTITION_KEY = ST.PARTITION_KEY
                        AND STAT.DB_ID = ST.DB_ID
                        AND STAT.SQL_UID = ST.SQL_UID
                        AND ST.SEQ = 1
            ORDER  BY STAT.TXN_NAME ,
                    STAT.SQL_ID ,
                    STAT.START_TIME ,
                    STAT.time_key
    """  
    # Export Data
    cur1.execute(query) 
    rs_list1 = cur1.fetchall()                      
    max_df = pd.DataFrame(rs_list1)
    max_df.columns = [i_desc[0] for i_desc in cur1.description]
    #inter_df = pd.read_sql(query, conn)
    #inter_df = df.astype({'sql_uid':'category'} | {'sql_id':'category'})

    end_tm = datetime.now() # Query 종료 시간 추출하기
    logger.info("Select 종료시간 : "+ str(end_tm))            
    logger.info("Select 수행시간 : "+ str(end_tm - start_tm)+"\n") 

    cur1.close()
    conn1.close()

    #inter_df.to_excel("..\\..\\..\\export_file\\excel\\was_excel_"+prtitionDate+".xlsx")
    #max_df.to_excel("..\\..\\..\\export_file\\excel\\db_excel_"+prtitionDate+".xlsx")

    # Merge Job 이후 Export Excel            
    df_result = pd.merge(inter_df,max_df, how='left', on=["time_key","txn_name","sid"] )
    
    #df_result.to_excel("..\\..\\..\\export_file\\excel\\text_excel_"+prtitionDate+".xlsx")
    #df_result.to_csv("..\\..\\..\\export_file\\csv\\text_csv.csv")

    start_tm = datetime.now() # Query 시작 시간 추출하기            
    logger.info("Insert 시작시간 : "+ str(start_tm))   

    engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
    logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

    # Import Data
    df_result.to_sql(name = 'ae_was_db_summary',
                con = engine,
                schema = 'public',
                if_exists = 'append',
                index = False#,
                # chunksize=10000,
                # method='multi'
                )

    engine.dispose()

    buf = StringIO()
    df_result.info(buf=buf)
    logger.info("적재 데이터 Info 정보")
    logger.info(type(df_result))
    logger.info(buf.getvalue())

    end_tm = datetime.now() # Query 종료 시간 추출하기
    logger.info("Insert 종료시간 : "+ str(end_tm))    
    logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")     

    logger.info("##################################################\n")
except:
    logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())