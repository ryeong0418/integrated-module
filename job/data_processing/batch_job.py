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
class processBatch:
    #%%수동 Export 시 날짜를 입력받아서 처리 
    def was_procs_batch(self):
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
            
            #파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()            
            date_condition = now_day+timedelta(days=-1)

            prtitionDate = date_condition.strftime('%y%m%d')
            
            startDate_re = date_condition
            startDate = startDate_re.strftime('%Y-%m-%d 00:00:00')
            endDate = now_day.strftime('%Y-%m-%d 00:00:00')
             
            logger.info("현재 날짜 : "+ str(date_condition))
            logger.info("파티션카 날짜 : "+ prtitionDate)
            logger.info("조회 시작 날짜 : "+ startDate)
            logger.info("조회 종료 날짜 : "+ endDate)

            commands = (
                """                    
                    create table ae_txn_detail_summary_temp as
                    select date_trunc( 'hour' , time ) +((( date_part( 'minute' , time ) ::integer /10::integer ) * 10::integer ) || ' minutes' ) ::interval as ten_min_time ,
                            awi.was_name as was_name ,
                            atn.txn_name as txn_name ,
                            count( atd.tid ) as txn_exec_count ,
                            sum( atd.txn_elapse ) as txn_elapse_sum ,
                            avg( atd.txn_elapse ) as txn_elapse_avg ,
                            max( atd.txn_elapse ) as txn_elapse_max ,
                            sum( atd.txn_cpu_time ) as txn_cpu_time_sum ,
                            avg( atd.txn_cpu_time ) as txn_cpu_time_avg ,
                            max( atd.txn_cpu_time ) as txn_cpu_time_max ,
                            avg( atd.thread_memory ) as thread_memory_avg ,
                            max( atd.thread_memory ) as thread_memory_max ,
                            sum( atd.fetched_rows ) as fetched_rows_sum ,
                            avg( atd.fetched_rows ) as fetched_rows_avg ,
                            max( atd.fetched_rows ) as fetched_rows_max ,
                            sum( atd.fetch_time ) as fetch_time_sum ,
                            avg( atd.fetch_time ) as fetch_time_avg ,
                            max( atd.fetch_time ) as fetch_time_max ,
                            sum(atd.jdbc_fetch_count) as jdbc_fetch_count_sum,
                            avg(atd.jdbc_fetch_count) as jdbc_fetch_count_avg,
                            max(atd.jdbc_fetch_count) as jdbc_fetch_count_max,
                            sum( atd.exception ) as exception_sum ,
                            sum( atd.remote_count ) as remote_count_sum ,
                            avg( atd.remote_count ) as remote_count_avg ,
                            max( atd.remote_count ) as remote_count_max ,
                            sum( atd.remote_elapse ) as remote_elapse_sum ,
                            avg( atd.remote_elapse ) as remote_elapse_avg ,
                            max( atd.remote_elapse ) as remote_elapse_max
                    from   ae_txn_detail atd ,
                        ae_txn_name atn ,
                        ae_was_info awi
                    where  1 = 1
                    and    atn.txn_id = atd.txn_id
                    and    atd.was_id = awi.was_id
                    and atd.time >= '"""+startDate+"""'::timestamp
                    and atd.time < '"""+endDate+"""'::timestamp
                    group  by ten_min_time ,
                                was_name ,
                                txn_name                 
                """,
                """     
                    create table ae_txn_sql_detail_summary_temp as
                    select date_trunc( 'hour' , time ) +((( date_part( 'minute' , time ) ::integer /10::integer ) * 10::integer ) || ' minutes' ) ::interval as ten_min_time ,
                        awi.was_name as was_name ,
                        atn.txn_name as txn_name ,
                        atsd.sql_id as was_sql_id ,
                        sum( atsd.execute_count ) as sql_exec_count_sum ,
                        sum( atsd.elapsed_time ) as sql_elapse_sum ,
                        avg( atsd.elapsed_time ) as sql_elapse_avg ,
                        max( atsd.elapsed_time_max ) as sql_elapse_max ,
                        aei.instance_name ,
                        atsd.db_id ,
                        aei.db_type ,
                        atsd.sid
                    from   ae_txn_sql_detail atsd ,
                        ae_was_db_info aei ,
                        ae_txn_name atn ,
                        ae_was_info awi
                    where  1=1
                    and    atsd.db_id = aei.db_id
                    and    atn.txn_id=atsd.txn_id
                    and    atsd.was_id = awi.was_id
                    AND atsd.time >= '"""+startDate+"""'::timestamp
                    AND atsd.time < '"""+endDate+"""'::timestamp
                    group  by ten_min_time ,
                        was_name ,
                        txn_name ,
                        aei.instance_name ,
                        aei.db_type ,
                        atsd.db_id ,
                        atsd.sid ,
                        atsd.sql_id 
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
                select tt.ten_min_time ,
                        tt.was_name ,
                        tt.txn_name ,
                        tt.txn_exec_count ,
                        tt.exception_sum ,
                        tt.txn_elapse_sum ,
                        tt.txn_elapse_avg ,
                        tt.txn_elapse_max ,
                        tt.txn_cpu_time_sum ,
                        tt.txn_cpu_time_avg ,
                        tt.txn_cpu_time_max ,
                        tt.thread_memory_avg ,
                        tt.thread_memory_max ,
                        tt.fetched_rows_sum ,
                        tt.fetched_rows_avg ,
                        tt.fetched_rows_max ,
                        tt.fetch_time_sum ,
                        tt.fetch_time_avg ,
                        tt.fetch_time_max ,
                        tt.remote_count_sum ,
                        tt.remote_count_avg ,
                        tt.remote_count_max ,
                        tt.remote_elapse_sum ,
                        tt.remote_elapse_avg ,
                        tt.remote_elapse_max,	   
                        st.instance_name ,
                        st.db_id as was_db_id,
                        st.db_type ,
                        st.sid ,
                        st.was_sql_id ,
                        st.sql_exec_count_sum ,
                        st.sql_elapse_sum ,
                        st.sql_elapse_avg ,
                        st.sql_elapse_max       
                    from   ae_txn_detail_summary_temp tt left outer join ae_txn_sql_detail_summary_temp st
                                                                    on tt.was_name = st.was_name
                                                                    and tt.txn_name = st.txn_name
                                                                    and tt.ten_min_time = st.ten_min_time
                    where tt.ten_min_time >= '"""+startDate+"""'::timestamp
                    and tt.ten_min_time < '"""+endDate+"""'::timestamp
                    order  by tt.ten_min_time asc        
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

            engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
            logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

            # Import Data
            inter_df.to_sql(name = 'ae_txn_sql_summary',
                        con = engine,
                        schema = 'public',
                        if_exists = 'append',
                        index = False#,
                        # chunksize=10000,
                        # method='multi'
                        )

            engine.dispose()

            buf = StringIO()
            inter_df.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(inter_df))
            logger.info(buf.getvalue())

            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("Insert 종료시간 : "+ str(end_tm))    
            logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
            logger.info(prtitionDate+" Import 데이터 ae_was_db_summary Insert 완료")       

            logger.info("##################################################\n")
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())

    # def db_procs_batch(self):
    #     try:
    #         #log 파일을 생성하기 위한 logger 인스턴스 생성
    #         logger = log.get_logger("log2")         
    #         logger.info("##################################################")
            
    #         #DB Connection Information            
    #         #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
    #         db_info = ET.parse('..\\config\\db_info.xml')
            
    #         # local Repository DB Information
    #         root = db_info.getroot()        
    #         import_db = root.findall("import_db")

    #         im_dbname = [x.findtext("sid") for x in import_db]
    #         im_host = [x.findtext("conn_ip") for x in import_db]
    #         im_port = [x.findtext("conn_port") for x in import_db]
    #         im_user = [x.findtext("user") for x in import_db]
    #         im_password = [x.findtext("password") for x in import_db]
            
    #         conn_string="""dbname="""+im_dbname[0]+""" host="""+im_host[0]+""" port="""+im_port[0]+""" user="""+im_user[0]+""" password="""+im_password[0]
    #         logger.info("DB 접속 정보 : "+ conn_string)
            
    #         #WAS row Data Read 후 dataframe에 적재
    #         conn=db.connect(conn_string)          
    #         cur=conn.cursor()      
    #         logger.info("DB 접속 완료")                   
            
    #         #파티션 날짜 정보 : 배치가 수행되는 날 -1
    #         now_day = datetime.now()            
    #         date_condition = now_day+timedelta(days=-1)

    #         prtitionDate = date_condition.strftime('%y%m%d')
            
    #         startDate_re = date_condition
    #         startDate = startDate_re.strftime('%Y-%m-%d 00:00:00')
    #         endDate = now_day.strftime('%Y-%m-%d 00:00:00')
             
    #         logger.info("현재 날짜 : "+ str(date_condition))
    #         logger.info("파티션카 날짜 : "+ prtitionDate)
    #         logger.info("조회 시작 날짜 : "+ startDate)
    #         logger.info("조회 종료 날짜 : "+ endDate)

    #         commands = (
    #             """                   
    #             """
    #         )            

    #         for command in commands:
    #             logger.info("####DB Temp Table Select JOb####")

    #             start_tm = datetime.now() # Query 시작 시간 추출하기            
    #             logger.info("Temp Select 시작시간 : "+ str(start_tm))

    #             cur.execute(command)

    #             end_tm = datetime.now() # Query 종료 시간 추출하기
    #             logger.info("Select 종료시간 : "+ str(end_tm))            
    #             logger.info("Select 수행시간 : "+ str(end_tm - start_tm)+"\n") 

    #         query="""
               
    #         """  

    #         start_tm = datetime.now() # Query 시작 시간 추출하기 
    #         logger.info("Select 시작시간 : "+ str(start_tm))
       
    #         # Export Data
    #         cur.execute(query) 
    #         rs_list = cur.fetchall()                      
    #         inter_df = pd.DataFrame(rs_list)
    #         inter_df.columns = [i_desc[0] for i_desc in cur.description] # Columns
    #         #inter_df = pd.read_sql(query, conn)
    #         #inter_df = df.astype({'sql_uid':'category'} | {'sql_id':'category'})
            
    #         end_tm = datetime.now() # Query 종료 시간 추출하기

    #         logger.info("Select 종료시간 : "+ str(end_tm))            
    #         logger.info("Select 수행시간 : "+ str(end_tm - start_tm)+"\n") 

    #         cur.close()
    #         conn.close()           

    #         engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
    #         logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

    #         # Import Data
    #         inter_df.to_sql(name = 'ae_sql_db_summary',
    #                     con = engine,
    #                     schema = 'public',
    #                     if_exists = 'append',
    #                     index = False#,
    #                     # chunksize=10000,
    #                     # method='multi'
    #                     )

    #         engine.dispose()

    #         buf = StringIO()
    #         inter_df.info(buf=buf)
    #         logger.info("적재 데이터 Info 정보")
    #         logger.info(type(inter_df))
    #         logger.info(buf.getvalue())

    #         end_tm = datetime.now() # Query 종료 시간 추출하기
    #         logger.info("Insert 종료시간 : "+ str(end_tm))    
    #         logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
    #         logger.info(prtitionDate+" Import 데이터 ae_sql_db_summary Insert 완료")       

    #         logger.info("##################################################\n")
    #     except:
    #         logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())