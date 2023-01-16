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
class wasBatch:
    #%%
    def txn_detail(self):        
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log1")         
            logger.info("##################################################")
            
            #DB Connection Information            
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')
            
            # InterMax Repository DB Information
            root = db_info.getroot()        
            inter_repo = root.findall("intermax_repo")
            
            ex_dbname = [x.findtext("sid") for x in inter_repo]
            ex_host = [x.findtext("conn_ip") for x in inter_repo]
            ex_port = [x.findtext("conn_port") for x in inter_repo]
            ex_user = [x.findtext("user") for x in inter_repo]
            ex_password =  [x.findtext("password") for x in inter_repo]
            
            conn_string="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
            logger.info("DB 접속 정보 : "+ conn_string)
            
            conn=db.connect(conn_string)                
            logger.info("DB 접속 완료")
            
            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')
        
            # txn_detail 상세데이터 export     
            logger.info("export 대상 파티션테이블 날짜 : "+ date_condition)
            
            start_tm = datetime.now() # Query 시작 시간 추출하기            
            logger.info("export 시작시간 : "+ str(start_tm))

#22.08.01 export txn_detail() 메소드 jdbc_fetch_count 컬럼 추가 
            query="""
                   select tid,
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
                     from xapm_txn_detail_p20"""+date_condition+"""  
            """  
            
            # Export Data
            df = pd.read_sql(query, conn)
            #print(df.head())        
            
            df1 = df.astype({'txn_id':'category'})
            
            # 다양한 파일 저장 방식 중 효율이 제일 좋은 parquest 방식으로 저장 
            #df.to_csv(".//txn_detail_csv_"+date_condition+".csv", index=False)
            df1.to_parquet("..\\..\\..\\export_file\\parquet\\txn_detail_parquet_"+date_condition+".parquet", engine='pyarrow', index=False, compression='gzip')
            #df.to_pickle(".//txn_detail_pickle_"+date_condition+".pkl")
            #df.to_feather(".//txn_detail_feather_"+date_condition+".ftr")
               
            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("export 종료시간 : "+ str(end_tm))
            
            logger.info("export 수행시간 : "+ str(end_tm - start_tm)+"\n")    
            
            buf = StringIO()
            df1.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(df1))
            logger.info(buf.getvalue())
            
            logger.info("xapm_txn_detail_p20"+date_condition+" -> txn_detail_parquet_"+date_condition+".parquet 파일 생성")
            
            conn.close()
            logger.info("##################################################\n")
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())
            
    #%%
    def txn_sql_detail(self):        
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log2")         
            logger.info("##################################################")
            
            # DB Connection Information            
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')
            
            # InterMax Repository DB Information
            root = db_info.getroot()        
            inter_repo = root.findall("intermax_repo")
            
            ex_dbname = [x.findtext("sid") for x in inter_repo]
            ex_host = [x.findtext("conn_ip") for x in inter_repo]
            ex_port = [x.findtext("conn_port") for x in inter_repo]
            ex_user = [x.findtext("user") for x in inter_repo]
            ex_password =  [x.findtext("password") for x in inter_repo]
            
            conn_string="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
            logger.info("DB 접속 정보 : "+ conn_string)
            
            conn=db.connect(conn_string)                
            logger.info("DB 접속 완료")
            
            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')
        
            # txn_sql_detail 상세데이터 export
    
            logger.info("export 대상 파티션테이블 날짜 : "+ date_condition)
            
            start_tm = datetime.now() # Query 시작 시간 추출하기
            
            logger.info("export 시작시간 : "+ str(start_tm))

#22.07.15 export txn_sql_detail() 메소드 db_id 컬럼 추가   
#22.08.01 export txn_sql_detail() 메소드 cursor_id 컬럼 추가  
            query="""
                    select tid,
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
                    from xapm_txn_sql_detail_p20"""+date_condition+"""  
            """  
            
            # Export Data
            df = pd.read_sql(query, conn)
            #print(df.head())        
            
            df1 = df.astype({'txn_id':'category'} | {'sql_id':'category'})
            
            # 다양한 파일 저장 방식 중 효율이 제일 좋은 parquest 방식으로 저장 
            #df.to_csv(".//txn_sql_detail_csv_"+date_condition+".csv", index=False)
            df1.to_parquet("..\\..\\..\\export_file\\parquet\\txn_sql_detail_parquet_"+date_condition+".parquet", engine='pyarrow', index=False, compression='gzip')
            #df.to_pickle(".//txn_sql_detail_pickle_"+date_condition+".pkl")
            #df.to_feather(".//txn_sql_detail_feather_"+date_condition+".ftr")
               
            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("export 종료시간 : "+ str(end_tm))
            
            logger.info("export 수행시간 : "+ str(end_tm - start_tm)+"\n")    
            
            buf = StringIO()
            df1.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(df1))
            logger.info(buf.getvalue())
            
            logger.info("xapm_txn_sql_detail_p20"+date_condition+" -> txn_sql_detail_parquet_"+date_condition+".parquet 파일 생성")
            
            conn.close()        
            logger.info("##################################################\n")
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())            
            #%%

#22.08.01 export txn_sql_fetch 메소드 신규 추가            
    def txn_sql_fetch(self):        
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log15")         
            logger.info("##################################################")
            
            #DB Connection Information            
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')
            
            # InterMax Repository DB Information
            root = db_info.getroot()        
            inter_repo = root.findall("intermax_repo")
            
            ex_dbname = [x.findtext("sid") for x in inter_repo]
            ex_host = [x.findtext("conn_ip") for x in inter_repo]
            ex_port = [x.findtext("conn_port") for x in inter_repo]
            ex_user = [x.findtext("user") for x in inter_repo]
            ex_password =  [x.findtext("password") for x in inter_repo]
            
            conn_string="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
            logger.info("DB 접속 정보 : "+ conn_string)
            
            conn=db.connect(conn_string)                
            logger.info("DB 접속 완료")
            
            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')
        
            # txn_sql_fetch 상세데이터 export     
            logger.info("export 대상 파티션테이블 날짜 : "+ date_condition)
            
            start_tm = datetime.now() # Query 시작 시간 추출하기            
            logger.info("export 시작시간 : "+ str(start_tm))

            query="""
                   select tid,
                        time,                        
                        txn_id, 
                        server_id as was_id,      
                        cursor_id,                        
                        fetch_count as fetched_rows,
                        fetch_time,
                        fetch_time_max,
                        internal_fetch_count as jdbc_fetch_count                   
                     from xapm_txn_sql_fetch_p20"""+date_condition+"""  
            """  
            
            # Export Data
            df = pd.read_sql(query, conn)
            #print(df.head())        
            
            df1 = df.astype({'txn_id':'category'})
            
            # 다양한 파일 저장 방식 중 효율이 제일 좋은 parquest 방식으로 저장 
            df1.to_parquet("..\\..\\..\\export_file\\parquet\\txn_sql_fetch_parquet_"+date_condition+".parquet", engine='pyarrow', index=False, compression='gzip')
               
            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("export 종료시간 : "+ str(end_tm))
            
            logger.info("export 수행시간 : "+ str(end_tm - start_tm)+"\n")    
            
            buf = StringIO()
            df1.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(df1))
            logger.info(buf.getvalue())
            
            logger.info("xapm_txn_sql_fetch_p20"+date_condition+" -> txn_sql_fetch_parquet_"+date_condition+".parquet 파일 생성")
            
            conn.close()
            logger.info("##################################################\n")
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())
class dbBatch:
    #%%
#22.07.13 export ora_session_info 메소드 신규 추가
    def ora_session_info(self):        
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log3")         
            logger.info("##################################################")
            
            #DB Connection Information            
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')
            
            # MaxGauge Repository DB Information
            root = db_info.getroot()        
            max_repo = root.findall("maxgauge_repo")
            
            ex_dbname = [x.findtext("sid") for x in max_repo]
            ex_host = [x.findtext("conn_ip") for x in max_repo]
            ex_port = [x.findtext("conn_port") for x in max_repo]
            ex_user = [x.findtext("user") for x in max_repo]
            ex_password =  [x.findtext("password") for x in max_repo]
            
            conn_string="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
            logger.info("DB 접속 정보 : "+ conn_string)
            
            conn=db.connect(conn_string)                
            logger.info("DB 접속 완료")
            
            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')
        
            # ora_session_info 상세데이터 export     
            logger.info("export 대상 파티션테이블 날짜 : "+ date_condition)
            
            start_tm = datetime.now() # Query 시작 시간 추출하기            
            logger.info("export 시작시간 : "+ str(start_tm))

#22.07.18 db_id 조건 제거  
            query="""
                    select  partition_key,
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
                    from ora12c.ora_session_info si
                    where partition_key = """+date_condition+"""002                    
                    --and db_id=2
            """              
            # Export Data
            df = pd.read_sql(query, conn)
            #print(df.head())        
            
            df1 = df.astype({'spid':'category'} | {'schema':'category'} | {'os_user':'category'} | {'machine':'category'} | {'terminal':'category'} | {'cpid':'category'} | {'program':'category'})
            #df1 = df

            # 다양한 파일 저장 방식 중 효율이 제일 좋은 parquest 방식으로 저장 
            #df.to_csv(".//ora_session_info_csv_"+date_condition+".csv", index=False)
            df1.to_parquet("..\\..\\..\\export_file\\parquet\\ora_session_info_parquet_"+date_condition+".parquet", engine='pyarrow', index=False, compression='gzip')
            #df.to_pickle(".//ora_session_info_pickle_"+date_condition+".pkl")
            #df.to_feather(".//ora_session_info_feather_"+date_condition+".ftr")
               
            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("export 종료시간 : "+ str(end_tm))            
            logger.info("export 수행시간 : "+ str(end_tm - start_tm)+"\n")    
            
            buf = StringIO()
            df1.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(df1))
            logger.info(buf.getvalue())
            
            logger.info("ora_session_info_p20"+date_condition+" -> ora_session_info_parquet_"+date_condition+".parquet 파일 생성")
            
            conn.close()
            logger.info("##################################################\n")
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())   
    #%%
    def ora_session_stat(self):        
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log4")         
            logger.info("##################################################")
            
            #DB Connection Information            
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')
            
            # MaxGauge Repository DB Information
            root = db_info.getroot()        
            max_repo = root.findall("maxgauge_repo")
            
            ex_dbname = [x.findtext("sid") for x in max_repo]
            ex_host = [x.findtext("conn_ip") for x in max_repo]
            ex_port = [x.findtext("conn_port") for x in max_repo]
            ex_user = [x.findtext("user") for x in max_repo]
            ex_password =  [x.findtext("password") for x in max_repo]
            
            conn_string="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
            logger.info("DB 접속 정보 : "+ conn_string)
            
            conn=db.connect(conn_string)                
            logger.info("DB 접속 완료")
            
            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')
        
            # ora_session_stat 상세데이터 export     
            logger.info("export 대상 파티션테이블 날짜 : "+ date_condition)
            
            start_tm = datetime.now() # Query 시작 시간 추출하기            
            logger.info("export 시작시간 : "+ str(start_tm))

#22.07.13 SELECT 쿼리 수정 sid, serial, logon_time, module, action, was_id, tan_name 컬럼 추가 및 변경
#22.07.18 db_id 조건 제거
            query="""
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
                    from ora12c.ora_session_stat
                    where partition_key = """+date_condition+"""002
                    --and db_id=2
                    and sql_id is not null
            """  
            
            # Export Data
            df = pd.read_sql(query, conn)
            #print(df.head())        
            
            df1 = df.astype({'sql_uid':'category'} | {'sql_id':'category'} | {'client_identifier':'category'} | {'txn_name':'category'})
            #df1 = df

            # 다양한 파일 저장 방식 중 효율이 제일 좋은 parquest 방식으로 저장 
            #df.to_csv(".//ora_session_stat_csv_"+date_condition+".csv", index=False)
            df1.to_parquet("..\\..\\..\\export_file\\parquet\\ora_session_stat_parquet_"+date_condition+".parquet", engine='pyarrow', index=False, compression='gzip')
            #df.to_pickle(".//ora_session_stat_pickle_"+date_condition+".pkl")
            #df.to_feather(".//ora_session_stat_feather_"+date_condition+".ftr")
               
            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("export 종료시간 : "+ str(end_tm))            
            logger.info("export 수행시간 : "+ str(end_tm - start_tm)+"\n")    
            
            buf = StringIO()
            df1.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(df1))
            logger.info(buf.getvalue())
            
            logger.info("ora_session_stat_p20"+date_condition+" -> ora_session_stat_parquet_"+date_condition+".parquet 파일 생성")
            
            conn.close()
            logger.info("##################################################\n")
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())   
    #%%
    def apm_sql_list(self):        
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log5")         
            logger.info("##################################################")
            
            #DB Connection Information            
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')
            
            # MaxGauge Repository DB Information
            root = db_info.getroot()        
            max_repo = root.findall("maxgauge_repo")
            
            ex_dbname = [x.findtext("sid") for x in max_repo]
            ex_host = [x.findtext("conn_ip") for x in max_repo]
            ex_port = [x.findtext("conn_port") for x in max_repo]
            ex_user = [x.findtext("user") for x in max_repo]
            ex_password =  [x.findtext("password") for x in max_repo]
            
            conn_string="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
            logger.info("DB 접속 정보 : "+ conn_string)
            
            conn=db.connect(conn_string)                
            logger.info("DB 접속 완료")
            
            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')
        
            # apm_sql_list 상세데이터 export     
            logger.info("export 대상 파티션테이블 날짜 : "+ date_condition)
            
            start_tm = datetime.now() # Query 시작 시간 추출하기            
            logger.info("export 시작시간 : "+ str(start_tm))

#22.07.18 db_id 조건 제거            
            query="""
             SELECT  PARTITION_KEY
            	    ,DB_ID
        			,SQL_UID
        			,SEQ
        			,SQL_TEXT
        	   FROM ORA12C.APM_SQL_LIST
        	  WHERE PARTITION_KEY = """+date_condition+"""002
        	  --  AND DB_ID=2
            """  
            
            # Export Data
            df = pd.read_sql(query, conn)
            #print(df.head())        
            
            #df1 = df.astype({'SQL_UID':'category'})
            df1 = df

            # 다양한 파일 저장 방식 중 효율이 제일 좋은 parquest 방식으로 저장 
            #df.to_csv(".//apm_sql_list_csv_"+date_condition+".csv", index=False)
            df1.to_parquet("..\\..\\..\\export_file\\parquet\\apm_sql_list_parquet_"+date_condition+".parquet", engine='pyarrow', index=False, compression='gzip')
            #df.to_pickle(".//apm_sql_list_pickle_"+date_condition+".pkl")
            #df.to_feather(".//apm_sql_list_feather_"+date_condition+".ftr")
               
            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("export 종료시간 : "+ str(end_tm))            
            logger.info("export 수행시간 : "+ str(end_tm - start_tm)+"\n")    
            
            buf = StringIO()
            df1.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(df1))
            logger.info(buf.getvalue())
            
            logger.info("apm_sql_list_p20"+date_condition+" -> apm_sql_list_parquet_"+date_condition+".parquet 파일 생성")
            
            conn.close()
            logger.info("##################################################\n")
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc()) 
    #%%
    def ora_sql_stat_10min(self):        
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log6")         
            logger.info("##################################################")
            
            #DB Connection Information            
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')
            
            # MaxGauge Repository DB Information
            root = db_info.getroot()        
            max_repo = root.findall("maxgauge_repo")
            
            ex_dbname = [x.findtext("sid") for x in max_repo]
            ex_host = [x.findtext("conn_ip") for x in max_repo]
            ex_port = [x.findtext("conn_port") for x in max_repo]
            ex_user = [x.findtext("user") for x in max_repo]
            ex_password =  [x.findtext("password") for x in max_repo]
            
            conn_string="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
            logger.info("DB 접속 정보 : "+ conn_string)
            
            conn=db.connect(conn_string)                
            logger.info("DB 접속 완료")
            
            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')
        
            # ora_sql_stat_10min 상세데이터 export     
            logger.info("export 대상 파티션테이블 날짜 : "+ date_condition)
            
            start_tm = datetime.now() # Query 시작 시간 추출하기            
            logger.info("export 시작시간 : "+ str(start_tm))

#22.07.13 SELECT 쿼리 수정 ACTION 컬럼 추가  
#22.07.18 db_id 조건 제거
            query="""
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
                    FROM  ORA12C.ORA_SQL_STAT_10MIN A
                    WHERE  PARTITION_KEY = """+date_condition+"""002
                    --AND  A.DB_ID = 2
            """              
            # Export Data
            df = pd.read_sql(query, conn)
            #print(df.head())        
            
            df1 = df.astype({'sql_uid':'category'} | {'sql_id':'category'} | {'schema':'category'} | {'program':'category'} | {'module':'category'} | {'action':'category'} | {'machine':'category'} | {'os_user':'category'})
            #df1 = df

            # 다양한 파일 저장 방식 중 효율이 제일 좋은 parquest 방식으로 저장 
            #df.to_csv(".//ora_sql_stat_10min_csv_"+date_condition+".csv", index=False)
            df1.to_parquet("..\\..\\..\\export_file\\parquet\\ora_sql_stat_10min_parquet_"+date_condition+".parquet", engine='pyarrow', index=False, compression='gzip')
            #df.to_pickle(".//ora_sql_stat_10min_pickle_"+date_condition+".pkl")
            #df.to_feather(".//ora_sql_stat_10min_feather_"+date_condition+".ftr")
               
            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("export 종료시간 : "+ str(end_tm))            
            logger.info("export 수행시간 : "+ str(end_tm - start_tm)+"\n")    
            
            buf = StringIO()
            df1.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(df1))
            logger.info(buf.getvalue())
            
            logger.info("ora_sql_stat_10min_p20"+date_condition+" -> ora_sql_stat_10min_parquet_"+date_condition+".parquet 파일 생성")
            
            conn.close()
            logger.info("##################################################\n")
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())  
    #%%
    def ora_sql_wait_10min(self):        
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log7")         
            logger.info("##################################################")
            
            #DB Connection Information            
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')
            
            # MaxGauge Repository DB Information
            root = db_info.getroot()        
            max_repo = root.findall("maxgauge_repo")
            
            ex_dbname = [x.findtext("sid") for x in max_repo]
            ex_host = [x.findtext("conn_ip") for x in max_repo]
            ex_port = [x.findtext("conn_port") for x in max_repo]
            ex_user = [x.findtext("user") for x in max_repo]
            ex_password =  [x.findtext("password") for x in max_repo]
            
            conn_string="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
            logger.info("DB 접속 정보 : "+ conn_string)
            
            conn=db.connect(conn_string)                
            logger.info("DB 접속 완료")
            
            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')
        
            # ora_sql_wait_10min 상세데이터 export     
            logger.info("export 대상 파티션테이블 날짜 : "+ date_condition)
            
            start_tm = datetime.now() # Query 시작 시간 추출하기            
            logger.info("export 시작시간 : "+ str(start_tm))

#22.07.13 SELECT 쿼리 수정 ACTION 컬럼 추가            
#22.07.18 db_id 조건 제거
            query="""
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
                        FROM ORA12C.ORA_SQL_WAIT_10MIN A ,
                             ORA_EVENT_NAME B
                        WHERE PARTITION_KEY = """+date_condition+"""002
                        --AND A.DB_ID = 2
                        AND A.DB_ID=B.DB_ID
                        AND A.EVENT_ID=B.EVENT_ID
                        AND A.EVENT_VERSION=B.EVENT_VERSION
            """              
            # Export Data
            df = pd.read_sql(query, conn)
            #print(df.head())        
            
            df1 = df.astype({'sql_uid':'category'} | {'sql_id':'category'} | {'schema':'category'} | {'program':'category'} | {'module':'category'} | {'action':'category'} | {'machine':'category'} | {'os_user':'category'} | {'event_name':'category'} | {'wait_class':'category'})
            #df1 = df

            # 다양한 파일 저장 방식 중 효율이 제일 좋은 parquest 방식으로 저장 
            #df.to_csv(".//ora_sql_wait_10min_csv_"+date_condition+".csv", index=False)
            df1.to_parquet("..\\..\\..\\export_file\\parquet\\ora_sql_wait_10min_parquet_"+date_condition+".parquet", engine='pyarrow', index=False, compression='gzip')
            #df.to_pickle(".//ora_sql_wait_10min_pickle_"+date_condition+".pkl")
            #df.to_feather(".//ora_sql_wait_10min_feather_"+date_condition+".ftr")
               
            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("export 종료시간 : "+ str(end_tm))            
            logger.info("export 수행시간 : "+ str(end_tm - start_tm)+"\n")    
            
            buf = StringIO()
            df1.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(df1))
            logger.info(buf.getvalue())
            
            logger.info("ora_sql_wait_10min_p20"+date_condition+" -> ora_sql_wait_10min_parquet_"+date_condition+".parquet 파일 생성")
            
            conn.close()
            logger.info("##################################################\n")
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())  

class impBatch:
    #%%
    def txn_detail(self):        
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log8")         
            logger.info("##################################################")

            #DB Connection Information   
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')

            root = db_info.getroot()      
            # Import Repository DB Information                 
            import_db = root.findall("import_db")

            im_dbname = [x.findtext("sid") for x in import_db]
            im_host = [x.findtext("conn_ip") for x in import_db]
            im_port = [x.findtext("conn_port") for x in import_db]
            im_user = [x.findtext("user") for x in import_db]
            im_password = [x.findtext("password") for x in import_db]

            engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
            logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

            #전날 export parquet 파일을 로드하여 인서트       
            start_tm = datetime.now() # Query 시작 시간 추출하기 
            logger.info("Insert 시작시간 : "+ str(start_tm))

            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')

            logger.info("load File 일자: "+ str(date_condition))

            #export data 파일을 읽어온다
            #result = pd.read_csv(".//txn_detail_csv_220703.csv") 
            result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\txn_detail_parquet_" +date_condition+ ".parquet", engine='pyarrow')
            #result = pd.read_pickle(".//txn_detail_pickle_220702.pkl") 
            #result = pd.read_feather(".//txn_detail_feather_220703.ftr", use_threads=True) 
            #logger.info("..\\..\\..\\export_file\\parquet\\txn_detail_parquet_" +date_condition+ ".parquet")
            #result = result

            # Import Data
            result.to_sql(name = 'ae_txn_detail',
                        con = engine,
                        schema = 'public',
                        if_exists = 'append',
                        index = False#,
                        # chunksize=10000,
                        # method='multi'
                        )

            engine.dispose()

            buf = StringIO()
            result.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(result))
            logger.info(buf.getvalue())

            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("Insert 종료시간 : "+ str(end_tm))    
            logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
            logger.info("xapm_txn_detail_p20"+date_condition+" Import 데이터 ae_txn_detail Insert 완료")
            logger.info("##################################################\n")         
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc()) 

    def txn_sql_detail(self):
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log9")         
            logger.info("##################################################")

            #DB Connection Information   
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')

            root = db_info.getroot()      
            # Import Repository DB Information                 
            import_db = root.findall("import_db")

            im_dbname = [x.findtext("sid") for x in import_db]
            im_host = [x.findtext("conn_ip") for x in import_db]
            im_port = [x.findtext("conn_port") for x in import_db]
            im_user = [x.findtext("user") for x in import_db]
            im_password = [x.findtext("password") for x in import_db]

            engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
            logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

            #입력받은 기간동안 저장된 parquet 파일을 로드하여 인서트하는 반복문
            start_tm = datetime.now() # Query 시작 시간 추출하기 
            logger.info("Insert 시작시간 : "+ str(start_tm))

            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')

            logger.info("load File 일자: "+ str(date_condition))

            #export data 파일을 읽어온다
            #result = pd.read_csv(".//txn_sql_detail_csv_220703.csv") 
            result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\txn_sql_detail_parquet_" +date_condition+ ".parquet", engine='pyarrow')
            #result = pd.read_pickle(".//txn_sql_detail_pickle_220702.pkl") 
            #result = pd.read_feather(".//txn_sql_detail_feather_220703.ftr", use_threads=True) 
            #logger.info("..\\..\\..\\export_file\\parquet\\txn_sql_detail_parquet_" +date_condition+ ".parquet")
            #result = result
            
            # Import Data
            result.to_sql(name = 'ae_txn_sql_detail',
                        con = engine,
                        schema = 'public',
                        if_exists = 'append',
                        index = False#,
                        # chunksize=10000,
                        # method='multi'
                        )

            engine.dispose()

            buf = StringIO()
            result.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(result))
            logger.info(buf.getvalue())

            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("Insert 종료시간 : "+ str(end_tm))    
            logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
            logger.info("xapm_txn_sql_detail_p20"+date_condition+" Import 데이터 ae_txn_sql_detail Insert 완료")
            logger.info("##################################################\n")              
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())   

#22.08.01 insert txn_sql_fetch 메소드 신규 추가 
    def txn_sql_fetch(self):
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log16")         
            logger.info("##################################################")

            #DB Connection Information   
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')

            root = db_info.getroot()      
            # Import Repository DB Information                 
            import_db = root.findall("import_db")

            im_dbname = [x.findtext("sid") for x in import_db]
            im_host = [x.findtext("conn_ip") for x in import_db]
            im_port = [x.findtext("conn_port") for x in import_db]
            im_user = [x.findtext("user") for x in import_db]
            im_password = [x.findtext("password") for x in import_db]

            engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
            logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

            #입력받은 기간동안 저장된 parquet 파일을 로드하여 인서트하는 반복문
            start_tm = datetime.now() # Query 시작 시간 추출하기 
            logger.info("Insert 시작시간 : "+ str(start_tm))

            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')

            logger.info("load File 일자: "+ str(date_condition))

            #export data 파일을 읽어온다
            result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\txn_sql_fetch_parquet_" +date_condition+ ".parquet", engine='pyarrow')

            # Import Data
            result.to_sql(name = 'ae_txn_sql_fetch',
                        con = engine,
                        schema = 'public',
                        if_exists = 'append',
                        index = False#,
                        # chunksize=10000,
                        # method='multi'
                        )

            engine.dispose()

            buf = StringIO()
            result.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(result))
            logger.info(buf.getvalue())

            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("Insert 종료시간 : "+ str(end_tm))    
            logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
            logger.info("xapm_txn_sql_fetch_p20"+date_condition+" Import 데이터 ae_txn_sql_fetch Insert 완료")
            logger.info("##################################################\n")              
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())   
#22.07.13 insert ora_session_info 메소드 신규 추가
    def ora_session_info(self):
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log10")         
            logger.info("##################################################")

            #DB Connection Information   
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')

            root = db_info.getroot()      
            # Import Repository DB Information                 
            import_db = root.findall("import_db")

            im_dbname = [x.findtext("sid") for x in import_db]
            im_host = [x.findtext("conn_ip") for x in import_db]
            im_port = [x.findtext("conn_port") for x in import_db]
            im_user = [x.findtext("user") for x in import_db]
            im_password = [x.findtext("password") for x in import_db]

            engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
            logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

            #입력받은 기간동안 저장된 parquet 파일을 로드하여 인서트하는 반복문
            start_tm = datetime.now() # Query 시작 시간 추출하기 
            logger.info("Insert 시작시간 : "+ str(start_tm))

            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')

            logger.info("load File 일자: "+ str(date_condition))

            #export data 파일을 읽어온다
            #result = pd.read_csv(".//ora_session_info_csv_220703.csv") 
            result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\ora_session_info_parquet_" +date_condition+ ".parquet", engine='pyarrow')
            #result = pd.read_pickle(".//ora_session_info_pickle_220702.pkl") 
            #result = pd.read_feather(".//ora_session_info_feather_220703.ftr", use_threads=True) 
            #logger.info("..\\..\\..\\export_file\\parquet\\ora_session_info_parquet_" +date_condition+ ".parquet")
            #result = result

            # Import Data
            result.to_sql(name = 'ae_session_info',
                        con = engine,
                        schema = 'public',
                        if_exists = 'append',
                        index = False#,
                        # chunksize=10000,
                        # method='multi'
                        )

            engine.dispose()

            buf = StringIO()
            result.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(result))
            logger.info(buf.getvalue())

            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("Insert 종료시간 : "+ str(end_tm))    
            logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
            logger.info("ora_session_info_p20"+date_condition+" Import 데이터 ae_session_info Insert 완료")
            logger.info("##################################################\n")              
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())    
        #%%

    def ora_session_stat(self):
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log11")         
            logger.info("##################################################")

            #DB Connection Information   
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')

            root = db_info.getroot()      
            # Import Repository DB Information                 
            import_db = root.findall("import_db")

            im_dbname = [x.findtext("sid") for x in import_db]
            im_host = [x.findtext("conn_ip") for x in import_db]
            im_port = [x.findtext("conn_port") for x in import_db]
            im_user = [x.findtext("user") for x in import_db]
            im_password = [x.findtext("password") for x in import_db]

            engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
            logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

            #입력받은 기간동안 저장된 parquet 파일을 로드하여 인서트하는 반복문
            start_tm = datetime.now() # Query 시작 시간 추출하기 
            logger.info("Insert 시작시간 : "+ str(start_tm))

            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')

            logger.info("load File 일자: "+ str(date_condition))

            #export data 파일을 읽어온다
            #result = pd.read_csv(".//ora_session_stat_csv_220703.csv") 
            result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\ora_session_stat_parquet_" +date_condition+ ".parquet", engine='pyarrow')
            #result = pd.read_pickle(".//ora_session_stat_pickle_220702.pkl") 
            #result = pd.read_feather(".//ora_session_stat_feather_220703.ftr", use_threads=True) 
            #logger.info("..\\..\\..\\export_file\\parquet\\ora_session_stat_parquet_" +date_condition+ ".parquet")
            #result = result

            # Import Data
            result.to_sql(name = 'ae_session_stat',
                        con = engine,
                        schema = 'public',
                        if_exists = 'append',
                        index = False#,
                        # chunksize=10000,
                        # method='multi'
                        )

            engine.dispose()

            buf = StringIO()
            result.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(result))
            logger.info(buf.getvalue())

            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("Insert 종료시간 : "+ str(end_tm))    
            logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
            logger.info("ora_session_stat_p20"+date_condition+" Import 데이터 ae_session_stat Insert 완료")
            logger.info("##################################################\n")              
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())    
        #%%

    def apm_sql_list(self):       
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log12")         
            logger.info("##################################################")

            #DB Connection Information   
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')

            root = db_info.getroot()      
            # Import Repository DB Information                 
            import_db = root.findall("import_db")

            im_dbname = [x.findtext("sid") for x in import_db]
            im_host = [x.findtext("conn_ip") for x in import_db]
            im_port = [x.findtext("conn_port") for x in import_db]
            im_user = [x.findtext("user") for x in import_db]
            im_password = [x.findtext("password") for x in import_db]

            engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
            logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

            #입력받은 기간동안 저장된 parquet 파일을 로드하여 인서트하는 반복문

            start_tm = datetime.now() # Query 시작 시간 추출하기 
            logger.info("Insert 시작시간 : "+ str(start_tm))

            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')

            logger.info("load File 일자: "+ str(date_condition))

            #export data 파일을 읽어온다
            #result = pd.read_csv(".//apm_sql_list_csv_220703.csv") 
            result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\apm_sql_list_parquet_" +date_condition+ ".parquet", engine='pyarrow')
            #result = pd.read_pickle(".//apm_sql_list_pickle_220702.pkl") 
            #result = pd.read_feather(".//apm_sql_list_feather_220703.ftr", use_threads=True) 
            #logger.info("..\\..\\..\\export_file\\parquet\\apm_sql_list_parquet_" +date_condition+ ".parquet")
            #result = result

#22.07.13 insert 테이블 명 수정 : apm_sql_list -> ae_db_sql_text        
            # Import Data
            result.to_sql(name = 'ae_db_sql_text',
                        con = engine,
                        schema = 'public',
                        if_exists = 'append',
                        index = False#,
                        # chunksize=10000,
                        # method='multi'
                        )

            engine.dispose()

            buf = StringIO()
            result.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(result))
            logger.info(buf.getvalue())

            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("Insert 종료시간 : "+ str(end_tm))    
            logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
            logger.info("apm_sql_list_p20"+date_condition+" Import 데이터 ae_db_sql_text Insert 완료")
            logger.info("##################################################\n")              
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())    
    #%%
    def ora_sql_stat_10min(self):       
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log13")         
            logger.info("##################################################")

            #DB Connection Information   
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')

            root = db_info.getroot()      
            # Import Repository DB Information                 
            import_db = root.findall("import_db")

            im_dbname = [x.findtext("sid") for x in import_db]
            im_host = [x.findtext("conn_ip") for x in import_db]
            im_port = [x.findtext("conn_port") for x in import_db]
            im_user = [x.findtext("user") for x in import_db]
            im_password = [x.findtext("password") for x in import_db]

            engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
            logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

            #입력받은 기간동안 저장된 parquet 파일을 로드하여 인서트하는 반복문
            start_tm = datetime.now() # Query 시작 시간 추출하기 
            logger.info("Insert 시작시간 : "+ str(start_tm))

            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')

            logger.info("load File 일자: "+ str(date_condition))

            #export data 파일을 읽어온다
            #result = pd.read_csv(".//ora_sql_stat_10min_csv_220703.csv") 
            result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\ora_sql_stat_10min_parquet_" +date_condition+ ".parquet", engine='pyarrow')
            #result = pd.read_pickle(".//ora_sql_stat_10min_pickle_220702.pkl") 
            #result = pd.read_feather(".//ora_sql_stat_10min_feather_220703.ftr", use_threads=True) 
            #logger.info("..\\..\\..\\export_file\\parquet\\ora_sql_stat_10min_parquet_" +date_condition+ ".parquet")
            #result = result

            # Import Data
            result.to_sql(name = 'ae_sql_stat_10min',
                        con = engine,
                        schema = 'public',
                        if_exists = 'append',
                        index = False#,
                        # chunksize=10000,
                        # method='multi'
                        )

            engine.dispose()

            buf = StringIO()
            result.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(result))
            logger.info(buf.getvalue())

            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("Insert 종료시간 : "+ str(end_tm))    
            logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
            logger.info("ora_sql_stat_10min_p20"+date_condition+" Import 데이터 ae_sql_stat_10min Insert 완료")
            logger.info("##################################################\n")              
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())  

        #%%
    def ora_sql_wait_10min(self):      
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log14")         
            logger.info("##################################################")

            #DB Connection Information   
            #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
            db_info = ET.parse('..\\config\\db_info.xml')

            root = db_info.getroot()      
            # Import Repository DB Information                 
            import_db = root.findall("import_db")

            im_dbname = [x.findtext("sid") for x in import_db]
            im_host = [x.findtext("conn_ip") for x in import_db]
            im_port = [x.findtext("conn_port") for x in import_db]
            im_user = [x.findtext("user") for x in import_db]
            im_password = [x.findtext("password") for x in import_db]

            engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
            logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

            #입력받은 기간동안 저장된 parquet 파일을 로드하여 인서트하는 반복문
            start_tm = datetime.now() # Query 시작 시간 추출하기 
            logger.info("Insert 시작시간 : "+ str(start_tm))

            #export 파티션 날짜 정보 : 배치가 수행되는 날 -1
            now_day = datetime.now()
            date_condition = now_day+timedelta(days=-1)
            date_condition = date_condition.strftime('%y%m%d')

            logger.info("load File 일자: "+ str(date_condition))

            #export data 파일을 읽어온다
            #result = pd.read_csv(".//ora_sql_wait_10min_csv_220703.csv") 
            result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\ora_sql_wait_10min_parquet_" +date_condition+ ".parquet", engine='pyarrow')
            #result = pd.read_pickle(".//ora_sql_wait_10min_pickle_220702.pkl") 
            #result = pd.read_feather(".//ora_sql_wait_10min_feather_220703.ftr", use_threads=True) 
            #logger.info("..\\..\\..\\export_file\\parquet\\ora_sql_wait_10min_parquet_" +date_condition+ ".parquet")
            #result = result

            # Import Data
            result.to_sql(name = 'ae_sql_wait_10min',
                        con = engine,
                        schema = 'public',
                        if_exists = 'append',
                        index = False#,
                        # chunksize=10000,
                        # method='multi'
                        )

            engine.dispose()

            buf = StringIO()
            result.info(buf=buf)
            logger.info("적재 데이터 Info 정보")
            logger.info(type(result))
            logger.info(buf.getvalue())

            end_tm = datetime.now() # Query 종료 시간 추출하기
            logger.info("Insert 종료시간 : "+ str(end_tm))    
            logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
            logger.info("ora_sql_wait_10min_p20"+date_condition+" Import 데이터 ae_sql_wait_10min Insert 완료")
            logger.info("##################################################\n")              
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())  