#%%
import psycopg2 as db #postgreDB 연동 모듈 pip install psycopg2
import pandas as pd   #데이터 활용 모듈 pip install pandas
from sqlalchemy import create_engine # ORM 모듈 pip install sqlalchemy  
import xml.etree.ElementTree as ET #xml 관리 모듈
from datetime import datetime, timedelta
import log # 별도로 만든 log.py 클래스를 사용하기 위해 import
import traceback # Exception Stack Trace 내용을 출력하기 위한 
from io import StringIO # DataFrame Info 정보를 출력하기 위해 사용한 모듈
#%%
def action():
    try:
        #log 파일을 생성하기 위한 logger 인스턴스 생성
        logger = log.get_logger("log1")         
        logger.info("##################################################")
            
        #DB Connection Information   
        #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
        db_info = ET.parse('..\\config\\db_info.xml')
        
        root = db_info.getroot()  
        
        #InterMax Export DB 접속 정보 Read 
        student = root.findall("intermax_repo")
        
        ex_dbname = [x.findtext("sid") for x in student]
        ex_host = [x.findtext("conn_ip") for x in student]
        ex_port = [x.findtext("conn_port") for x in student]
        ex_user = [x.findtext("user") for x in student]
        ex_password =  [x.findtext("password") for x in student]
        
        #%% xapm_was_info 데이터 load 및 적재
        conn_string="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
        logger.info("Export DB 접속 정보 : "+ conn_string)      
        
        conn=db.connect(conn_string)
        logger.info("Export DB 접속 완료")
        
        start_tm = datetime.now() # Query 시작 시간 추출하기 
        logger.info("Export 시작시간 : "+ str(start_tm))
        
        query="select was_id, was_name, host_name from xapm_was_info"
    
        df = pd.read_sql(query, conn)        
       
        #print(df.head())              
        result = df
        end_tm = datetime.now() # Query 종료 시간 추출하기
        logger.info("Export 종료시간 : "+ str(end_tm))
        
        logger.info("Export 수행시간 : "+ str(end_tm - start_tm)+"")  
        logger.info("Export 데이터 조회 완료\n")    
        
        buf = StringIO()
        result.info(buf=buf)
        logger.info("적재 데이터 Info 정보")
        logger.info(type(result))
        logger.info(buf.getvalue())
         
        start_tm = datetime.now() # Query 시작 시간 추출하기 
        logger.info("Insert 시작시간 : "+ str(start_tm))
        
        #import DB 접속 정보 Read 
        student1 = root.findall("import_db")
        
        im_dbname = [x.findtext("sid") for x in student1]
        im_host = [x.findtext("conn_ip") for x in student1]
        im_port = [x.findtext("conn_port") for x in student1]
        im_user = [x.findtext("user") for x in student1]
        im_password =  [x.findtext("password") for x in student1]
        
        engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))    
        logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

        result.to_sql(name = 'ae_was_info',
                      con = engine,
                      schema = 'public',
                      if_exists = 'append',
                      index = False
                      )
        
        conn.close()
        engine.dispose()
        
        end_tm = datetime.now() # Query 종료 시간 추출하기
        logger.info("Insert 종료시간 : "+ str(end_tm))    
        logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
        logger.info("xapm_was_info Import 데이터 ae_was_info Insert 완료")
        logger.info("##################################################\n")
        
        #%% xapm_txn_name 데이터 load 및 적재
        logger.info("##################################################")
        conn_string1="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
        logger.info("Export DB 접속 정보 : "+ conn_string)
        
        conn1=db.connect(conn_string1)
        logger.info("Export DB 접속 완료")
        
        start_tm = datetime.now() # Query 시작 시간 추출하기 
        logger.info("Export 시작시간 : "+ str(start_tm))
        
        query1="select txn_id, txn_name, business_id, business_name  from xapm_txn_name"
        df1 = pd.read_sql(query1, conn1)
        
        #print(df1.head())        
        result = df1
        end_tm = datetime.now() # Query 종료 시간 추출하기
        logger.info("Export 종료시간 : "+ str(end_tm))
        
        logger.info("Export 수행시간 : "+ str(end_tm - start_tm)+"")  
        logger.info("Export 데이터 조회 완료\n")
       
        buf = StringIO()
        result.info(buf=buf)
        logger.info("적재 데이터 Info 정보")
        logger.info(type(result))
        logger.info(buf.getvalue())
        
        start_tm = datetime.now() # Query 시작 시간 추출하기 
        logger.info("Insert 시작시간 : "+ str(start_tm))
        
        engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
        logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

        result.to_sql(name = 'ae_txn_name',
                      con = engine,
                      schema = 'public',
                      if_exists = 'append',
                      index = False
                      )
        
        conn.close()
        engine.dispose()
        
        end_tm = datetime.now() # Query 종료 시간 추출하기
        logger.info("Insert 종료시간 : "+ str(end_tm))    
        logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
        logger.info("xapm_txn_name Import 데이터 ae_txn_name Insert 완료")
        logger.info("##################################################\n")    
    
        #%% xapm_sql_text 데이터 load 및 적재
        logger.info("##################################################")
        conn_string1="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
        logger.info("Export DB 접속 정보 : "+ conn_string)
        
        conn1=db.connect(conn_string1)
        logger.info("Export DB 접속 완료")
      
        start_tm = datetime.now() # Query 시작 시간 추출하기 
        logger.info("Export 시작시간 : "+ str(start_tm))
        
        query1="select sql_id, sql_text_100, sql_text from xapm_sql_text"
        df1 = pd.read_sql(query1, conn1)
        
        #print(df1.head())        
        result = df1 
        end_tm = datetime.now() # Query 종료 시간 추출하기
        logger.info("Export 종료시간 : "+ str(end_tm))
        
        logger.info("Export 수행시간 : "+ str(end_tm - start_tm)+"")  
        logger.info("Export 데이터 조회 완료\n")
       
        buf = StringIO()
        result.info(buf=buf)
        logger.info("적재 데이터 Info 정보")
        logger.info(type(result))
        logger.info(buf.getvalue())
        
        start_tm = datetime.now() # Query 시작 시간 추출하기 
        logger.info("Insert 시작시간 : "+ str(start_tm))
        
        engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
        logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

#22.07.13 insert 테이블 명 수정 ae_apm_sql_text -> ae_was_sql_text
        result.to_sql(name = 'ae_was_sql_text',
                      con = engine,
                      schema = 'public',
                      if_exists = 'append',
                      index = False
                      )
        
        conn.close()
        engine.dispose()
        
        end_tm = datetime.now() # Query 종료 시간 추출하기
        logger.info("Insert 종료시간 : "+ str(end_tm))    
        logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
        logger.info("xapm_sql_text Import 데이터 ae_apm_sql_text Insert 완료")
        logger.info("##################################################\n")

#22.07.15 ae_was_db_info insert 로직 추가
        #%% ae_was_db_info 데이터 load 및 적재
        logger.info("##################################################")
        conn_string1="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
        logger.info("Export DB 접속 정보 : "+ conn_string)
        
        conn1=db.connect(conn_string1)
        logger.info("Export DB 접속 완료")
      
        start_tm = datetime.now() # Query 시작 시간 추출하기 
        logger.info("Export 시작시간 : "+ str(start_tm))
        
        query1="select db_id, host_name, instance_name, instance_alias , db_type, host_ip, sid, lsnr_port from xapm_db_info"
        df1 = pd.read_sql(query1, conn1)
        
        #print(df1.head())        
        result = df1 
        end_tm = datetime.now() # Query 종료 시간 추출하기
        logger.info("Export 종료시간 : "+ str(end_tm))
        
        logger.info("Export 수행시간 : "+ str(end_tm - start_tm)+"")  
        logger.info("Export 데이터 조회 완료\n")
       
        buf = StringIO()
        result.info(buf=buf)
        logger.info("적재 데이터 Info 정보")
        logger.info(type(result))
        logger.info(buf.getvalue())
        
        start_tm = datetime.now() # Query 시작 시간 추출하기 
        logger.info("Insert 시작시간 : "+ str(start_tm))
        
        engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
        logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])   

        result.to_sql(name = 'ae_was_db_info',
                      con = engine,
                      schema = 'public',
                      if_exists = 'append',
                      index = False
                      )
        
        conn.close()
        engine.dispose()
        
        end_tm = datetime.now() # Query 종료 시간 추출하기
        logger.info("Insert 종료시간 : "+ str(end_tm))    
        logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
        logger.info("xapm_sql_text Import 데이터 ae_was_db_info Insert 완료")
        logger.info("##################################################\n")

        #%% apm_db_info 데이터 load 및 적재
        logger.info("##################################################")
        
        max_repo = root.findall("maxgauge_repo")
        
        ex_dbname = [x.findtext("sid") for x in max_repo]
        ex_host = [x.findtext("conn_ip") for x in max_repo]
        ex_port = [x.findtext("conn_port") for x in max_repo]
        ex_user = [x.findtext("user") for x in max_repo]
        ex_password =  [x.findtext("password") for x in max_repo]

        conn_string1="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
        logger.info("Export DB 접속 정보 : "+ conn_string)
        
        conn1=db.connect(conn_string1)
        logger.info("Export DB 접속 완료")
        
        start_tm = datetime.now() # Query 시작 시간 추출하기 
        logger.info("Export 시작시간 : "+ str(start_tm))
        
        query1="select * from APM_DB_INFO"
        df1 = pd.read_sql(query1, conn1)
        
        #print(df1.head())        
        result = df1
        end_tm = datetime.now() # Query 종료 시간 추출하기
        logger.info("Export 종료시간 : "+ str(end_tm))
        
        logger.info("Export 수행시간 : "+ str(end_tm - start_tm)+"")  
        logger.info("Export 데이터 조회 완료\n")
       
        buf = StringIO()
        result.info(buf=buf)
        logger.info("적재 데이터 Info 정보")
        logger.info(type(result))
        logger.info(buf.getvalue())
        
        start_tm = datetime.now() # Query 시작 시간 추출하기 
        logger.info("Insert 시작시간 : "+ str(start_tm))
        
        engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
        logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

        result.to_sql(name = 'ae_db_info',
                      con = engine,
                      schema = 'public',
                      if_exists = 'append',
                      index = False
                      )
        
        conn.close()
        engine.dispose()
        
        end_tm = datetime.now() # Query 종료 시간 추출하기
        logger.info("Insert 종료시간 : "+ str(end_tm))    
        logger.info("Insert 수행시간 : "+ str(end_tm - start_tm)+"")   
        logger.info("apm_db_info Import 데이터 ae_db_info Insert 완료")
        logger.info("##################################################\n") 
    except:
        logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())    