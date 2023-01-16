# %%
import psycopg2 as db #postgreDB 연동 모듈 pip install psycopg2
import pandas as pd   #데이터 활용 모듈 pip install pandas
from sqlalchemy import create_engine # ORM 모듈 pip install sqlalchemy  
import xml.etree.ElementTree as ET #xml 관리 모듈
from datetime import datetime, timedelta
#import pyarrow.parquet as pq # file 관리 모듈 pip install pyarrow 
#from pyarrow import csv
import log # 별도로 만든 log.py 클래스를 사용하기 위해 import
import traceback # Exception Stack Trace 내용을 출력하기 위한 
from io import StringIO # DataFrame Info 정보를 출력하기 위해 사용한 모듈

# %% DB Connection Information
class wasJob:
    #%%
    def txn_detail(self, input_date, input_interval):
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log1")         
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
            for i in range(1,input_interval):
                start_tm = datetime.now() # Query 시작 시간 추출하기 
                logger.info("Insert 시작시간 : "+ str(start_tm))

                from_date = datetime.strptime(input_date,'%y%m%d') 
                #logger.info("from_date: "+ str(from_date))
                date_condition = from_date+timedelta(days=i-1)
                #logger.info("date_condition: "+ str(date_condition))
                date_condition = date_condition.strftime('%y%m%d')
                #logger.info("ate_condition result: "+ str(date_condition))

                logger.info("load File 일자: "+ str(date_condition))

                #export data 파일을 읽어온다
                result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\txn_detail_parquet_" +date_condition+ ".parquet", engine='pyarrow')
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
     #%%
    def txn_sql_detail(self, input_date, input_interval):
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log2")         
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
            for i in range(1,input_interval):
                start_tm = datetime.now() # Query 시작 시간 추출하기 
                logger.info("Insert 시작시간 : "+ str(start_tm))

                from_date = datetime.strptime(input_date,'%y%m%d') 
                #logger.info("from_date: "+ str(from_date))
                date_condition = from_date+timedelta(days=i-1)
                #logger.info("date_condition: "+ str(date_condition))
                date_condition = date_condition.strftime('%y%m%d')
                #logger.info("ate_condition result: "+ str(date_condition))

                logger.info("load File 일자: "+ str(date_condition))

                #export data 파일을 읽어온다 
                result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\txn_sql_detail_parquet_" +date_condition+ ".parquet", engine='pyarrow')
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
    def txn_sql_fetch(self, input_date, input_interval):
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

            #입력받은 기간동안 저장된 parquet 파일을 로드하여 인서트하는 반복문
            for i in range(1,input_interval):
                start_tm = datetime.now() # Query 시작 시간 추출하기 
                logger.info("Insert 시작시간 : "+ str(start_tm))

                from_date = datetime.strptime(input_date,'%y%m%d') 
                #logger.info("from_date: "+ str(from_date))
                date_condition = from_date+timedelta(days=i-1)
                #logger.info("date_condition: "+ str(date_condition))
                date_condition = date_condition.strftime('%y%m%d')
                #logger.info("ate_condition result: "+ str(date_condition))

                logger.info("load File 일자: "+ str(date_condition))

                #export data 파일을 읽어온다 
                result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\txn_sql_fetch_parquet_" +date_condition+ ".parquet", engine='pyarrow')
                #result = result
                
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
                logger.info("xapm_txn_sql_detail_p20"+date_condition+" Import 데이터 ae_txn_sql_detail Insert 완료")
                logger.info("##################################################\n")              
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())   

class dbJob:
    #22.07.13 insert ora_session_info 메소드 신규 추가
    def ora_session_info(self, input_date, input_interval):
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log3")         
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
            for i in range(1,input_interval):
                #입력받은 기간동안 저장된 parquet 파일을 로드하여 인서트하는 반복문
                start_tm = datetime.now() # Query 시작 시간 추출하기 
                logger.info("Insert 시작시간 : "+ str(start_tm))

                from_date = datetime.strptime(input_date,'%y%m%d') 
                #logger.info("from_date: "+ str(from_date))
                date_condition = from_date+timedelta(days=i-1)
                #logger.info("date_condition: "+ str(date_condition))
                date_condition = date_condition.strftime('%y%m%d')
                #logger.info("ate_condition result: "+ str(date_condition))

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
    def ora_session_stat(self, input_date, input_interval):
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log4")         
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
            for i in range(1,input_interval):
                start_tm = datetime.now() # Query 시작 시간 추출하기 
                logger.info("Insert 시작시간 : "+ str(start_tm))

                from_date = datetime.strptime(input_date,'%y%m%d') 
                #logger.info("from_date: "+ str(from_date))
                date_condition = from_date+timedelta(days=i-1)
                #logger.info("date_condition: "+ str(date_condition))
                date_condition = date_condition.strftime('%y%m%d')
                #logger.info("ate_condition result: "+ str(date_condition))

                logger.info("load File 일자: "+ str(date_condition))

                #export data 파일을 읽어온다
                result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\ora_session_stat_parquet_" +date_condition+ ".parquet", engine='pyarrow')
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
    def apm_sql_list(self, input_date, input_interval):       
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log5")         
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
            for i in range(1,input_interval):
                start_tm = datetime.now() # Query 시작 시간 추출하기 
                logger.info("Insert 시작시간 : "+ str(start_tm))

                from_date = datetime.strptime(input_date,'%y%m%d') 
                #logger.info("from_date: "+ str(from_date))
                date_condition = from_date+timedelta(days=i-1)
                #logger.info("date_condition: "+ str(date_condition))
                date_condition = date_condition.strftime('%y%m%d')
                #logger.info("ate_condition result: "+ str(date_condition))

                logger.info("load File 일자: "+ str(date_condition))

                #export data 파일을 읽어온다
                result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\apm_sql_list_parquet_" +date_condition+ ".parquet", engine='pyarrow')
                #result = result

#22.07.13 insert 테이블 명 수정 apm_sql_list -< ae_db_sql_text        
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
    def ora_sql_stat_10min(self, input_date, input_interval):       
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log6")         
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
            for i in range(1,input_interval):
                start_tm = datetime.now() # Query 시작 시간 추출하기 
                logger.info("Insert 시작시간 : "+ str(start_tm))

                from_date = datetime.strptime(input_date,'%y%m%d') 
                #logger.info("from_date: "+ str(from_date))
                date_condition = from_date+timedelta(days=i-1)
                #logger.info("date_condition: "+ str(date_condition))
                date_condition = date_condition.strftime('%y%m%d')
                #logger.info("ate_condition result: "+ str(date_condition))

                logger.info("load File 일자: "+ str(date_condition))

                #export data 파일을 읽어온다
                result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\ora_sql_stat_10min_parquet_" +date_condition+ ".parquet", engine='pyarrow')
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
    def ora_sql_wait_10min(self, input_date, input_interval):      
        try:
            #log 파일을 생성하기 위한 logger 인스턴스 생성
            logger = log.get_logger("log7")         
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
            for i in range(1,input_interval):
                start_tm = datetime.now() # Query 시작 시간 추출하기 
                logger.info("Insert 시작시간 : "+ str(start_tm))

                from_date = datetime.strptime(input_date,'%y%m%d') 
                #logger.info("from_date: "+ str(from_date))
                date_condition = from_date+timedelta(days=i-1)
                #logger.info("date_condition: "+ str(date_condition))
                date_condition = date_condition.strftime('%y%m%d')
                #logger.info("ate_condition result: "+ str(date_condition))

                logger.info("load File 일자: "+ str(date_condition))

                #export data 파일을 읽어온다
                result = pd.read_parquet("..\\..\\..\\export_file\\parquet\\ora_sql_wait_10min_parquet_" +date_condition+ ".parquet", engine='pyarrow')
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