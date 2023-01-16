#%%
import psycopg2 as db #postgreDB 연동 모듈 pip install psycopg2
import pandas as pd   #데이터 활용 모듈 pip install pandas
import xml.etree.ElementTree as ET #xml 관리 모듈
import log # 별도로 만든 log.py 클래스를 사용하기 위해 import
import traceback # Exception Stack Trace 내용을 출력하기 위한 모듈
from sqlalchemy import create_engine # ORM 모듈 pip install sqlalchemy  
import sqlparse 
import numpy as np
#%%
def action():
    try:
        #log 파일을 생성하기 위한 logger 인스턴스 생성
        logger = log.get_logger("log1")         
        logger.info("##################################################")
        
        #DB Connection Information            
        #db_info = ET.parse('C:\\Users\\APM_PJH\\Desktop\\AE그룹\\WAS_DB 통합분석\\job\\db_info.xml')
        db_info = ET.parse('..\\config\\db_info.xml')
        
        # local Repository DB Information
        root = db_info.getroot()        

        #Local DB 접속 정로 로딩
        import_db = root.findall("import_db")
        im_dbname = [x.findtext("sid") for x in import_db]
        im_host = [x.findtext("conn_ip") for x in import_db]
        im_port = [x.findtext("conn_port") for x in import_db]
        im_user = [x.findtext("user") for x in import_db]
        im_password = [x.findtext("password") for x in import_db]
        
        conn_string="""dbname="""+im_dbname[0]+""" host="""+im_host[0]+""" port="""+im_port[0]+""" user="""+im_user[0]+""" password="""+im_password[0]
        logger.info("Local DB 접속 정보 : "+ conn_string)
        
        #InterMax DB 접속 정로 로딩
        student = root.findall("intermax_repo")    
        ex_dbname = [x.findtext("sid") for x in student]
        ex_host = [x.findtext("conn_ip") for x in student]
        ex_port = [x.findtext("conn_port") for x in student]
        ex_user = [x.findtext("user") for x in student]
        ex_password =  [x.findtext("password") for x in student]

        conn_string1="""dbname="""+ex_dbname[0]+""" host="""+ex_host[0]+""" port="""+ex_port[0]+""" user="""+ex_user[0]+""" password="""+ex_password[0]
        logger.info("InterMax DB 접속 정보 : "+ conn_string)    

        #Insert 전 ae_was_sql_text 테이블 truncate 작업    
        conn_drop=db.connect(conn_string)  
        cur=conn_drop.cursor()

        query_drop = "truncate table ae_sql_text"

        cur.execute(query_drop)
        conn_drop.commit()   

        cur.close()
        conn_drop.close()
                
        #InterMax DB 에서 xapm_sql_text 정보를 읽어온다
        conn_ex=db.connect(conn_string1)       

        query_ex="""
            select sql_id, sql_text, sql_text_100 from xapm_sql_text
            """  
            
        df_ex = pd.read_sql(query_ex, conn_ex)

        #ae_db_sql_text 데이터를 읽어와 sql_text 기준으로 상단 xapm_sql_text 테이블과 merge 작업 수행
        conn_im=db.connect(conn_string)                  

        query_im="""
            select sql_uid,
                sql_full(a.db_id, a.partition_key, a.sql_uid) as sql_text
            from (	   
                    select sql_uid,
                        db_id,
                        partition_key	   		   
                    from ae_db_sql_text 
                ) a	 
            """   
        df_im = pd.read_sql(query_im, conn_im)
        # ae_db_sql_text 테이블은 파티션이 관리되기 때문에 일자별로 중복된 데이터가 있어서 제거 작업 수행
        df_im = df_im.drop_duplicates(subset=['sql_uid'], keep='first', inplace=False, ignore_index=False)    
    
        # ae_db_sql_text 테이블 SQL_TEXT 데이터 정제 작업 (공백(' '),탭(\t),엔터(\n),앞엔터(\r))
        df_im = df_im.apply(lambda x: x.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r", " "], value=["","",""], regex=True, inplace=False), axis=1)

        for idx, row in df_im.iterrows():
            sql_text = row['sql_text']
            # ae_db_sql_text 테이블 SQL_TEXT 데이터 정제 작업 (주석제거,upper)
            df_im['sql_text'] = np.where(df_im['sql_text'] == sql_text, sqlparse.format(sql_text, reindent=False, keyword_case='upper', identifier_case='upper', strip_comments=True), df_im['sql_text'])         

        #주석 제거 여부 체크를 하기 위한 State_Code 컬럼값 df_ex DataFrame에 추가
        df_ex = df_ex.reindex(columns = df_ex.columns.tolist() + ["state_code"])   
        
        # xapm_sql_text 테이블 SQL_TEXT 데이터 정제 작업 (공백(' '),탭(\t),엔터(\n),앞엔터(\r))
        df_ex = df_ex.apply(lambda x: x.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r", " "], value=["","",""], regex=True, inplace=False), axis=1)    

        for idx, row in df_ex.iterrows():
            #np.where 조건문에 사용하기 위해 기존 SQL_TEXT 값을 sql_text변수에 세팅
            sql_text = row['sql_text']
            
            # np.where 함수 설명 : (조건문,참일경우 넣는값, 거짓일경우 넣는값)
            # xapm_sql_Text 테이블 SQL_TEXT 데이터 정제 작업 (주석제거,upper) 
            df_ex['sql_text'] = np.where(df_ex['sql_text'] == sql_text, sqlparse.format(sql_text, reindent=False, keyword_case='upper', identifier_case='upper', strip_comments=True), df_ex['sql_text'])        

            #주석 조건을 제외한 정제작업 조건을 맞추기 위해 SQL_TEXT 변수 정제 작업(공백(' '),탭(\t),엔터(\n),앞엔터(\r),upper)
            #sql_text = sql_text.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r", " "], value=["","",""])
            #sql_text = sqlparse.format(sql_text, reindent=False, keyword_case='upper', identifier_case='upper')

            #주석 제거 전 SQL_TEXT와 제거 후 SQL_TEXT 데이터를 비교하여 State_Code 데이터를 세팅하는 IF문 : 주석제거했으면 1, 않했으면 0으로 세팅
            #df_ex["state_code"] = np.where(df_ex['sql_text'] != sql_text, 1, 0)  

        # xapm_sql_text 테이블과 ae_db_sql_text 테이블을 정제된 sql_text 값 기준으로 merge 작업 수행
        df_result = pd.merge(df_ex,df_im, how='left', on=['sql_text'])      
    
        engine = create_engine("postgresql+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=im_user[0], pw=im_password[0], ip=im_host[0], port=im_port[0], db=im_dbname[0]))
        logger.info("import DB 접속 정보 : IP="+ im_host[0] + " PORT="+ im_port[0] + " SID="+ im_dbname[0] + " USER="+ im_user[0] + " PW="+ im_password[0])

        #merge 결과를 ae_sql_text 테이블 형식게 맞게 정제 작업 수행
        df_result.rename(columns={'sql_id':'was_sql_id'}, inplace=True)
        df_result.rename(columns={'sql_uid':'db_sql_uid'}, inplace=True)    
        df_result.drop(["sql_text"], axis=1, inplace=True)
        #df_result = df_result.reindex(columns = df_result.columns.tolist() + ["state_code"])

        # merge 결과를 ae_was_sql_text 테이블에 다시 인서트
        df_result.to_sql(name = 'ae_sql_text',
                        con = engine,
                        schema = 'public',
                        if_exists = 'append',
                        index = False
                        )
        
        conn_im.close()
        conn_ex.close()
        engine.dispose()

    except:
        logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())