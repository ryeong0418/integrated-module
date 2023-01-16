#%%
import psycopg2 as db #postgreDB 연동 모듈 pip install psycopg2
import pandas as pd   #데이터 활용 모듈 pip install pandas
from pandas.api.types import is_numeric_dtype # 데이터 타입이 int형인지 확인 하기 위한 모듈
from pandas.api.types import is_string_dtype  # 데이터 타입이 str형인지 확인하기 위한 모듈
import xml.etree.ElementTree as ET #xml 관리 모듈
from datetime import datetime, timedelta
import log # 별도로 만든 log.py 클래스를 사용하기 위해 import
import traceback # Exception Stack Trace 내용을 출력하기 위한 모듈
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
import os
#%%
class exportJob:
    #%%수동 Export 시 날짜를 입력받아서 처리 
    def procs_job(self):
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
            
            file_list = os.listdir('..\\config\\query') 
            print(file_list)

            for sql_name in file_list:
                file_text = open("..\\config\\query\\"+sql_name+"", "r", encoding='UTF-8')   
                conn=db.connect(conn_string)      
                cur=conn.cursor() 

                query = file_text.read()

                cur.execute(query)
                rs_list = cur.fetchall() 
                df_result = pd.DataFrame(rs_list)
                #column 정보도 dataframe에 넣는 로직
                df_result.columns = [i_desc[0] for i_desc in cur.description] # Columns       
                #column 값을 전부 대문자로 변환    
                df_result.columns = map(lambda x: str(x).upper(), df_result.columns)
                #int형으로 변환할 수 있는 데이터는 변환하고(pd.to_numeric), 변환 하지 못하는 문자열은 그대로 둔다(errors='ignore')
                df_result = df_result.apply(pd.to_numeric, errors='ignore')

                # TIME 컬럼 값이 있으면 date Tyep로 변환한다
                if 'TIME' in df_result.columns :
                    df_result['TIME'] = pd.to_datetime(df_result['TIME'])
                    
                now_day = datetime.now() 
                prtitionDate = now_day.strftime('%y%m%d')

                #해당 경로에 해당 파일이 있는지 여부를 확인하기 위한 변수
                path_file = "..\\..\\..\\export_file\\excel\\ae_was_db_summary_"+prtitionDate+".xlsx"
                
                #파일명을 추출하여 sheet_name으로 하기 위한 변수
                sheet_name_txt = sql_name.split('.',1)           
                #sheet_name_txt[0] = ILLEGAL_CHARACTERS_RE.sub(r'',sheet_name_txt[0])
                
                logger.info(sheet_name_txt[0]) 
                if not os.path.exists(path_file):                    
                    logger.info("ae_was_db_summary_"+prtitionDate+".xlsx 파일 신규 생성") 
                    with pd.ExcelWriter(path_file, mode='w', engine='openpyxl') as writer:
                        df_result.to_excel(writer, sheet_name=sheet_name_txt[0] , index=False)                       

                else:
                    logger.info("기존 엑셀 파일"+sheet_name_txt[0]+" 시트 업데이트") 
                    with pd.ExcelWriter(path_file, mode='a', engine='openpyxl', if_sheet_exists= 'replace') as writer:                                    
                        df_result.to_excel(writer, sheet_name=sheet_name_txt[0]  , index=False)
                       
                cur.close()
                conn.close()
                file_text.close()                            
             
            logger.info("##################################################\n")
        except:
            logger.error("예외가 발생했습니다.\n %s", traceback.format_exc())