import create_tables
import meta_data_insert
import detail_job
import export_job

# 원하는 메뉴 선택
Btn_data = input("작업 메뉴 선택 (1.테이블 생성, 2.메타데이터 인서트, 3.상세데이터 인서트, 4.수동 Export:")

if Btn_data == '1':  
    #create_tables.py 모듈 action() 메소드 호출 
    create_tables.action()

if Btn_data == '2':
    #meta_data_insert.py 모듈 action() 메소드 호출
    meta_data_insert.action()

if Btn_data == '3':
    #import 기간을 입력받는 로직
    input_date = input("인서트 시작 일자를 입력해주세요(예:220601(년월일) :")
    input_interval = input("인서트 기간을 입력해주세요(예:5) :")
    input_interval = int(input_interval)+1

    #detail_job.py 모듈 wasJob 클래스 객체 선언
    wj = detail_job.wasJob()  
    #wasJob 클래스 txn_detail() 메소드 호출
    wj.txn_detail(input_date, input_interval)
    wj.txn_sql_detail(input_date, input_interval)
    wj.txn_sql_fetch(input_date, input_interval) #22.08.01 insert 신규 메소드 호출 추가

    # batch_job.py 모듈 dbBatch 클래스 객체 선언
    dj = detail_job.dbJob()  
    #dbBatch 클래스 ora_session_stat(),apm_sql_list(),ora_sql_stat_10min(),ora_sql_wait_10min() 메소드 호출
    dj.ora_session_info(input_date, input_interval) # 22.07.03 insert 신규 메소드 호출 추가
    dj.ora_session_stat(input_date, input_interval) 
    dj.apm_sql_list(input_date, input_interval)
    dj.ora_sql_stat_10min(input_date, input_interval)
    dj.ora_sql_wait_10min(input_date, input_interval)

if Btn_data == '4':
    #import 기간을 입력받는 로직
    input_date = input("Export 일자를 입력해주세요(예:220601(년월일) :")
    input_interval = input("Export 기간을 입력해주세요(예:5) :")
    input_interval = int(input_interval)+1

    # export_job.py 모듈 wasBatch 클래스 객체 선언
    wb = export_job.wasBatch()  
    #wasBatch 클래스 txn_detail(),txn_sql_detail() 메소드 호출
    wb.txn_detail(input_date, input_interval)
    wb.txn_sql_detail(input_date, input_interval)
    wb.txn_sql_fetch(input_date, input_interval) # 22.08.01 Export 신규 메소드 호출 추가 


    # export_job.py 모듈 dbBatch 클래스 객체 선언
    db = export_job.dbBatch()  
    #dbBatch 클래스 ora_session_stat(),apm_sql_list(),ora_sql_stat_10min(),ora_sql_wait_10min() 메소드 호출
    db.ora_session_info(input_date, input_interval) # 22.07.03 export 신규 메소드 호출 추가
    db.ora_session_stat(input_date, input_interval) 
    db.apm_sql_list(input_date, input_interval)
    db.ora_sql_stat_10min(input_date, input_interval)
    db.ora_sql_wait_10min(input_date, input_interval)