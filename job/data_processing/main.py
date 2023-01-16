import procs_job
import sql_text_merge_job

# 원하는 메뉴 선택
Btn_data = input("작업 메뉴 선택 (1.WAS전처리, 2.DB전처리, 3.SQL TEXT Merge :")


if Btn_data == '1':  
    #import 기간을 입력받는 로직
    input_date = input("인서트 시작 일자를 입력해주세요(예:220601(년월일) :")
    input_interval = input("인서트 기간을 입력해주세요(예:5) :")
    input_interval = int(input_interval)+1 

    #procs_job.py 모듈 procs_job 클래스 객체 선언
    pj = procs_job.processJob()  
    #procs_job 클래스 was_procs_job() 메소드 호출
    pj.was_procs_job(input_date, input_interval)

if Btn_data == '2':
    #import 기간을 입력받는 로직
    input_date = input("인서트 시작 일자를 입력해주세요(예:220601(년월일) :")
    input_interval = input("인서트 기간을 입력해주세요(예:5) :")
    input_interval = int(input_interval)+1 

if Btn_data == '3':
    #sql_text_merge_job.py 모듈 action() 메소드 실행
    sql_text_merge_job.action()
