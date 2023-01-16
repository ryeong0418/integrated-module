import batch_job

#%%WAS 상세 데이터 export
## batch_job.py 모듈 wasBatch 클래스 객체 선언
wb = batch_job.wasBatch()  
## wasBatch 클래스 txn_detail(),txn_sql_detail() 메소드 호출
wb.txn_detail()
wb.txn_sql_detail()
wb.txn_sql_fetch() #22.08.01 export 신규 메소드 추가

#%%DB 상세 데이터 export
## batch_job.py 모듈 dbBatch 클래스 객체 선언
db = batch_job.dbBatch()  

## dbBatch 클래스 ora_session_stat(),apm_sql_list(),ora_sql_stat_10min(),ora_sql_wait_10min() 메소드 호출
db.ora_session_info() # 22.07.03 export 신규 메소드 호출 추가
db.ora_session_stat() 
db.apm_sql_list()
db.ora_sql_stat_10min()
db.ora_sql_wait_10min()

#%% export 된 파일를 로드하여 insert batch 클래스 객체 선언 vhgghg
imb = batch_job.impBatch()  

imb.txn_detail()
imb.txn_sql_detail()   
imb.txn_sql_fetch() #22.08.01 insert 신규 메소드 추가
imb.ora_session_info() # 22.07.03 insert 신규 메소드 호출 추가 
imb.ora_session_stat()
imb.apm_sql_list()
imb.ora_sql_stat_10min()
imb.ora_sql_wait_10min()