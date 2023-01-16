import batch_job

#%%WAS 상세 데이터 export
## batch_job.py 모듈 wasBatch 클래스 객체 선언
pb = batch_job.processBatch()  
## wasBatch 클래스 txn_detail(),txn_sql_detail() 메소드 호출
pb.was_procs_batch()
#pb.db_procs_batch()
