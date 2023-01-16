select TXN_NM
	  ,SUM(TXN_EXCT_TOT) as TXN_EXEC_TOT
	  ,SUM(SQL_EXCT_TOT) as SQL_EXEC_TOT
	  ,ROUND(SUM(TXN_ELAP_TOT),3) as TXN_ELAP_TOT
	  ,ROUND(AVG(TXN_ELAP_AVG),3) as TXN_ELAP_AVG
	  ,ROUND(MAX(TXN_ELAP_MAX),3) as TXN_ELAP_MAX
	  ,ROUND(SUM(SQL_ELAP_TOT),3) as SQL_ELAP_TOT
	  ,ROUND(AVG(SQL_ELAP_AVG),3) as SQL_ELAP_AVG
	  ,ROUND(MAX(SQL_ELAP_MAX),3) as SQL_ELAP_MAX
	  ,SUM(FEC_RW_TOT) as FEC_RW_TOT
	  ,ROUND(AVG(FEC_RW_AVG),1) as FEC_RW_AVG
	  ,ROUND(MAX(FEC_RW_MAX),1) as FEC_RW_MAX
	  ,SUM(FET_CT_TOT) as FET_CT_TOT
	  ,ROUND(AVG(FET_CT_AVG),1) as FET_CT_AVG
	  ,ROUND(MAX(FET_CT_MAX),1) as FET_CT_MAX
	  ,SUM(ERRCT_TOT) as ERRCT_TOT
	  ,SUM(RM_CT_TOT) as RM_CT_TOT
	  ,ROUND(SUM(RM_ELAP_TOT),3) as RM_ELAP_TOT
	  ,ROUND(AVG(RM_ELAP_AVG),3) as RM_ELAP_AVG
	  ,ROUND(MAX(RM_ELAP_MAX),3) as RM_ELAP_MAX
	  ,ROUND(AVG(TRD_MEM_AVG),1) as TRD_MEM_AVG
	  ,ROUND(AVG(TXN_CTM_AVG),1) as TXN_CTM_AVG
from ( --SQL_ID 별 별도 집계를 하기 위한 인라인 뷰 서브쿼리
	select ten_min_time as TM_TIME
		   ,TXN_NAME as TXN_NM
	       ,round((max( txn_elapse_sum ) / 1000.0),3) as TXN_ELAP_TOT 
	       ,round((sum( sql_elapse_sum ) / 1000.0),3) as SQL_ELAP_TOT --sql_id별 집계
	       ,max( txn_exec_count ) as TXN_EXCT_TOT 
	       ,sum( sql_exec_count_sum ) as SQL_EXCT_TOT --sql_id별 집계
	       ,round((max( txn_elapse_max ) / 1000.0),3) as TXN_ELAP_MAX 
	       ,round((max( sql_elapse_max ) / 1000.0),3) as SQL_ELAP_MAX --sql_id별 집계
	       ,round((max( txn_elapse_avg ) / 1000.0),3) as TXN_ELAP_AVG 
	       ,round((avg( sql_elapse_avg ) / 1000.0),3) as SQL_ELAP_AVG --sql_id별 집계
	       ,max( fetched_rows_sum ) as FEC_RW_TOT 
	       ,max( fetched_rows_avg ) as FEC_RW_AVG 
	       ,max( fetched_rows_max ) as FEC_RW_MAX 
	       ,max( jdbc_fetch_count_sum ) as FET_CT_TOT
	       ,max( jdbc_fetch_count_avg ) as FET_CT_AVG
	       ,max( jdbc_fetch_count_max ) as FET_CT_MAX
	       ,max( exception_sum ) as ERRCT_TOT
	       ,max( remote_count_sum ) as RM_CT_TOT
	       ,round((max( remote_elapse_sum ) / 1000.0),3) as RM_ELAP_TOT 
	       ,round((max( remote_elapse_avg ) / 1000.0),3) as RM_ELAP_AVG
	       ,round((max( remote_elapse_max ) / 1000.0),3) as RM_ELAP_MAX
	       ,max( thread_memory_avg ) as TRD_MEM_AVG
	       ,round((sum( txn_cpu_time_avg ) / 1000.0),3) as TXN_CTM_AVG        
	from   ae_txn_sql_summary
	where  1=1
	--and    ten_min_time >= '2022-07-27 00:00:00' ::timestamp
	--and    ten_min_time < '2022-07-27 00:10:00' ::timestamp
	group  by txn_name, ten_min_time 
)a 
group by TXN_NM