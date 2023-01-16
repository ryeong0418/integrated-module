select txn_name ,
       round((max( txn_elapse_sum ) / 1000),1) txn_elapse_sum ,
       round((sum( sql_elapse_sum ) / 1000),1) sql_elapse_sum ,
       max( txn_exec_count ) txn_exec_count ,
       sum( sql_exec_count_sum ) sql_exec_count_sum ,
       max( fetched_rows_sum ) fetched_rows_sum ,
       max( fetched_rows_sum ) /sum( sql_exec_count_sum ) fetch_count_avg ,
       max( exception_sum ) exception_sum
from   ae_txn_sql_summary
where  1=1
--and    ten_min_time >= '2022-07-27 00:00:00' ::timestamp
--and    ten_min_time < '2022-07-27 00:10:00' ::timestamp
group  by txn_name