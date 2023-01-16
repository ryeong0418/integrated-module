select a.txn_name AS TXN_NM
	  --,a.was_sql_id
	  ,b.sql_id as SQL_ID 
	  ,a.was_sql_elap_time as SQL_ELAP_TOT 
	  ,a.was_sql_exec_cnt as SQL_EXEC_TOT
	  ,a.was_sql_1_max as SQL_ELAP_MAX
	  --,b.db_sql_exec
	  ,b.db_lio_total as SQL_LIO_TOT
	  ,b.db_pio_total as SQL_PIO_TOT
	  ,b.db_sql_1exec_lio as SQL_1EXEC_LIO
	  ,b.db_sql_1exec_pio as SQL_1EXEC_PIO
	  ,b.db_sql_elap_time as DB_SQL_ELAP_TOT
	  ,b.db_sql_cpu_time as DB_SQL_CTM_TOT
      ,b.db_sql_wait as DB_SQL_WTM_TOT
      ,b.db_sql_wait_time_top1 as SQL_WEVTM_TOP1
      ,b.db_sql_wait_event_top1 as SQL_WEV_TOP1  
      ,'1' as MAX_SQL_ID_RANK     
from   (
       select *
        from   (
        select a.* ,
               c.db_sql_uid ,
               row_number( ) over( partition by txn_name
                              order     by was_sql_elap_time desc ) rank
        from   (
                select txn_name ,
                       was_sql_id  ,
                       aa.dpm_db_id db_id ,
                       round( sum( sql_elapse_sum ) /1000 , 1 ) as was_sql_elap_time ,
                       sum( sql_exec_count_sum ) as was_sql_exec_cnt ,
                       max( sql_elapse_max ) as was_sql_1_max
                from   ae_txn_sql_summary atss ,
                       (
                        select adi.db_id dpm_db_id ,
                               awdi.db_id apm_db_id
                        from   ae_was_db_info awdi ,
                               ae_db_info adi
                        where  upper( split_part( awdi.instance_name , '.' , 6 ) ) = adi.instance_name --  apm 에서 저장된 db_id와 dpm에서 저장된 db_id 매핑
                        and    awdi.host_ip = adi.host_ip
                       ) aa
                where  was_sql_id is not null
                and    atss.was_db_id=aa.apm_db_id
                group  by txn_name ,
                       was_sql_id ,
                       aa.dpm_db_id  
               ) a left outer join ae_sql_text c --- was 성능 정보 집함 a , 뒤단의 db 성능정보와 매핑하기 위에 db_sql_uid를 찾는다 (아우터 조인)
                                on a.was_sql_id=c.was_sql_id
         ) b
        where  b.rank = 1       
       ) a left outer join 
       (
        select stat.db_id ,
               stat.sql_uid ,
               stat.sql_id ,
               stat.db_sql_exec ,
               stat.db_lio_total,
               stat.db_pio_total ,
               stat.db_sql_1exec_lio,
               stat.db_sql_1exec_pio,
               stat.db_sql_elap_time ,
               stat.db_sql_cpu_time ,
               stat.db_sql_wait,
               wait.db_sql_wait_time_top1 ,
               wait.db_sql_wait_event_top1              
        from   (
                select assm.db_id ,
                       assm.sql_uid ,
                       assm.sql_id,
                       round(sum( assm.execution_count ), 1) as db_sql_exec ,
                       round(sum( assm.logical_reads ), 1) as db_lio_total ,
                       round(sum( assm.physical_reads ), 1) as db_pio_total ,
                       round(sum(assm.logical_reads) / sum(assm.execution_count), 1) as db_sql_1exec_lio,
                       round(sum(assm.physical_reads) / sum(assm.execution_count), 1) as db_sql_1exec_pio,
                       round(sum( assm.elapsed_time ) /100, 1) as db_sql_elap_time ,
                       round(sum( assm.cpu_time ) /100, 1) as db_sql_cpu_time ,
                       round(sum( assm.wait_time ) /100, 1) as db_sql_wait
                from   ae_sql_stat_10min assm
				where   assm.execution_count > 0
                group  by assm.db_id ,
                       assm.sql_uid,
                       assm.sql_id
               ) stat left outer join 
               (
                select db_id ,
                       sql_uid ,
                       wait_time db_sql_wait_time_top1 ,
                       event_name db_sql_wait_event_top1
                from   (
                        select db_id ,
                               sql_uid ,
                               event_name ,
                               wait_time ,
                               row_number( ) over( partition by db_id , sql_uid order     by wait_time desc ) rank
                        from   (
                                select db_id ,
                                       sql_uid ,
                                       round(sum( wait_time ) /100, 1) as wait_time ,
                                       event_name
                                from   ae_sql_wait_10min aswm
                                group  by db_id ,
                                       sql_uid ,
                                       event_name
                               ) a
                       ) b
                where  b.rank=1 
               ) wait
        on    stat.db_id = wait.db_id
        and    stat.sql_uid = wait.sql_uid
       ) b
       on   a.db_id=b.db_id
      and    a.db_sql_uid=b.sql_uid
	 order  by  was_sql_elap_time desc , a.txn_name  