select a.was_date as DATE ,
       a.txn_name as TXN_NM,
       --a.was_sql_id,
       b.sql_id as SQL_ID,
       a.was_sql_elap_time as SQL_ELAP_TOT,
       a.was_sql_exec_cnt as SQL_EXEC_TOT,
       a.was_sql_elap_avg as SQL_ELAP_AVG,
       a.was_sql_1_max as SQL_ELAP_MAX,
       b.db_sql_exec as DB_SQL_EXE,
       b.db_lio_total as SQL_LIO_TOT,
       b.db_pio_total as SQL_PIO_TOT,
       b.db_sql_1exec_lio as SQL_1EXEC_LIO,
       b.db_sql_1exec_pio as SQL_1EXEC_PIO,
       b.db_sql_elap_time as DB_SQL_ELAP_TOT,
       b.db_sql_cpu_time as DB_SQL_CTM_TOT,
       b.db_sql_wait_time_tot as DB_SQL_WTM_TOT,
       b.db_sql_wait_event_top1 as SQL_WEV_TOP1,
       b.db_sql_wait_event_top1_time as SQL_WEVTM_TOP1,
       b.db_sql_wait_event_top2 as SQL_WEV_TOP2,
       b.db_sql_wait_event_top2_time as SQL_WEVTM_TOP2,
       b.db_sql_wait_event_top3 as SQL_WEV_TOP3,
       b.db_sql_wait_event_top3_time as SQL_WEVTM_TOP3
from   (
         select *
        from   (
        select a.* ,
               c.db_sql_uid ,
               row_number( ) over( partition by was_date, txn_name  order     by was_sql_elap_time desc ) rank
        from   (
                select to_char(ten_min_time,'yyyy-mm-dd') was_date,
                       txn_name ,
                       was_sql_id  ,
                       aa.dpm_db_id db_id ,
                       round( sum( sql_elapse_sum ) /1000 , 1 ) as was_sql_elap_time ,
                       sum( sql_exec_count_sum ) as was_sql_exec_cnt ,
                       avg( sql_elapse_avg ) as was_sql_elap_avg ,
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
                and    atss.txn_name='/IMX_Test/Sleep_Test.do'     ---특정 트랜잭션 네임을 조건으로 들어가야한다.
                group  by to_char(ten_min_time,'yyyy-mm-dd'),
                         txn_name ,
                       was_sql_id ,
                       aa.dpm_db_id 
               ) a left outer join ae_sql_text c --- was 성능 정보 집함 a , 뒤단의 db 성능정보와 매핑하기 위에 db_sql_uid를 찾는다 (아우터 조인)
                                on a.was_sql_id=c.was_sql_id
         ) b
        where  b.rank in ( 1 , 2 , 3 )       
       ) a left outer join 
       (
        select stat.db_date,
               stat.db_id ,
               stat.sql_uid ,
               stat.sql_id,
               stat.db_sql_exec ,
               stat.db_lio_total,
               stat.db_pio_total ,
               stat.db_sql_1exec_lio,
               stat.db_sql_1exec_pio,
               stat.db_sql_elap_time ,
               stat.db_sql_cpu_time ,
               wait.db_sql_wait_time_tot,
               wait.db_sql_wait_event_top1,
               wait.db_sql_wait_event_top1_time,
               wait.db_sql_wait_event_top2,
               wait.db_sql_wait_event_top2_time,
               wait.db_sql_wait_event_top3,
               wait.db_sql_wait_event_top3_time
        from   (
                select to_char(assm.time,'yyyy-mm-dd') db_date,
                       assm.db_id ,
                       assm.sql_uid ,
                       assm.sql_id,
                       round(sum( assm.execution_count ) ,1) as db_sql_exec ,
                       round(sum( assm.logical_reads ) ,1) as db_lio_total ,
                       round(sum( assm.physical_reads ) ,1) as db_pio_total ,
                       round(sum(assm.logical_reads) / sum(assm.execution_count) ,1) as db_sql_1exec_lio,
                       round(sum(assm.physical_reads) / sum(assm.execution_count) ,1) as db_sql_1exec_pio,
                       round(sum( assm.elapsed_time ) /100 ,1) as db_sql_elap_time ,
                       round(sum( assm.cpu_time ) /100 ,1) as db_sql_cpu_time ,
                       round(sum( assm.wait_time ) /100 ,1) as db_sql_wait_time
                from   ae_sql_stat_10min assm
                where   assm.execution_count > 0
                group  by to_char(assm.time,'yyyy-mm-dd') ,
                       assm.db_id ,
                       assm.sql_uid,
                       assm.sql_id
               ) stat left outer join 
               (
                select db_date,
                       db_id ,
                       sql_uid ,                      
                       max(sum_wait) db_sql_wait_time_tot ,
                       max(case when rank=1 then event_name end) db_sql_wait_event_top1,
                       max(case when rank=1 then wait_time  end) db_sql_wait_event_top1_time,
                       max(case when rank=2 then event_name end) db_sql_wait_event_top2,
                       max(case when rank=2 then wait_time  end) db_sql_wait_event_top2_time,
                       max(case when rank=3 then event_name end) db_sql_wait_event_top3,
                       max(case when rank=3 then wait_time  end) db_sql_wait_event_top3_time
                from   (
                        select db_date,
                               db_id ,
                               sql_uid ,
                               event_name ,
                               wait_time ,
                               sum(wait_time) over ( partition by db_date, db_id , sql_uid ) sum_wait,  
                               row_number( ) over( partition by db_date, db_id , sql_uid order     by wait_time desc ) rank
                        from   (
                                select to_char(time,'yyyy-mm-dd') db_date,
                                       db_id ,
                                       sql_uid ,
                                       round(sum( wait_time ) /100 ,1) as wait_time ,
                                       event_name
                                from   ae_sql_wait_10min                                
                                group  by to_char(time,'yyyy-mm-dd'),
                                       db_id ,
                                       sql_uid ,
                                       event_name
                               ) a
                       ) b                       
                       group by  db_date, db_id, sql_uid
               ) wait
        on     stat.db_date=wait.db_date
        and    stat.db_id = wait.db_id
        and    stat.sql_uid = wait.sql_uid
       ) b
       on    a.was_date=b.db_date
      and    a.db_id=b.db_id
      and    a.db_sql_uid=b.sql_uid
     order  by a.was_date, was_sql_elap_time desc