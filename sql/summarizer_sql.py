class SummarizerQuery:

    DDL_ae_txn_detail_summary_temp_SQL = (
            """
             drop table if exists ae_txn_detail_summary_temp;
             create table ae_txn_detail_summary_temp as
                    select date_trunc( 'hour' , time ) +((( date_part( 'minute' , time ) ::integer /10::integer ) * 10::integer ) || ' minutes' ) ::interval as ten_min_time ,
                            awi.was_name as was_name ,
                            atn.txn_name as txn_name ,
                            count( atd.tid ) as txn_exec_count ,
                            sum( atd.txn_elapse ) as txn_elapse_sum ,
                            avg( atd.txn_elapse ) as txn_elapse_avg ,
                            max( atd.txn_elapse ) as txn_elapse_max ,
                            sum( atd.txn_cpu_time ) as txn_cpu_time_sum ,
                            avg( atd.txn_cpu_time ) as txn_cpu_time_avg ,
                            max( atd.txn_cpu_time ) as txn_cpu_time_max ,
                            avg( atd.thread_memory ) as thread_memory_avg ,
                            max( atd.thread_memory ) as thread_memory_max ,
                            sum( atd.fetched_rows ) as fetched_rows_sum ,
                            avg( atd.fetched_rows ) as fetched_rows_avg ,
                            max( atd.fetched_rows ) as fetched_rows_max ,
                            sum( atd.fetch_time ) as fetch_time_sum ,
                            avg( atd.fetch_time ) as fetch_time_avg ,
                            max( atd.fetch_time ) as fetch_time_max ,
                            sum(atd.jdbc_fetch_count) as jdbc_fetch_count_sum,
                            avg(atd.jdbc_fetch_count) as jdbc_fetch_count_avg,
                            max(atd.jdbc_fetch_count) as jdbc_fetch_count_max,
                            sum( atd.exception ) as exception_sum ,
                            sum( atd.remote_count ) as remote_count_sum ,
                            avg( atd.remote_count ) as remote_count_avg ,
                            max( atd.remote_count ) as remote_count_max ,
                            sum( atd.remote_elapse ) as remote_elapse_sum ,
                            avg( atd.remote_elapse ) as remote_elapse_avg ,
                            max( atd.remote_elapse ) as remote_elapse_max
                    from   ae_txn_detail atd ,
                        ae_txn_name atn ,
                        ae_was_info awi
                    where  1 = 1
                    and    atn.txn_id = atd.txn_id
                    and    atd.was_id = awi.was_id
                    and atd.time >= '""" + "#(StartDate)" + """'::timestamp
                                and atd.time < '""" + "#(EndDate)" + """'::timestamp
                                group  by ten_min_time ,
                                            was_name ,
                                            txn_name                  
                    """
    )

    DDL_ae_txn_sql_detail_summary_temp_SQL = (
            """            
                drop table if exists ae_txn_sql_detail_summary_temp;         
                         create table ae_txn_sql_detail_summary_temp as
                                select date_trunc( 'hour' , time ) +((( date_part( 'minute' , time ) ::integer /10::integer ) * 10::integer ) || ' minutes' ) ::interval as ten_min_time ,
                                    awi.was_name as was_name ,
                                    atn.txn_name as txn_name ,
                                    atsd.sql_id as was_sql_id ,
                                    sum( atsd.execute_count ) as sql_exec_count_sum ,
                                    sum( atsd.elapsed_time ) as sql_elapse_sum ,
                                    avg( atsd.elapsed_time ) as sql_elapse_avg ,
                                    max( atsd.elapsed_time_max ) as sql_elapse_max ,
                                    aei.instance_name ,
                                    atsd.db_id ,
                                    aei.db_type ,
                                    atsd.sid
                                from   ae_txn_sql_detail atsd ,
                                    ae_was_db_info aei ,
                                    ae_txn_name atn ,
                                    ae_was_info awi
                                where  1=1
                                and    atsd.db_id = aei.db_id
                                and    atn.txn_id=atsd.txn_id
                                and    atsd.was_id = awi.was_id
                                AND atsd.time >= '""" + "#(StartDate)" + """'::timestamp
                                        AND atsd.time < '""" + "#(EndDate)" + """'::timestamp
                                        group  by ten_min_time ,
                                            was_name ,
                                            txn_name ,
                                            aei.instance_name ,
                                            aei.db_type ,
                                            atsd.db_id ,
                                            atsd.sid ,
                                            atsd.sql_id 
                    """

    )


class InterMaxGaugeSummarizerQuery:

    WAS_DB_JOIN = (
        """
             select tt.ten_min_time ,
                tt.was_name ,
                tt.txn_name ,
                tt.txn_exec_count ,
                tt.exception_sum ,
                tt.txn_elapse_sum ,
                tt.txn_elapse_avg ,
                tt.txn_elapse_max ,
                tt.txn_cpu_time_sum ,
                tt.txn_cpu_time_avg ,
                tt.txn_cpu_time_max ,
                tt.thread_memory_avg ,
                tt.thread_memory_max ,
                tt.fetched_rows_sum ,
                tt.fetched_rows_avg ,
                tt.fetched_rows_max ,
                tt.fetch_time_sum ,
                tt.fetch_time_avg ,
                tt.fetch_time_max ,
                tt.remote_count_sum ,
                tt.remote_count_avg ,
                tt.remote_count_max ,
                tt.remote_elapse_sum ,
                tt.remote_elapse_avg ,
                tt.remote_elapse_max,	   
                st.instance_name ,
                st.db_id as was_db_id,
                st.db_type ,
                st.sid ,
                st.was_sql_id ,
                st.sql_exec_count_sum ,
                st.sql_elapse_sum ,
                st.sql_elapse_avg ,
                st.sql_elapse_max       
            from   ae_txn_detail_summary_temp tt left outer join ae_txn_sql_detail_summary_temp st
                on tt.was_name = st.was_name
                and tt.txn_name = st.txn_name
                and tt.ten_min_time = st.ten_min_time
            where tt.ten_min_time >= '""" + "#(StartDate)" + """'::timestamp
    and tt.ten_min_time < '""" + "#(EndDate)" + """'::timestamp
    order  by tt.ten_min_time asc    
        """
    )