def data_test(spark, block, base_path, nmonth=0, current_date=None, logger=None, cluster_path=''):
        
        # start = time.time()

        if (config_block[block]["daily_updated"]):
                logger.info("")
                logger.info(f"Table: {block} (daily updated)")

                alerts = []
            
                if not current_date:
                        tmp_date = datetime.date.today()
                else:
                        tmp_date = current_date
                        
                yesterday_stg1 = tmp_date - datetime.timedelta(days=1)
                yesterday_stg1_str = yesterday_stg1.strftime('%Y-%m-%d')

                yesterday_stg2 = tmp_date - datetime.timedelta(days=config_block[block]["days_back"]["stg2"])
                yesterday_stg2_str = yesterday_stg2.strftime('%Y-%m-%d')

                yesterday_stg3 = tmp_date - datetime.timedelta(days=config_block[block]["days_back"]["stg3"])
                yesterday_stg3_str = yesterday_stg3.strftime('%Y-%m-%d')
                

                if (nmonth==1):
                        today = datetime.date.today()
                        tmp_date = datetime.date(today.year, today.month, 1)
                        previous_month = today - relativedelta(months=1)

                        yesterday_stg1 = datetime.date(previous_month.year, previous_month.month, calendar.monthrange(previous_month.year, previous_month.month)[1])
                        yesterday_stg1_str = yesterday_stg1.strftime('%Y-%m-%d')

                        yesterday_stg2_str = yesterday_stg1_str
                        yesterday_stg3_str = yesterday_stg1_str                               
                        
                prev_month = yesterday_stg1 - relativedelta(months=1)
                prev_month_yyyymm = prev_month.strftime('%Y%m')

                try:
                        spark.read.parquet(cluster_path+base_path).where(f'yyyymm >= {prev_month_yyyymm}').createOrReplaceTempView("tmp")
                except:
                        msg = f"Error!!! Can't read {cluster_path+base_path}"
                        logger.info(msg)
                        send_slack_notif(msg)
                        return None
                
                if (nmonth==0):
                        logger.info("Stage 1: Recent date check")
                        # stage 1 , last date
                        
                        df_last_date=pd.to_datetime(spark.sql(f"""select max({config_block[block]['date_col']}) md 
                                                                from tmp
                                                                --where {config_block[block]['date_col']} <= cast('{tmp_date}' as date)
                                                                """).toPandas()["md"].values[0]).strftime('%Y-%m-%d')

                        if df_last_date<yesterday_stg1_str:
                                logger.info(f"Alert! Last date is {df_last_date}")
                                alerts.append(df_last_date)
                        else:
                                logger.info("OK")
                                alerts.append(None)
                        
                        # end = time.time()
                        
                        # time_taken = round((end - start)/60,2)
                        
                        # logger.info(f"stage 1 took {time_taken} mins")
                else:
                        alerts.append(None)
                
                # stage 2 and 3

                # start = time.time()

                columns = spark.read.parquet(cluster_path+base_path).columns

                stg3_dict = {}

                thrsh_stg3=0.8
                thrsh_stg2=(0.6,0.3)

                query_prev_month_p1 = f"with prev_month_val as (select 0 as dummy, avg(cnt_prev) cnt_prev_avg"
                query_prev_month_p2 = f"select count(1) cnt_prev"

                query_cur_month = f"cur_month_val as (select 0 as dummy, date_trunc('day',{config_block[block]['date_col']}) date, count(1) cnt_cur"
                
                main_query = f"select date, {thrsh_stg3} as stg3_thrsh"

                for col in columns:
                    query_prev_month_p1 += f" , avg({col}_not_null_ratio_prev) {col}_not_null_ratio_prev n"
                    query_prev_month_p2 += f" , count({col}) / count(1) {col}_not_null_ratio_prev n"

                    query_cur_month += f" , count({col}) / count(1) {col}_not_null_ratio_cur n"

                    main_query += f", ({col}_not_null_ratio_cur / {col}_not_null_ratio_prev) < {thrsh_stg3} {col}_not_null_share_bool, ({col}_not_null_ratio_cur / {col}_not_null_ratio_prev) {col}_not_null_share  n"


                query_prev_month_full = f"""{query_prev_month_p1} from ({query_prev_month_p2}
                from tmp
                        where {config_block[block]['date_col']} >= date_trunc('month', add_months('{yesterday_stg1_str}', -1)) and {config_block[block]['date_col']} < date_trunc('month', "{yesterday_stg1_str}") 
                        group by date_trunc("day",{config_block[block]['date_col']})
                        )),
                """

                query_cur_month_full = f"""{query_cur_month}
                from tmp
                        where cast({config_block[block]['date_col']} as date) between date_trunc('month','{yesterday_stg1_str}') and cast('{yesterday_stg1_str}' as date)
                        and {config_block[block]['date_col']} not in (cast('2021-12-31' as date), cast('2022-01-01' as date), cast('2022-12-31' as date), 
                        cast('2023-01-01' as date), cast('2023-12-31' as date), cast('2024-01-01' as date), cast('2024-12-31' as date), cast('2025-01-01' as date))
                        group by date_trunc("day",{config_block[block]['date_col']})
                        )
                """

                main_query_full = f"""
                {query_prev_month_full} n
                {query_cur_month_full} n

                {main_query} n
                ,case when MONTH(date) != 1 then {thrsh_stg2[0]} else {thrsh_stg2[1]} end stg2_thrshn
                ,round(cnt_cur / cnt_prev_avg, 2) filling_rate n
                ,case when MONTH(date) != 1 then (cnt_cur / cnt_prev_avg) < {thrsh_stg2[0]} 
                      else (cnt_cur / cnt_prev_avg) < {thrsh_stg2[1]} end ratio_check  n
                from cur_month_val cmv n
                left join prev_month_val pmv on cmv.dummy=pmv.dummy
                order by 1
                """
            
                stg2_3=spark.sql(main_query_full).toPandas()
                
                
                logger.info("Stage 2: Count all rows, comparison with previous month")
                
                stg2_solo = stg2_3[stg2_3["date"]<=yesterday_stg2_str].copy()
                
                if stg2_solo["ratio_check"].any():
                        tmp = stg2_solo[stg2_solo['ratio_check']][['date', 'filling_rate', 'stg2_thrsh']].copy()
                        tmp['date'] = pd.to_datetime(tmp['date']).apply(lambda x: x.strftime('%Y-%m-%d'))
                        tmp['filling_rate'] = tmp['filling_rate'].astype(float)
                        tmp['stg2_thrsh'] = tmp['stg2_thrsh'].astype(float)
                        logger.info(tmp.set_index('date'))
                        alerts.append(stg2_solo[stg2_solo['ratio_check']][["date","filling_rate"]])

                else:
                        logger.info("OK")
                        alerts.append(None)
                
                logger.info("stage 3: not null values check, column-wise")
                
                stg3_solo = stg2_3[stg2_3["date"]<=yesterday_stg3_str].copy()
                
                stg3_cols = list(set(stg3_solo.columns[2:]) - {'filling_rate', 'stg2_thrsh', 'ratio_check' })
                
                for col in [x for x in stg3_cols if ('bool' not in x) & ('yyyymm' not in x)]:
                    if stg3_solo[f'{col}_bool'].any():
                        tmp = stg3_solo[stg3_solo[f'{col}_bool']][['date', col, 'stg3_thrsh']].copy()
                        tmp['date'] = pd.to_datetime(tmp['date']).apply(lambda x: x.strftime('%Y-%m-%d'))
                        tmp['stg3_thrsh'] = tmp['stg3_thrsh'].astype(float)
                        tmp[col] = tmp[col].astype(float)
                        logger.info(f"{col.replace('_not_null_share','')} n {tmp.set_index('date')}")
                        tmp = stg3_solo[stg3_solo[f'{col}_bool']][["date",col]]
                        tmp = tmp.rename(columns={col:"curr2prev"})
                        stg3_dict[col] = tmp[["date","curr2prev"]]
                    else:
                        logger.info(f"'{col.replace('_not_null_share','')}': OK")
  
                # end = time.time()
                # time_taken = round((end - start)/60,2)
                
                # logger.info(f"stage 2 and 3 took {time_taken} mins")
                
                if len(stg3_dict)>0:
                    alerts.append(stg3_dict)
                else:
                    alerts.append(None)

        else:
                logger.info("")
                logger.info(f"Table: {block} (fully updated)")
                
                alerts=[None,None,None]
                
                try:
                        spark.read.parquet(cluster_path+base_path).createOrReplaceTempView("tmp")
                except:
                        msg = f"Error!!! Can't read {cluster_path+base_path}"
                        logger.info(msg)
                        send_slack_notif(msg)
                        return None
                
                # start = time.time()
                    
                stg1_cols={}
                cols = spark.read.parquet(cluster_path+base_path).columns

                sql_with = f"with stats as ( select count(1) all_cnt n"
                sql_main = f"select 1 dummy"

                for col in cols:
                #         logger.info(config_block[block]["threshold"][col])
                        sql_with+=f" , coalesce(count({col}), 0) {col}_not_null_cnt n"
                        sql_main+=f""" ,round(({col}_not_null_cnt/all_cnt),2) {col}_not_null_share, 
                        ({col}_not_null_cnt/all_cnt) < {config_block[block]["null_threshold"].get(col, 0.8)} {col}_not_null_share_bool, n
                        {config_block[block]['null_threshold'].get(col, 0.8)} as {col}_thrsh
                        """

                sql_with += "from tmp ) "       

                sql_main_full = f"""
                                {sql_with}

                                {sql_main}

                                from stats
                                """

                stg1_1=spark.sql(sql_main_full).toPandas()

                for col in [x for x in cols if ('check' not in x) & ('yyyymm' not in x)]:
                    if stg1_1[f"{col}_not_null_share_bool"].any():
                            tmp = stg1_1[[f'{col}_not_null_share',f'{col}_thrsh']].copy()
                            tmp[f'{col}_not_null_share'] = tmp[f'{col}_not_null_share'].astype(float)
                            tmp[f'{col}_thrsh'] = tmp[f'{col}_thrsh'].astype(float)
                            logger.info(f"{col} n {tmp}")
                            stg1_cols[col] = stg1_1[[f"{col}_not_null_share"]].values[0]
                    else:
                            logger.info(f"{col}: OK")

                if len(stg1_cols)>0:
                    alerts = [None, None, [stg1_cols]]
                
               
        pretty_alert({block:alerts}, logger)


def pretty_alert(alert_table, logger):
        if (len(alert_table)>0):
                
                stg1_mes = False
                for table_name in alert_table.keys():
                        
                        stat=alert_table[table_name]

                        if stat[0] is not None and not stg1_mes:
                                stg1_mes = True
                                send_slack_notif('Recent date not found!')
                                
                        if stat[0] is not None:
                                table1 = f"{table_name}: {stat[0]}"
                                send_slack_notif(table1)
                                
                                
                stg2_mes = False
                for table_name in alert_table.keys():
                
                        stat=alert_table[table_name]
                
                        if stat[1] is not None and not stg2_mes:
                                stg2_mes = True
                                # logger.info('nOverall_count_share is critical!n')
                                send_slack_notif('nLow day counts!n')
                                
                        
                        if stat[1] is not None:
                                table2 = pd.DataFrame()
                                table2["date"] = [pd.to_datetime(x[0]).strftime("%Y-%m-%d") for x in stat[1].values]
                                table2["filling_rate"] = [round(x[1],2) for x in stat[1].values]
                                table2["column"] = table_name
                                table2[''] = table2["column"].astype(str) + " " + table2["date"].astype(str) + " " + table2["filling_rate"].astype(str)
                                table2 = table2.drop(["date", "filling_rate","column"], axis=1)
                                # logger.info(table2.set_index('').to_markdown(tablefmt="plain"))
                                send_slack_notif(table2.set_index("").to_markdown(tablefmt="plain"))

                stg3_mes = False
                for table_name in alert_table.keys():
                
                        stat=alert_table[table_name]
                        
                        if stat[2] is not None and not stg3_mes:
                                stg3_mes = True
                                send_slack_notif('nLow not_null_share!n')
                                # logger.info('nNot_null_share critical!n')
                        
                        if stat[2] is not None:

                                table3_res = ""

                                if isinstance(stat[2], dict):
                                        coll = []
                                        for key in stat[2].keys():
                                                shell = pd.DataFrame()
                                                shell[["date", "curr2prev"]]=stat[2][key]
                                                shell["date"] = shell["date"].dt.strftime("%Y-%m-%d")
                                                shell['curr2prev']= shell['curr2prev'].round(2)
                                                shell["column"] = key.replace("_not_null_share","")
                                                shell[table_name] = shell["column"].astype(str) + " " + shell["date"].astype(str) + " " + shell["curr2prev"].astype(str)
                                                shell = shell.drop(["date", "curr2prev", "column"],axis=1)
                                                coll.append(shell)
                                        table3=pd.concat(coll)
                                        table3_res+=str(table3.set_index(table_name).to_markdown(tablefmt="plain"))
                                else:
                                        table3 = pd.DataFrame.from_dict(stat[2][0],orient="index")
                                        table3 = table3.rename(columns={0:table_name})
                                        table3=table3.reset_index()
                                        table3 = table3.rename(columns={0:table_name})
                                        table3[table_name] = table3['index'].astype(str) + ' ' + table3[table_name].astype(str)
                                        table3=table3.drop('index',axis=1)
                                        table3_res = str(table3.set_index(table_name).to_markdown(tablefmt="plain"))
                                
                                if table3_res!="":
                                        send_slack_notif(table3_res)
}
