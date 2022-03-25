#!/usr/bin/env python
# coding: utf-8

# # 주배치

# In[1]:


import awswrangler as wr
import pandas
import pymysql
from datetime import datetime, timedelta


import boxing_star_info as info
from job_status import Status
from database_connection import RdsConnection, Athena
from utils import Utils

# # NRU - 신규고객 + 닉네임 생성

# In[2]:


def summary_nru():
    
    print(f"{Utils.dt()} ##### NRU #####")
    
    status = Status()
        
    start_week = status.get_status(info.game_code, "nru", 'week', info.default_date)
    
    end_dt_utc = status.get_status(info.game_code, "member_info", 'cdc', default=info.default_date)
    end_dt = str(datetime.strptime(end_dt_utc.split('.', 1)[0], "%Y-%m-%d %H:%M:%S") + timedelta(hours=info.time_difference))
    end_date = end_dt.split(' ', 1)[0]

    query = f"\
SELECT DATE(DATE_ADD(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour), INTERVAL (DAYOFWEEK(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) * -1 + 1) DAY)) division_week\n\
      ,IFNULL(ie.os_type, 0) os_type\n\
      ,IFNULL(ie.nation_id, '00') country_code\n\
      ,SUM(CASE WHEN mi.member_name != '' THEN 1 ELSE 0 END) nru1\n\
      ,SUM(CASE WHEN mi.member_name = '' THEN 1 ELSE 0 END) nru2\n\
  FROM game.boxing_star_member_info mi\n\
  LEFT JOIN game.boxing_star_member_info_exp ie\n\
    ON ie.member_idx = mi.member_idx\n\
 WHERE DATE(DATE_ADD(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour), INTERVAL (DAYOFWEEK(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) * -1 + 1) DAY)) >= DATE(DATE_ADD(DATE_ADD(STR_TO_DATE('{start_week}', '%Y-%m-%d'), INTERVAL -1 * DAYOFWEEK(STR_TO_DATE('{start_week}', '%Y-%m-%d')) + 1 DAY), INTERVAL 1 WEEK))\n\
   AND DATE(DATE_ADD(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour), INTERVAL (DAYOFWEEK(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) * -1 + 1) DAY)) < DATE(DATE_ADD(now(), INTERVAL (DAYOFWEEK(now()) - 1) * -1 DAY))\n\
 GROUP BY DATE(DATE_ADD(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour), INTERVAL (DAYOFWEEK(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) * -1 + 1) DAY))\n\
      ,IFNULL(ie.os_type, 0)\n\
      ,IFNULL(ie.nation_id, '00')\n\
;\
"
            
    print(query)

    rds_conn = RdsConnection()
    wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb") 

    try:
        rds_df = wr.mysql.read_sql_query(
            query,
            con=wotan_con
        )

    finally:
        wotan_con.close()

    print(rds_df.head())
    print(f">>DataFrame Size : {len(rds_df)}")
    
    if len(rds_df) > 0:
        
        table_name = "nru_week"
        wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb")

        try:
            sql_cleanup = f"\
DELETE\n\
  FROM {table_name}\n\
 WHERE game_code = '{info.game_code}'\n\
   AND division_week >= DATE(DATE_ADD(DATE_ADD(STR_TO_DATE('{start_week}', '%Y-%m-%d'), INTERVAL -1 * DAYOFWEEK(STR_TO_DATE('{start_week}', '%Y-%m-%d')) + 1 DAY), INTERVAL 1 WEEK))\n\
;\
"
            curs = wotan_con.cursor(pymysql.cursors.DictCursor)
            curs.execute(sql_cleanup)
            wotan_con.commit()
        finally:
            wotan_con.close()

        last_dt = rds_df['division_week'].max()

        rds_df.insert(0, "game_code", info.game_code)

        print(rds_df.head())

        wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb") 

        try:
            wr.mysql.to_sql(
                df=rds_df,
                table=table_name,
                mode="upsert_replace_into",
                use_column_names = True,
                schema="wotan",
                con=wotan_con
            )
        finally:
            wotan_con.close()

        # update status
        status.set_status(info.game_code, "nru", 'week', last_dt)
    

# # WAU - 주간 접속자수

# In[3]:


def summary_wau():
    
    print(f"{Utils.dt()} ##### WAU #####")
    
    status = Status()
    
    last_dt = status.get_status(info.game_code, "member_login_log", "dms", info.default_dt)
    last_date = last_dt.split(' ', 1)[0]
    
    start_date = status.get_status(info.game_code, "wau", "week", default=info.default_date)
    start_date = start_date.split(' ', 1)[0]
    
    athena = Athena()
    where_dt = athena.datetime_range_where(start_date, last_date, freq="D")

    query = f"\
SELECT division_login_week\n\
      ,os_type\n\
      ,country_code\n\
      ,MAX(division_login_week) max_login_week\n\
      ,count(*) wau\n\
  FROM (\n\
          SELECT -- DATE(DATE_ADD('day', -1 * day_of_week(ml.login_dt) + 1, ml.login_dt)) division_login_week\n\
                 DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, ml.login_dt)) - 1), ml.login_dt)) division_login_week\n\
                ,ml.os_type\n\
                ,ml.country_code\n\
                ,ml.member_idx\n\
            FROM AwsDataCatalog.wotan_s3.boxing_star_member_login_log  ml\n\
           WHERE DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, ml.login_dt)) - 1), ml.login_dt)) > DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, DATE_PARSE('{start_date}', '%Y-%m-%d'))) - 1), DATE_PARSE('{start_date}', '%Y-%m-%d')))\n\
             AND DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, ml.login_dt)) - 1), ml.login_dt)) < DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, DATE_ADD('hour', {info.time_difference}, now()))) - 1), DATE_ADD('hour', {info.time_difference}, now())))\n\
             AND (\n\
{where_dt}\
                 )\n\
           GROUP BY DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, ml.login_dt)) - 1), ml.login_dt))\n\
                   ,ml.os_type\n\
                   ,ml.country_code\n\
                   ,ml.member_idx\n\
       ) pl\n\
 GROUP BY division_login_week\n\
         ,os_type\n\
         ,country_code\n\
;\
"
    print(query)
    
    athena_database = "wotan_s3"
    athena_df = athena.exeuteQuery2Df(athena_database, query)
    
    athena_df.drop(athena_df.columns[len(athena_df.columns) - 1], axis=1, inplace=True)
    athena_df.dropna(subset = ["division_login_week"], inplace=True)
    print(f">>DataFrame Size : {len(athena_df)}")
    
    if len(athena_df) > 0:
        
        rds_conn = RdsConnection()
        
        table_name = "wau_week"
        wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb")

        try:
            sql_cleanup = f"\
DELETE\n\
  FROM {table_name}\n\
 WHERE game_code = '{info.game_code}'\n\
   AND division_week >= DATE(DATE_ADD(DATE_ADD(STR_TO_DATE('{start_date}', '%Y-%m-%d'), INTERVAL -1 * DAYOFWEEK(STR_TO_DATE('{start_date}', '%Y-%m-%d')) + 1 DAY), INTERVAL 1 WEEK))\n\
;\
"
            curs = wotan_con.cursor(pymysql.cursors.DictCursor)
            curs.execute(sql_cleanup)
            wotan_con.commit()
        finally:
            wotan_con.close()

        bookmark_reg_dt = athena_df["max_login_week"].max()

        athena_df.drop("max_login_week", axis=1, inplace=True)
        athena_df.insert(0, "game_code", info.game_code)
        athena_df.rename(columns={"division_login_week" : "division_week"}, inplace=True)

        print(athena_df.head())
    
        wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb")

        try:
            wr.mysql.to_sql(
                df=athena_df,
                table=table_name,
                mode="upsert_replace_into",
                use_column_names = True,
                schema="wotan",
                con=wotan_con
            )
        finally:
            wotan_con.close()

        # update status
        status.set_status(info.game_code, "wau", "week", bookmark_reg_dt)


# # PU - 주간 구매자 수

# In[4]:


def summary_pu():
    
    print(f"{Utils.dt()} ##### PU #####")
    
    status = Status()
    
    start_week = status.get_status(info.game_code, "pu", "week", default=info.default_date)
    start_week = start_week.split('.', 1)[0]
    
    last_dt = status.get_status(info.game_code, "player_billing_log", "dms", default=info.default_dt)
    last_date = last_dt.split(' ', 1)[0]
    
    athena = Athena()
    where_dt = athena.datetime_range_where(start_week, last_date, freq="D")

    query = f"\
SELECT division_log_week\n\
      ,os_type\n\
      ,country_code\n\
      ,count(*) pu\n\
  FROM (\n\
          SELECT pb.member_idx\n\
                ,pb.os_type\n\
                ,pb.nation_id country_code\n\
                -- ,DATE(DATE_ADD('day', -1 * day_of_week(pb.log_dt) + 1, pb.log_dt)) division_log_week\n\
                ,DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, pb.log_dt)) - 1), pb.log_dt)) division_log_week\n\
                ,ROW_NUMBER() OVER(PARTITION BY pb.member_idx, DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, pb.log_dt)) - 1), pb.log_dt)) ORDER BY DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, pb.log_dt)) - 1), pb.log_dt))) week_seq\n\
            FROM AwsDataCatalog.wotan_s3.boxing_star_player_billing_log pb\n\
           WHERE DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, pb.log_dt)) - 1), pb.log_dt)) > DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, DATE_PARSE('{start_week}', '%Y-%m-%d'))) - 1), DATE_PARSE('{start_week}', '%Y-%m-%d')))\n\
             AND DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, pb.log_dt)) - 1), pb.log_dt)) < DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, DATE_ADD('hour', {info.time_difference}, now()))) - 1), DATE_ADD('hour', {info.time_difference}, now())))\n\
             AND (\n\
{where_dt}\
                 )\n\
           GROUP BY pb.member_idx\n\
                   ,DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, pb.log_dt)) - 1), pb.log_dt))\n\
                   ,pb.os_type\n\
                   ,pb.nation_id\n\
       ) pl\n\
 WHERE week_seq = 1\n\
 GROUP BY division_log_week\n\
         ,os_type\n\
         ,country_code\n\
;\
"
    print(query)
    
    athena_database = "wotan_s3"
    athena_df = athena.exeuteQuery2Df(athena_database, query)
    
    athena_df.drop(athena_df.columns[len(athena_df.columns) - 1], axis=1, inplace=True)
    athena_df.dropna(subset = ["division_log_week"], inplace=True)
    print(f">>DataFrame Size : {len(athena_df)}")
    
    
    if len(athena_df) > 0:
        
        rds_conn = RdsConnection()
        
        table_name = "pu_week"
        wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb")

        try:
            sql_cleanup = f"\
DELETE\n\
  FROM {table_name}\n\
 WHERE game_code = '{info.game_code}'\n\
   AND division_week >= DATE(DATE_ADD(DATE_ADD(STR_TO_DATE('{start_week}', '%Y-%m-%d'), INTERVAL -1 * DAYOFWEEK(STR_TO_DATE('{start_week}', '%Y-%m-%d')) + 1 DAY), INTERVAL 1 WEEK))\n\
;\
"
            curs = wotan_con.cursor(pymysql.cursors.DictCursor)
            curs.execute(sql_cleanup)
            wotan_con.commit()
        finally:
            wotan_con.close()

        bookmark_reg_dt = athena_df["division_log_week"].max()

        athena_df.insert(0, "game_code", info.game_code)
        athena_df.rename(columns={"division_log_week" : "division_week"}, inplace=True)

        print(athena_df.head())

        wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb")

        try:
            wr.mysql.to_sql(
                df=athena_df,
                table=table_name,
                mode="upsert_replace_into",
                use_column_names = True,
                schema="wotan",
                con=wotan_con
            )
        finally:
            wotan_con.close()

        # update status
        status.set_status(info.game_code, "pu", "week", bookmark_reg_dt)


# # ERU - 주간가입 + 가입당일 어디까지 Clear하는지 (사업부기준의 clear)

# In[5]:


def summary_eru():
    
    print(f"{Utils.dt()} ##### ERU #####")
    
    status = Status()
    
    start_week = status.get_status(info.game_code, "eru", "week", default=info.default_date)
    
    last_dt = status.get_status(info.game_code, "tutorial_end_log", "dms", default=info.default_dt)
    last_date = last_dt.split(' ', 1)[0]
    
    athena = Athena()
    where_dt = athena.datetime_range_where(start_week, last_date, freq="D")

    query = f"\
SELECT -- DATE(DATE_ADD('day', -1 * day_of_week(te.log_dt) + 1, te.log_dt)) division_week\n\
       DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, te.log_dt)) - 1), te.log_dt)) division_week\n\
      ,te.os_type\n\
      ,te.nation_id country_code\n\
      ,count(*) eru\n\
  FROM AwsDataCatalog.wotan_s3.boxing_star_tutorial_end_log te\n\
 INNER JOIN (\n\
          SELECT mi.member_idx\n\
                ,date_add('hour', {info.time_difference}, mi.reg_date) reg_date\n\
            FROM wotan_rds.game.boxing_star_member_info mi\n\
           WHERE DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, date_add('hour', {info.time_difference}, mi.reg_date))) - 1), date_add('hour', {info.time_difference}, mi.reg_date))) > DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, DATE_PARSE('{start_week}', '%Y-%m-%d'))) - 1), DATE_PARSE('{start_week}', '%Y-%m-%d')))\n\
             AND DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, date_add('hour', {info.time_difference}, mi.reg_date))) - 1), date_add('hour', {info.time_difference}, mi.reg_date))) < DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, DATE_ADD('hour', {info.time_difference}, now()))) - 1), DATE_ADD('hour', {info.time_difference}, now())))\n\
       ) mi\n\
    ON mi.member_idx = te.member_idx\n\
   AND DATE(mi.reg_date) = DATE(te.log_dt)\n\
 WHERE DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, te.log_dt)) - 1), te.log_dt)) > DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, DATE_PARSE('{start_week}', '%Y-%m-%d'))) - 1), DATE_PARSE('{start_week}', '%Y-%m-%d')))\n\
   AND DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, te.log_dt)) - 1), te.log_dt)) < DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, DATE_ADD('hour', {info.time_difference}, now()))) - 1), DATE_ADD('hour', {info.time_difference}, now())))\n\
   AND DATE(mi.reg_date) = DATE(te.log_dt)\n\
   AND (\n\
{where_dt}\
       )\n\
 GROUP BY DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, te.log_dt)) - 1), te.log_dt))\n\
      ,te.os_type\n\
      ,te.nation_id\n\
;\
"

    print(query)

    athena_database = "wotan_s3"
    athena_df = athena.exeuteQuery2Df(athena_database, query)
    
    print(athena_df.head())
    
    athena_df.drop(athena_df.columns[len(athena_df.columns) - 1], axis=1, inplace=True)
    athena_df.dropna(subset = ["division_week"], inplace=True)
    print(f">>DataFrame Size : {len(athena_df)}")
    
    if len(athena_df) > 0:
        
        rds_conn = RdsConnection()
        
        table_name = "eru_week"
        wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb")

        try:
            sql_cleanup = f"\
DELETE\n\
  FROM {table_name}\n\
 WHERE game_code = '{info.game_code}'\n\
   AND division_week >= DATE(DATE_ADD(DATE_ADD(STR_TO_DATE('{start_week}', '%Y-%m-%d'), INTERVAL -1 * DAYOFWEEK(STR_TO_DATE('{start_week}', '%Y-%m-%d')) + 1 DAY), INTERVAL 1 WEEK))\n\
;\
"
            curs = wotan_con.cursor(pymysql.cursors.DictCursor)
            curs.execute(sql_cleanup)
            wotan_con.commit()
        finally:
            wotan_con.close()

        bookmark_log_dt = athena_df["division_week"].max()

        athena_df.insert(0, "game_code", info.game_code)

        print(athena_df.head())

        wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb") 

        try:
            wr.mysql.to_sql(
                df=athena_df,
                table=table_name,
                mode="upsert_replace_into",
                use_column_names = True,
                schema="wotan",
                con=wotan_con
            )
        finally:
            wotan_con.close()

        # update status
        status.set_status(info.game_code, "eru", 'week', bookmark_log_dt)
    

# # RAU - 귀환고객

# In[6]:


def summary_rau():
    
    print(f"{Utils.dt()} ##### RAU #####")
    
    status = Status()
    
    start_week = status.get_status(info.game_code, "rau", "week", default=info.default_date)
    start_week = start_week.split('.', 1)[0]
    
    end_date = status.get_status(info.game_code, "rau", "day", default=info.default_date)
    
    query = f"\
SELECT -- DATE(DATE_ADD(rd.division_date, INTERVAL (WEEKDAY(rd.division_date) * -1) DAY)) division_week\n\
       DATE(DATE_ADD(rd.division_date, INTERVAL (DAYOFWEEK(rd.division_date) * -1 + 1) DAY)) division_week\n\
      ,rd.os_type\n\
      ,rd.country_code\n\
      ,SUM(rd.rau) rau\n\
  FROM rau_day rd\n\
 WHERE rd.game_code = '{info.game_code}'\n\
   AND DATE(DATE_ADD(rd.division_date, INTERVAL (DAYOFWEEK(rd.division_date) * -1 + 1) DAY)) >= DATE(DATE_ADD(DATE_ADD(STR_TO_DATE('{start_week}', '%Y-%m-%d'), INTERVAL -1 * DAYOFWEEK(STR_TO_DATE('{start_week}', '%Y-%m-%d')) + 1 DAY), INTERVAL 1 WEEK))\n\
   AND DATE(DATE_ADD(rd.division_date, INTERVAL (DAYOFWEEK(rd.division_date) * -1 + 1) DAY)) < DATE(DATE_ADD(now(), INTERVAL ((DAYOFWEEK(now()) - 1) * -1) DAY))\n\
 GROUP BY DATE(DATE_ADD(rd.division_date, INTERVAL (DAYOFWEEK(rd.division_date) * -1 + 1) DAY))\n\
      ,rd.os_type\n\
      ,rd.country_code\n\
;\
"
    print(query)
    
    rds_conn = RdsConnection()
    wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb")
    
    try:
        rds_df = wr.mysql.read_sql_query(
            query,
            con=wotan_con
        )
    finally:
        wotan_con.close()
    
    print(f">>DataFrame Size : {len(rds_df)}")
    
    if len(rds_df) > 0:
        
        table_name = "rau_week"
        wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb")

        try:
            sql_cleanup = f"\
DELETE\n\
  FROM {table_name}\n\
 WHERE game_code = '{info.game_code}'\n\
   AND division_week >= DATE(DATE_ADD(DATE_ADD(STR_TO_DATE('{start_week}', '%Y-%m-%d'), INTERVAL -1 * DAYOFWEEK(STR_TO_DATE('{start_week}', '%Y-%m-%d')) + 1 DAY), INTERVAL 1 WEEK))\n\
;\
"
            curs = wotan_con.cursor(pymysql.cursors.DictCursor)
            curs.execute(sql_cleanup)
            wotan_con.commit()
        finally:
            wotan_con.close()

        bookmark_date = rds_df["division_week"].max()
        rds_df.insert(0, "game_code", info.game_code)

        print(rds_df.head())

        wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb")
    
        try:
            wr.mysql.to_sql(
                df=rds_df,
                table=table_name,
                mode="upsert_replace_into",
                use_column_names = True,
                schema="wotan",
                con=wotan_con
            )
        finally:
            wotan_con.close()
        
        # update status
        status.set_status(info.game_code, "rau", "week", bookmark_date)


# # 주간 Sales

# In[7]:


def summary_sales():
    
    print(f"{Utils.dt()} ##### Sales #####")
    
    status = Status()
    
    start_week = status.get_status(info.game_code, "sales", "week", default=info.default_date)
    
    last_dt = status.get_status(info.game_code, "player_billing_log", "dms", default=info.default_dt)
    last_date = last_dt.split(' ', 1)[0]
    
    athena = Athena()
    where_dt = athena.datetime_range_where(start_week, last_date, freq="D")

    query = f"\
SELECT division_log_week\n\
      ,os_type\n\
      ,country_code\n\
      ,market\n\
      ,product_idx\n\
      ,count(*) user_count\n\
      ,SUM(sales_count) sales_count\n\
      ,SUM(real_cash_kr) real_cash_kr\n\
  FROM (\n\
          SELECT -- DATE(DATE_ADD('day', -1 * day_of_week(pb.log_dt) + 1, pb.log_dt)) division_log_week\n\
                 DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, pb.log_dt)) - 1), pb.log_dt)) division_log_week\n\
                ,pb.os_type\n\
                ,pb.nation_id country_code\n\
                ,pb.market_type market\n\
                ,pb.product_id product_idx\n\
                ,count(*) sales_count\n\
                ,SUM(pb.real_cash_kr) real_cash_kr\n\
            FROM AwsDataCatalog.wotan_s3.boxing_star_player_billing_log pb\n\
           WHERE DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, pb.log_dt)) - 1), pb.log_dt)) > DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, DATE_PARSE('{start_week}', '%Y-%m-%d'))) - 1), DATE_PARSE('{start_week}', '%Y-%m-%d')))\n\
             AND DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, pb.log_dt)) - 1), pb.log_dt)) < DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, DATE_ADD('hour', {info.time_difference}, now()))) - 1), DATE_ADD('hour', {info.time_difference}, now())))\n\
             AND (\n\
{where_dt}\
                 )\n\
           GROUP BY DATE(DATE_ADD('day', -1 * (day_of_week(DATE_ADD('day', 1, pb.log_dt)) - 1), pb.log_dt))\n\
                   ,pb.os_type\n\
                   ,pb.nation_id\n\
                   ,pb.market_type\n\
                   ,pb.product_id\n\
       ) pb1\n\
 GROUP BY division_log_week\n\
      ,os_type\n\
      ,country_code\n\
      ,market\n\
      ,product_idx\n\
;\
"
    print(query)
    
    athena_database = "wotan_s3"
    athena_df = athena.exeuteQuery2Df(athena_database, query)
    
    athena_df.drop(athena_df.columns[len(athena_df.columns) - 1], axis=1, inplace=True)
    athena_df.dropna(subset = ["division_log_week"], inplace=True)
    print(f">>DataFrame Size : {len(athena_df)}")
    
    if len(athena_df) > 0:
        
        rds_conn = RdsConnection()
        
        table_name = "sales_week"
        wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb")

        try:
            sql_cleanup = f"\
DELETE\n\
  FROM {table_name}\n\
 WHERE game_code = '{info.game_code}'\n\
   AND division_week >= DATE(DATE_ADD(DATE_ADD(STR_TO_DATE('{start_week}', '%Y-%m-%d'), INTERVAL -1 * DAYOFWEEK(STR_TO_DATE('{start_week}', '%Y-%m-%d')) + 1 DAY), INTERVAL 1 WEEK))\n\
;\
"
            curs = wotan_con.cursor(pymysql.cursors.DictCursor)
            curs.execute(sql_cleanup)
            wotan_con.commit()
        finally:
            wotan_con.close()

        bookmark_log_dt = athena_df["division_log_week"].max()

        athena_df.insert(0, "game_code", info.game_code)
        athena_df.rename(columns={"division_log_week" : "division_week"}, inplace=True)

        print(athena_df.head())
    
        wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb")

        try:
            wr.mysql.to_sql(
                df=athena_df,
                table=table_name,
                mode="upsert_replace_into",
                use_column_names = True,
                schema="wotan",
                con=wotan_con
            )
        finally:
            wotan_con.close()

        # update status
        status.set_status(info.game_code, "sales", "week", bookmark_log_dt)


# # Run Function

# In[8]:


def run():
    
    summary_nru()
    summary_wau()
    summary_pu()
    summary_eru()
    summary_rau()
    summary_sales()
    
    print("\n\n\n### JOB END. ###")

# In[9]:


if __name__=="__main__":
    run()
