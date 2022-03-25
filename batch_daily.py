#!/usr/bin/env python
# coding: utf-8

# # 일배치

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
        
    start_date = status.get_status(info.game_code, "nru", 'day', default=info.default_date)
    end_dt = status.get_status(info.game_code, "member_info", 'cdc', default=info.default_date)
    end_date = end_dt.split(' ', 1)[0]

    query = f"\
SELECT DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) division_date\n\
      ,IFNULL(ie.os_type, 0) os_type\n\
      ,IFNULL(ie.nation_id, '00') country_code\n\
      ,SUM(CASE WHEN mi.member_name != '' THEN 1 ELSE 0 END) nru1\n\
      ,SUM(CASE WHEN mi.member_name = '' THEN 1 ELSE 0 END) nru2\n\
  FROM game.boxing_star_member_info mi\n\
 INNER JOIN game.boxing_star_member_info_exp ie\n\
    ON ie.member_idx = mi.member_idx\n\
 WHERE DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) > DATE(DATE_ADD(STR_TO_DATE('{start_date}', '%Y-%m-%d'), INTERVAL -{info.before_days} DAY))\n\
   AND DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) < DATE(now())\n\
 GROUP BY DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour))\n\
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
        
        table_name = "nru_day"
        rds_conn.cleanup_day(table_name, start_date)

        last_dt = rds_df['division_date'].max()

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
        status.set_status(info.game_code, "nru", 'day', last_dt)
        
        
###################################################################################
        print(f"{Utils.dt()} ##### NRU DATA BACKUP #####")
    
        query = f"\
SELECT DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) division_date\n\
      ,mi.member_idx\n\
      ,IFNULL(ie.os_type, 0) os_type\n\
      ,IFNULL(ie.nation_id, '00') country_code\n\
      ,mi.member_name\n\
  FROM game.boxing_star_member_info mi\n\
 INNER JOIN game.boxing_star_member_info_exp ie\n\
    ON ie.member_idx = mi.member_idx\n\
 WHERE DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) > DATE(DATE_ADD(STR_TO_DATE('{start_date}', '%Y-%m-%d'), INTERVAL -{info.before_days} DAY))\n\
   AND DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) < DATE(now())\n\
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
        
        backup_s3_path = f"s3://wotan-data/boxing-star/nru_backup/{start_date}"

        backup_s3_nru = wr.s3.to_csv(
            df=rds_df,
            path=backup_s3_path,
            dataset=True,
            mode="overwrite"
        )

        print(len(backup_s3_nru["paths"]))

# # DAU - 당일 접속자수

# In[3]:


def summary_dau():
    
    print(f"{Utils.dt()} ##### DAU #####")
    
    status = Status()
    
    start_date = status.get_status(info.game_code, "dau", "day", default=info.default_date)
    start_date = start_date.split('.', 1)[0]
    
    last_dt = status.get_status(info.game_code, "member_login_log", "dms", default=info.default_dt)
    last_date = last_dt.split(' ', 1)[0]
    
    athena = Athena()
    where_dt = athena.datetime_range_where(start_date, last_date, freq="D")
    
    start_month_date = str(datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=info.before_days))
    where_month = athena.datetime_range_where(start_month_date, last_dt, freq="MS")

#     query = f"\
# SELECT division_login_date\n\
#       ,os_type\n\
#       ,country_code\n\
#       -- ,MAX(division_login_date) max_login_date\n\
#       ,count(*) dau\n\
#   FROM (\n\
#           SELECT DATE(ml.login_dt) division_login_date\n\
#                 ,COALESCE(me.os_type, ml.os_type) os_type\n\
#                 ,COALESCE(me.nation_id, ml.country_code) country_code\n\
#                 ,ml.member_idx\n\
#             FROM AwsDataCatalog.wotan_s3.boxing_star_member_login_log  ml\n\
#             LEFT JOIN wotan_rds.game.boxing_star_member_info_exp me\n\
#               ON me.member_idx = ml.member_idx\n\
#            WHERE DATE(ml.login_dt) > DATE_ADD('day', -{info.before_days}, DATE_PARSE('{start_date}', '%Y-%m-%d'))\n\
#              AND DATE(ml.login_dt) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
#              AND login_date_order_no = 1\n\
#              AND (\n\
# {where_dt}\
#                  )\n\
#            GROUP BY DATE(ml.login_dt)\n\
#                    ,COALESCE(me.os_type, ml.os_type)\n\
#                    ,COALESCE(me.nation_id, ml.country_code)\n\
#                    ,ml.member_idx\n\
#        ) pl\n\
#  GROUP BY division_login_date\n\
#          ,os_type\n\
#          ,country_code\n\
# ;\
# "

    query = f"\
SELECT division_login_date\n\
      ,os_type\n\
      ,country_code\n\
      ,dau\n\
      ,sum_month_dau\n\
  FROM (\n\
           SELECT division_login_date\n\
                 ,os_type\n\
                 ,country_code\n\
                 ,SUM(CASE WHEN division_login_date = login_date THEN 1 ELSE 0 END) dau\n\
                 ,SUM(CASE WHEN login_date IS NOT NULL THEN 1 ELSE 0 END) sum_month_dau\n\
             FROM (\n\
                      SELECT dv.division_login_date\n\
                            ,MAX(pl.login_date) login_date\n\
                            ,dv.os_type\n\
                            ,dv.country_code\n\
                            ,pl.member_idx\n\
                        FROM (\n\
                                 SELECT division_login_date\n\
                                       ,os_type\n\
                                       ,country_code\n\
                                   FROM (\n\
                                            SELECT DATE(ml.login_dt) division_login_date\n\
                                              FROM AwsDataCatalog.wotan_s3.boxing_star_member_login_log ml\n\
                                             WHERE DATE(ml.login_dt) > DATE_ADD('day', -{info.before_days}, DATE_PARSE('{start_date}', '%Y-%m-%d'))\n\
                                               AND DATE(ml.login_dt) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
                                               AND login_date_order_no = 1\n\
                                               AND (\n\
{where_dt}\
                                                   )\n\
                                             GROUP BY DATE(ml.login_dt)\n\
                                        ) da\n\
                                  INNER JOIN (\n\
                                            SELECT COALESCE(me.os_type, ml.os_type) os_type\n\
                                                  ,COALESCE(me.nation_id, ml.country_code) country_code\n\
                                              FROM AwsDataCatalog.wotan_s3.boxing_star_member_login_log ml\n\
                                             INNER JOIN wotan_rds.game.boxing_star_member_info_exp me\n\
                                                ON me.member_idx = ml.member_idx\n\
                                             WHERE DATE(ml.login_dt) >= DATE(CONCAT(DATE_FORMAT(DATE_ADD('day', -{info.before_days}, DATE_PARSE('{start_date}', '%Y-%m-%d')), '%Y-%m'), '-01'))\n\
                                               AND DATE(ml.login_dt) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
                                               AND login_date_order_no = 1\n\
                                               AND (\n\
{where_month}\
                                                   )\n\
                                             GROUP BY COALESCE(me.os_type, ml.os_type)\n\
                                                  ,COALESCE(me.nation_id, ml.country_code)\n\
                                        ) oc\n\
                                     ON 1 = 1\n\
                             ) dv\n\
                        LEFT JOIN (\n\
                                 SELECT DATE(ml.login_dt) login_date\n\
                                       ,COALESCE(me.os_type, ml.os_type) os_type\n\
                                       ,COALESCE(me.nation_id, ml.country_code) country_code\n\
                                       ,ml.member_idx\n\
                                   FROM AwsDataCatalog.wotan_s3.boxing_star_member_login_log ml\n\
                                  INNER JOIN wotan_rds.game.boxing_star_member_info_exp me\n\
                                     ON me.member_idx = ml.member_idx\n\
                                  WHERE DATE(ml.login_dt) >= DATE(CONCAT(DATE_FORMAT(DATE_ADD('day', -{info.before_days}, DATE_PARSE('{start_date}', '%Y-%m-%d')), '%Y-%m'), '-01'))\n\
                                    AND DATE(ml.login_dt) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
                                    AND login_date_order_no = 1\n\
                                    AND (\n\
{where_month}\
                                        )\n\
                                  GROUP BY DATE(ml.login_dt)\n\
                                       ,COALESCE(me.os_type, ml.os_type)\n\
                                       ,COALESCE(me.nation_id, ml.country_code)\n\
                                       ,ml.member_idx\n\
                             ) pl\n\
                          ON DATE_FORMAT(pl.login_date, '%Y-%m') = DATE_FORMAT(dv.division_login_date, '%Y-%m')\n\
                         AND pl.login_date <= dv.division_login_date\n\
                         AND pl.os_type = dv.os_type\n\
                         AND pl.country_code = dv.country_code\n\
                       GROUP BY dv.division_login_date\n\
                            ,dv.os_type\n\
                            ,dv.country_code\n\
                            ,pl.member_idx\n\
                  ) pl\n\
            GROUP BY division_login_date\n\
                 ,os_type\n\
                 ,country_code\n\
       ) pl\n\
 WHERE sum_month_dau > 0\n\
;\
"

    print(query)
    
    athena_database = "wotan_s3"
    athena_df = athena.exeuteQuery2Df(athena_database, query)
    
    athena_df.drop(athena_df.columns[len(athena_df.columns) - 1], axis=1, inplace=True)
    athena_df.dropna(subset = ["division_login_date"], inplace=True)
    print(f">>DataFrame Size : {len(athena_df)}")
    
    if len(athena_df) > 0:
        
        rds_conn = RdsConnection()
        
        table_name = "dau_day"
        rds_conn.cleanup_day(table_name, start_date)

        bookmark_reg_dt = athena_df["division_login_date"].max()

#         athena_df.drop("max_login_date", axis=1, inplace=True)
        athena_df.insert(0, "game_code", info.game_code)
        athena_df.rename(columns={"division_login_date" : "division_date"}, inplace=True)

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
        status.set_status(info.game_code, "dau", "day", bookmark_reg_dt)
        
###################################################################################
        print(f"{Utils.dt()} ##### DAU DATA BACKUP #####")
    
        backup_s3_path = f"s3://wotan-data/boxing-star/dau_backup/{start_date}"

        backup_s3_nru = wr.s3.to_csv(
            df=athena_df,
            path=backup_s3_path,
            dataset=True,
            mode="overwrite"
        )

        print(len(backup_s3_nru["paths"]))


# # PU - 당일 구매자 수

# In[4]:


def summary_pu():
    
    print(f"{Utils.dt()} ##### PU #####")
    
    status = Status()
    
    start_date = status.get_status(info.game_code, "pu", "day", default=info.default_date)
    start_date = start_date.split('.', 1)[0]
    
    last_dt = status.get_status(info.game_code, "player_billing_log", "dms", default=info.default_dt)
    last_date = last_dt.split(' ', 1)[0]
    
    athena = Athena()
    where_dt = athena.datetime_range_where(start_date, last_date, freq="D")
    
    start_month_date = str(datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=info.before_days))
    where_month = athena.datetime_range_where(start_month_date, last_dt, freq="MS")

#     query = f"\
# SELECT division_log_date\n\
#       ,os_type\n\
#       ,country_code\n\
#       ,count(*) pu\n\
#   FROM (\n\
#           SELECT pb.member_idx\n\
#                 ,pb.os_type\n\
#                 ,pb.nation_id country_code\n\
#                 ,DATE(pb.log_dt) division_log_date\n\
#             FROM AwsDataCatalog.wotan_s3.boxing_star_player_billing_log pb\n\
#            WHERE DATE(pb.log_dt) > DATE_ADD('day', -{info.before_days}, DATE_PARSE('{start_date}', '%Y-%m-%d'))\n\
#              AND DATE(pb.log_dt) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
#              AND (\n\
# {where_dt}\
#                  )\n\
#            GROUP BY pb.member_idx\n\
#                    ,DATE(pb.log_dt)\n\
#                    ,pb.os_type\n\
#                    ,pb.nation_id\n\
#        ) pl\n\
#  GROUP BY division_log_date\n\
#          ,os_type\n\
#          ,country_code\n\
# ;\
# "

    query = f"\
SELECT division_log_date\n\
      ,os_type\n\
      ,country_code\n\
      ,pu\n\
      ,sum_month_pu\n\
  FROM (\n\
           SELECT division_log_date\n\
                 ,os_type\n\
                 ,country_code\n\
                 ,SUM(CASE WHEN division_log_date = log_date THEN 1 ELSE 0 END) pu\n\
                 ,SUM(CASE WHEN log_date IS NOT NULL THEN 1 ELSE 0 END) sum_month_pu\n\
             FROM (\n\
                      SELECT dv.division_log_date\n\
                            ,MAX(pb.log_date) log_date\n\
                            ,dv.os_type\n\
                            ,dv.country_code\n\
                            ,pb.member_idx\n\
                        FROM (\n\
                                 SELECT division_log_date\n\
                                       ,os_type\n\
                                       ,country_code\n\
                                   FROM (\n\
                                            SELECT DATE(pb.log_dt) division_log_date\n\
                                              FROM AwsDataCatalog.wotan_s3.boxing_star_player_billing_log pb\n\
                                             WHERE DATE(pb.log_dt) > DATE_ADD('day', -{info.before_days}, DATE_PARSE('{start_date}', '%Y-%m-%d'))\n\
                                               AND DATE(pb.log_dt) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
                                               AND (\n\
{where_dt}\
                                                   )\n\
                                             GROUP BY DATE(pb.log_dt)\n\
                                        ) ld\n\
                                  INNER JOIN (\n\
                                            SELECT me.os_type\n\
                                                  ,me.nation_id country_code\n\
                                              FROM wotan_rds.game.boxing_star_member_info_exp me\n\
                                             GROUP BY me.os_type\n\
                                                  ,me.nation_id\n\
                                        ) oc\n\
                                     ON 1 = 1\n\
                             ) dv\n\
                        LEFT JOIN (\n\
                                 SELECT pb.member_idx\n\
                                       ,me.os_type\n\
                                       ,me.nation_id country_code\n\
                                       ,DATE(pb.log_dt) log_date\n\
                                       ,ROW_NUMBER() OVER(PARTITION BY pb.member_idx, date(pb.log_dt) ORDER BY date(pb.log_dt)) day_seq\n\
                                   FROM AwsDataCatalog.wotan_s3.boxing_star_player_billing_log pb\n\
                                  INNER JOIN wotan_rds.game.boxing_star_member_info_exp me\n\
                                     ON me.member_idx = pb.member_idx\n\
                                  WHERE DATE(pb.log_dt) >= DATE(CONCAT(DATE_FORMAT(DATE_ADD('day', -{info.before_days}, DATE_PARSE('{start_date}', '%Y-%m-%d')), '%Y-%m'), '-01'))\n\
                                    AND DATE(pb.log_dt) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
                                               AND (\n\
{where_month}\
                                                   )\n\
                                  GROUP BY pb.member_idx\n\
                                          ,DATE(pb.log_dt)\n\
                                          ,me.os_type\n\
                                          ,me.nation_id\n\
                             ) pb\n\
                          ON DATE_FORMAT(pb.log_date, '%Y-%m') = DATE_FORMAT(dv.division_log_date, '%Y-%m')\n\
                         AND pb.log_date <= dv.division_log_date\n\
                         AND pb.os_type = dv.os_type\n\
                         AND pb.country_code = dv.country_code\n\
                         AND pb.day_seq = 1\n\
                       GROUP BY dv.division_log_date\n\
                            ,dv.os_type\n\
                            ,dv.country_code\n\
                            ,pb.member_idx\n\
                  ) pl\n\
            GROUP BY division_log_date\n\
                 ,os_type\n\
                 ,country_code\n\
       ) pl\n\
 WHERE sum_month_pu > 0\n\
;\
"

    print(query)
    
    athena_database = "wotan_s3"
    athena_df = athena.exeuteQuery2Df(athena_database, query)
    
    athena_df.drop(athena_df.columns[len(athena_df.columns) - 1], axis=1, inplace=True)
    athena_df.dropna(subset = ["division_log_date"], inplace=True)
    print(f">>DataFrame Size : {len(athena_df)}")
    
    if len(athena_df) > 0:
        
        rds_conn = RdsConnection()
        
        table_name = "pu_day"
        rds_conn.cleanup_day(table_name, start_date)

        bookmark_reg_dt = athena_df["division_log_date"].max()

        athena_df.insert(0, "game_code", info.game_code)
        athena_df.rename(columns={"division_log_date" : "division_date"}, inplace=True)

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
        status.set_status(info.game_code, "pu", "day", bookmark_reg_dt)


# # ERU - 당일가입 + 당일 어디까지 Clear하는지 (사업부기준의 clear)

# In[5]:


def summary_eru():
    
    print(f"{Utils.dt()} ##### ERU #####")
    
    status = Status()
        
    start_date = status.get_status(info.game_code, "eru", 'day', default=info.default_date)
    
    last_dt = status.get_status(info.game_code, "tutorial_end_log", "dms", default=info.default_dt)
    last_date = last_dt.split(' ', 1)[0]
    
    athena = Athena()
    where_dt = athena.datetime_range_where(start_date, last_date, freq="D")

    query = f"\
SELECT division_date\n\
      ,os_type\n\
      ,country_code\n\
      ,count(*) eru\n\
  FROM (\n\
           SELECT DATE(te.log_dt) division_date\n\
                 ,te.member_idx\n\
                 ,mi.os_type\n\
                 ,mi.country_code\n\
             FROM AwsDataCatalog.wotan_s3.boxing_star_tutorial_end_log te\n\
            INNER JOIN (\n\
                     SELECT mi.member_idx\n\
                           ,me.os_type\n\
                           ,me.nation_id country_code\n\
                           ,DATE_ADD('hour', {info.time_difference}, mi.reg_date) reg_date\n\
                        FROM wotan_rds.game.boxing_star_member_info mi\n\
                       INNER JOIN wotan_rds.game.boxing_star_member_info_exp me\n\
                          ON me.member_idx = mi.member_idx\n\
                       WHERE DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date)) > DATE_ADD('day', -{info.before_days}, DATE_PARSE('{start_date}', '%Y-%m-%d'))\n\
                         AND DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date)) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
                  ) mi\n\
               ON mi.member_idx = te.member_idx\n\
              AND DATE(mi.reg_date) = DATE(te.log_dt)\n\
            WHERE DATE(te.log_dt) > DATE_ADD('day', -{info.before_days}, DATE_PARSE('{start_date}', '%Y-%m-%d'))\n\
              AND DATE(te.log_dt) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
              AND (\n\
{where_dt}\
                  )\n\
            GROUP BY DATE(te.log_dt)\n\
                 ,te.member_idx\n\
                 ,mi.os_type\n\
                 ,mi.country_code\n\
       ) er\n\
 GROUP BY division_date\n\
      ,os_type\n\
      ,country_code\n\
;\
"

    print(query)

    athena_database = "wotan_s3"
    athena_df = athena.exeuteQuery2Df(athena_database, query)
    
    athena_df.drop(athena_df.columns[len(athena_df.columns) - 1], axis=1, inplace=True)
    athena_df.dropna(subset = ["division_date"], inplace=True)
    print(f">>DataFrame Size : {len(athena_df)}")
    
    if len(athena_df) > 0:
        
        rds_conn = RdsConnection()
        
        table_name = "eru_day"
        rds_conn.cleanup_day(table_name, start_date)

        bookmark_log_dt = athena_df["division_date"].max()

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
        status.set_status(info.game_code, "eru", 'day', bookmark_log_dt)
    

# # RAU - 귀환고객

# In[6]:


def summary_rau():
    
    print(f"{Utils.dt()} ##### RAU #####")
    
    status = Status()
    
    start_date = status.get_status(info.game_code, "rau", "day", default=info.default_date)
    
    last_dt = status.get_status(info.game_code, "member_login_log", "dms", default=info.default_dt)
    last_date = last_dt.split(' ', 1)[0]
    
    athena = Athena()
    where_dt = athena.datetime_range_where(start_date, last_date, freq="D")
    
    if start_date != info.default_date:
        start_before_date = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=9)).strftime("%Y-%m-%d")
    else:
        start_before_date = start_date
    
    print(f"start_before_date : {start_before_date}")
    print(f"last_dt : {last_dt}")
    where_before_dt = athena.datetime_range_where(start_before_date, last_dt, freq="D")

    query = f"\
SELECT division_date\n\
      ,COALESCE(ie.os_type, 0) os_type\n\
      ,COALESCE(ie.nation_id, '00') country_code\n\
      ,count(*) rau\n\
  FROM (\n\
           SELECT DATE(login_dt) division_date\n\
                 ,ml.member_idx\n\
             FROM AwsDataCatalog.wotan_s3.boxing_star_member_login_log ml\n\
            INNER JOIN wotan_rds.game.boxing_star_member_info me\n\
               ON me.member_idx = ml.member_idx\n\
              AND DATE(DATE_ADD('hour', {info.time_difference}, me.reg_date)) < DATE_ADD('day', -7, DATE(ml.login_dt))\n\
            WHERE login_date_order_no = 1\n\
              AND DATE(login_dt) > DATE(DATE_ADD('day', -1 * {info.before_days}, DATE('{start_date}')))\n\
              AND DATE(login_dt) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
              AND (\n\
{where_dt}\
                  )\n\
              AND NOT EXISTS (\n\
                      SELECT *\n\
                        FROM AwsDataCatalog.wotan_s3.boxing_star_member_login_log ml1\n\
                       WHERE ml1.login_date_order_no = 1\n\
                         AND ml1.member_idx = ml.member_idx\n\
                         AND DATE(ml1.login_dt) BETWEEN DATE_ADD('day', -7, DATE(ml.login_dt)) AND DATE_ADD('day', -1, DATE(ml.login_dt))\n\
                         AND (\n\
{where_before_dt}\
                             )\n\
                  )\n\
            GROUP BY DATE(login_dt)\n\
                 ,ml.member_idx\n\
       ) me\n\
  LEFT JOIN wotan_rds.game.boxing_star_member_info_exp ie\n\
    ON me.member_idx = ie.member_idx\n\
 GROUP BY division_date\n\
      ,COALESCE(ie.os_type, 0)\n\
      ,COALESCE(ie.nation_id, '00')\n\
;\
"

    print(query)
    
    athena_database = "wotan_s3"
    athena_df = athena.exeuteQuery2Df(athena_database, query)
    
    athena_df.drop(athena_df.columns[len(athena_df.columns) - 1], axis=1, inplace=True)
    athena_df.dropna(subset = ["division_date"], inplace=True)
    print(f">>DataFrame Size : {len(athena_df)}")
    
    if len(athena_df) > 0:
        
        rds_conn = RdsConnection()
        
        table_name = "rau_day"
        rds_conn.cleanup_day(table_name, start_date)

        bookmark_reg_dt = athena_df["division_date"].max()

        athena_df.insert(0, "game_code", info.game_code)
#         athena_df.rename(columns={"division_login_date" : "division_date"}, inplace=True)

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
        status.set_status(info.game_code, "rau", "day", bookmark_reg_dt)


# # 일간 Sales

# In[7]:


def summary_sales():
    
    print(f"{Utils.dt()} ##### Sales #####")
    
    status = Status()
    
    start_date = status.get_status(info.game_code, "sales", "day", default=info.default_date)
    
    last_dt = status.get_status(info.game_code, "player_billing_log", "dms", default=info.default_dt)
    last_date = last_dt.split(' ', 1)[0]
    
    athena = Athena()
    where_dt = athena.datetime_range_where(start_date, last_date, freq="D")

    query = f"\
SELECT division_log_dt\n\
      ,os_type\n\
      ,country_code\n\
      ,market\n\
      ,product_idx\n\
      ,count(*) user_count\n\
      ,SUM(sales_count) sales_count\n\
      ,SUM(real_cash_kr) real_cash_kr\n\
  FROM (\n\
          SELECT DATE(pb.log_dt) division_log_dt\n\
                ,pb.member_idx\n\
                ,pb.os_type\n\
                ,pb.nation_id country_code\n\
                ,pb.market_type market\n\
                ,pb.product_id product_idx\n\
                ,count(*) sales_count\n\
                ,SUM(pb.real_cash_kr) real_cash_kr\n\
            FROM AwsDataCatalog.wotan_s3.boxing_star_player_billing_log pb\n\
           WHERE DATE(pb.log_dt) > DATE_ADD('day', -{info.before_days}, DATE_PARSE('{start_date}', '%Y-%m-%d'))\n\
             AND DATE(pb.log_dt) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
             AND (\n\
{where_dt}\
                 )\n\
           GROUP BY DATE(pb.log_dt)\n\
                   ,pb.member_idx\n\
                   ,pb.os_type\n\
                   ,pb.nation_id\n\
                   ,pb.market_type\n\
                   ,pb.product_id\n\
       ) pb1\n\
 GROUP BY division_log_dt\n\
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
    athena_df.dropna(subset = ["division_log_dt"], inplace=True)
    print(f">>DataFrame Size : {len(athena_df)}")
    
    if len(athena_df) > 0:
        
        rds_conn = RdsConnection()
        
        table_name = "sales_day"
        rds_conn.cleanup_day(table_name, start_date)

        bookmark_log_dt = athena_df["division_log_dt"].max()

        athena_df.insert(0, "game_code", info.game_code)
        athena_df.rename(columns={"division_log_dt" : "division_date"}, inplace=True)

        print(athena_df.head())

        wotan_con = rds_conn.get_mysql_connection("wotan/rds/wotandb")

        try:
            wr.mysql.to_sql (
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
        status.set_status(info.game_code, "sales", "day", bookmark_log_dt)


# # RU - 누적고객

# In[8]:


def summary_ru():
    
    print(f"{Utils.dt()} ##### RU #####")
    
    status = Status()
        
    start_date = status.get_status(info.game_code, "ru", 'day', default=info.default_date)
    last_dt = status.get_status(info.game_code, "member_info", 'cdc', default=info.default_date)
    last_date = last_dt.split(' ', 1)[0]
    

    query = f"\
SELECT division_date\n\
      ,os_type\n\
      ,country_code\n\
      ,date_count\n\
      ,CASE WHEN @os_type = os_type AND @country_code = country_code THEN @sum_date_count := @sum_date_count + date_count\n\
            ELSE @sum_date_count := date_count\n\
        END sum_date_count\n\
      ,@os_type := os_type t_os_type\n\
      ,@country_code := country_code t_country_code\n\
  FROM (\n\
            SELECT f.division_date\n\
                  ,f.os_type\n\
                  ,f.country_code\n\
                  ,IFNULL(ui.date_count, 0) date_count\n\
              FROM (\n\
                       SELECT mi.division_date\n\
                             ,f.os_type\n\
                             ,f.country_code\n\
                         FROM (\n\
                                  SELECT DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) division_date\n\
                                    FROM game.boxing_star_member_info mi FORCE INDEX (IDX_bsmi_rd_lld_mi_mn)\n\
                                   WHERE DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) >= DATE_ADD(STR_TO_DATE('{start_date}', '%Y-%m-%d'), INTERVAL -{info.before_days} day)\n\
                                     AND DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) < DATE(now())\n\
                                   GROUP BY DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour))\n\
                              ) mi\n\
                        INNER JOIN (\n\
                                  SELECT os_type\n\
                                        ,nation_id country_code\n\
                                    FROM game.boxing_star_member_info_exp FORCE INDEX (IDX_bsmie_ot_ni)\n\
                                   GROUP BY os_type\n\
                                        ,nation_id\n\
                               ) f\n\
                   ) f\n\
              LEFT JOIN (\n\
                        SELECT create_date division_date\n\
                              ,os_type\n\
                              ,country_code\n\
                              ,user_count date_count\n\
                          FROM (\n\
                                    SELECT DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) create_date\n\
                                          ,me.os_type\n\
                                          ,me.nation_id country_code\n\
                                          ,COUNT(*) user_count\n\
                                      FROM game.boxing_star_member_info mi\n\
                                     INNER JOIN game.boxing_star_member_info_exp me FORCE INDEX (IDX_bsmie_mi_ot_ni)\n\
                                        ON me.member_idx = mi.member_idx\n\
                                     WHERE DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) > DATE_ADD(STR_TO_DATE('{start_date}', '%Y-%m-%d'), INTERVAL -{info.before_days} day)\n\
                                       AND DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour)) < DATE(now())\n\
                                     GROUP BY DATE(DATE_ADD(mi.reg_date, INTERVAL {info.time_difference} hour))\n\
                                          ,me.os_type\n\
                                          ,me.nation_id\n\
                                  UNION ALL\n\
                                     SELECT division_date create_date\n\
                                          ,os_type\n\
                                          ,country_code\n\
                                          ,sum_date_count\n\
                                      FROM ru_day rd\n\
                                     WHERE game_code = '{info.game_code}'\n\
                                       AND DATE(division_date) = DATE_ADD(STR_TO_DATE('{start_date}', '%Y-%m-%d'), INTERVAL -{info.before_days} day)\n\
                               ) ui\n\
                                     ORDER BY os_type\n\
                                             ,country_code\n\
                                             ,create_date\n\
                   ) ui\n\
                ON ui.division_date = f.division_date\n\
               AND ui.os_type = f.os_type\n\
               AND ui.country_code = f.country_code\n\
             ORDER BY f.os_type\n\
                  ,f.country_code\n\
                  ,f.division_date\n\
       ) ui\n\
 INNER JOIN (SELECT @sum_date_count := 0, @os_type := NULL, @country_code := NULL) t\n\
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
        
        table_name = "ru_day"
        rds_conn.cleanup_day(table_name, start_date)

        last_date = rds_df['division_date'].max()

        rds_df.drop(columns=['t_os_type', 't_country_code'], inplace=True)

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
        status.set_status(info.game_code, "ru", 'day', last_date)
    

# # 리텐션

# In[9]:


def summary_retention():
    
    print(f"{Utils.dt()} ##### Retention #####")
    
    status = Status()
    
    end_date = status.get_status(info.game_code, "member_login_log", "dms", default=info.default_date)
    end_date = end_date.split(' ', 1)[0]
    
    start_date = status.get_status(info.game_code, "retention", "day", default=info.default_date)
    start_date = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=32)).strftime("%Y-%m-%d")
    
    athena = Athena()
    where_dt = athena.datetime_range_where(start_date, end_date, freq="D")

    query = f"\
SELECT ui.reg_date division_reg_date\n\
      ,ui.os_type\n\
      ,ui.country_code\n\
      ,ui.user_count\n\
      ,user_day1 user_1\n\
      ,user_day2 user_2\n\
      ,user_day3 user_3\n\
      ,user_day4 user_4\n\
      ,user_day5 user_5\n\
      ,user_day6 user_6\n\
      ,user_day7 user_7\n\
      ,user_day8 user_8\n\
      ,user_day9 user_9\n\
      ,user_day10 user_10\n\
      ,user_day11 user_11\n\
      ,user_day12 user_12\n\
      ,user_day13 user_13\n\
      ,user_day14 user_14\n\
      ,user_day15 user_15\n\
      ,user_day16 user_16\n\
      ,user_day17 user_17\n\
      ,user_day18 user_18\n\
      ,user_day19 user_19\n\
      ,user_day20 user_20\n\
      ,user_day21 user_21\n\
      ,user_day22 user_22\n\
      ,user_day23 user_23\n\
      ,user_day24 user_24\n\
      ,user_day25 user_25\n\
      ,user_day26 user_26\n\
      ,user_day27 user_27\n\
      ,user_day28 user_28\n\
      ,user_day29 user_29\n\
      ,user_day30 user_30\n\
      ,sales_day0 sales_0\n\
      ,sales_day1 sales_1\n\
      ,sales_day2 sales_2\n\
      ,sales_day3 sales_3\n\
      ,sales_day4 sales_4\n\
      ,sales_day5 sales_5\n\
      ,sales_day6 sales_6\n\
      ,sales_day7 sales_7\n\
      ,sales_day8 sales_8\n\
      ,sales_day9 sales_9\n\
      ,sales_day10 sales_10\n\
      ,sales_day11 sales_11\n\
      ,sales_day12 sales_12\n\
      ,sales_day13 sales_13\n\
      ,sales_day14 sales_14\n\
      ,sales_day15 sales_15\n\
      ,sales_day16 sales_16\n\
      ,sales_day17 sales_17\n\
      ,sales_day18 sales_18\n\
      ,sales_day19 sales_19\n\
      ,sales_day20 sales_20\n\
      ,sales_day21 sales_21\n\
      ,sales_day22 sales_22\n\
      ,sales_day23 sales_23\n\
      ,sales_day24 sales_24\n\
      ,sales_day25 sales_25\n\
      ,sales_day26 sales_26\n\
      ,sales_day27 sales_27\n\
      ,sales_day28 sales_28\n\
      ,sales_day29 sales_29\n\
      ,sales_day30 sales_30\n\
  FROM (\n\
          SELECT DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date)) reg_date\n\
                ,me.os_type\n\
                ,me.nation_id country_code\n\
                ,count(*) user_count\n\
            FROM wotan_rds.game.boxing_star_member_info mi\n\
           INNER JOIN wotan_rds.game.boxing_star_member_info_exp me\n\
              ON me.member_idx = mi.member_idx\n\
           WHERE DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date)) > DATE_ADD('day', -{info.before_days}, DATE_PARSE('{start_date}', '%Y-%m-%d'))\n\
             AND DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date)) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
           GROUP BY DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))\n\
                ,me.os_type\n\
                ,me.nation_id\n\
       ) ui\n\
  LEFT JOIN (\n\
           SELECT DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date)) division_reg_date\n\
                 ,me.os_type\n\
                 ,me.nation_id country_code\n\
                 ,count(*) user_count\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 1, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day1\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 2, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day2\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 3, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day3\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 4, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day4\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 5, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day5\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 6, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day6\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 7, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day7\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 8, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day8\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 9, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day9\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 10, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day10\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 11, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day11\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 12, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day12\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 13, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day13\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 14, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day14\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 15, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day15\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 16, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day16\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 17, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day17\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 18, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day18\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 19, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day19\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 20, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day20\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 21, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day21\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 22, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day22\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 23, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day23\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 24, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day24\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 25, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day25\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 26, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day26\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 27, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day27\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 28, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day28\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 29, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day29\n\
                 ,SUM(CASE WHEN ml.login_date = DATE_ADD('day', 30, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))) THEN 1 ELSE 0 END) user_day30\n\
             FROM wotan_rds.game.boxing_star_member_info mi\n\
            INNER JOIN wotan_rds.game.boxing_star_member_info_exp me\n\
               ON me.member_idx = mi.member_idx\n\
             LEFT JOIN (\n\
                      SELECT DATE(login_dt) login_date\n\
                            ,member_idx\n\
                        FROM AwsDataCatalog.wotan_s3.boxing_star_member_login_log ml\n\
                       WHERE (\n\
{where_dt}\
                              )\n\
                       GROUP BY DATE(login_dt)\n\
                               ,member_idx\n\
                  ) ml\n\
               ON ml.member_idx = mi.member_idx\n\
              AND DATE(ml.login_date) BETWEEN DATE(DATE_ADD('day', 1, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date)))) AND DATE(DATE_ADD('day', 32, DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))))\n\
            WHERE DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date)) > DATE_ADD('day', -{info.before_days}, DATE_PARSE('{start_date}', '%Y-%m-%d'))\n\
              AND DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date)) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
            GROUP BY DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date))\n\
                 ,me.os_type\n\
                 ,me.nation_id\n\
       ) ul\n\
    ON ul.division_reg_date = ui.reg_date\n\
   AND ul.os_type = ui.os_type\n\
   AND ul.country_code = ui.country_code\n\
  LEFT JOIN (\n\
          SELECT mi.reg_date\n\
                ,mi.os_type\n\
                ,mi.country_code\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 0, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day0\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 1, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day1\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 2, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day2\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 3, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day3\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 4, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day4\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 5, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day5\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 6, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day6\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 7, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day7\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 8, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day8\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 9, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day9\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 10, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day10\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 11, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day11\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 12, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day12\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 13, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day13\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 14, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day14\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 15, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day15\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 16, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day16\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 17, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day17\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 18, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day18\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 19, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day19\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 20, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day20\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 21, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day21\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 22, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day22\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 23, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day23\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 24, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day24\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 25, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day25\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 26, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day26\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 27, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day27\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 28, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day28\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 29, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day29\n\
                ,SUM(CASE WHEN pb.log_date = DATE_ADD('day', 30, mi.reg_date) THEN pb.real_cash_kr ELSE 0 END) sales_day30\n\
            FROM (\n\
                    SELECT DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date)) reg_date\n\
                          ,mi.member_idx\n\
                          ,ie.os_type\n\
                          ,ie.nation_id country_code\n\
                      FROM wotan_rds.game.boxing_star_member_info mi\n\
                     INNER JOIN wotan_rds.game.boxing_star_member_info_exp ie\n\
                        ON ie.member_idx = mi.member_idx\n\
                     WHERE DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date)) > DATE(DATE_ADD('day', -{info.before_days}, DATE_PARSE('{start_date}', '%Y-%m-%d')))\n\
                       AND DATE(DATE_ADD('hour', {info.time_difference}, mi.reg_date)) < DATE(DATE_ADD('hour', {info.time_difference}, now()))\n\
                 ) mi\n\
            LEFT JOIN (\n\
                     SELECT DATE(log_dt) log_date\n\
                           ,member_idx\n\
                           ,SUM(real_cash_kr) real_cash_kr\n\
                       FROM AwsDataCatalog.wotan_s3.boxing_star_player_billing_log pb\n\
                      WHERE (\n\
{where_dt}\
                            )\n\
                      GROUP BY DATE(log_dt)\n\
                              ,member_idx\n\
                 ) pb\n\
              ON pb.member_idx = mi.member_idx\n\
             AND DATE(pb.log_date) BETWEEN mi.reg_date AND DATE_ADD('day', 31, mi.reg_date)\n\
           GROUP BY mi.reg_date\n\
                ,os_type\n\
                ,country_code\n\
       ) pb\n\
    ON pb.reg_date = ui.reg_date\n\
   AND pb.os_type = ui.os_type\n\
   AND pb.country_code = ui.country_code\n\
;\
"
    print(query)
    
    athena_database = "wotan_s3"
    athena_df = athena.exeuteQuery2Df(athena_database, query)
    
    athena_df.drop(athena_df.columns[len(athena_df.columns) - 1], axis=1, inplace=True)
    athena_df.dropna(subset = ["division_reg_date"], inplace=True)
    print(f">>DataFrame Size : {len(athena_df)}")
    
    if len(athena_df) > 0 :
        
        rds_conn = RdsConnection()
    
        table_name = "retention_day"
        rds_conn.cleanup_day(table_name, start_date)
    
        bookmark_reg_dt = athena_df["division_reg_date"].max()

        athena_df.insert(0, "game_code", info.game_code)
        athena_df.rename(columns={"division_reg_date" : "division_date"}, inplace=True)

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
        status.set_status(info.game_code, "retention", "day", bookmark_reg_dt)


# # Run Function

# In[10]:


def run():
    
#     summary_nru()
#     summary_dau()
    summary_pu()
#     summary_eru()
#     summary_rau()
#     summary_sales()
#     summary_ru()
#     summary_retention()
    
    print("\n\n\n### JOB END. ###")

# In[11]:


if __name__=="__main__":
    run()
