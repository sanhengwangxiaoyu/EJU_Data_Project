#%%
import sys
from typing import Tuple
import pandas as pd
import numpy as np
import time
import configparser
import os
from pandas.core import groupby
import pymysql



cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值


# -*- coding: utf-8 -*-
class MysqlClient:
    def __init__(self, db_host,database,user,password):
        """
        create connection to hive server
        """
        self.conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
    def query(self, sql):
        """
        query
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        columnDes = cur.description #获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]
        cur.close()
        return res,columnNames
    def close(self):
        self.conn.close()

# con = MysqlClient(db_host,database,user,password)

pymysql.install_as_MySQLdb()
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqldb://'+'mysql'+':'+'egSQ7HhxajHZjvdX'+'@'+'172.28.36.77'+':'+'3306'+'/'+'dws_db')# 初始化引擎
con = MysqlClient(db_host,database,user,password)

# period_value = '2020Q4'
# start_date = '2020-10-01'
# end_date = '2020-12-31'

period_value = '2021Q1'
start_date = '2021-01-01'
end_date = '2021-03-31'


#%%
# # # # # # # # # # # # # # # # # # # # # # # # 兴趣偏好TGITOP10 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  # # # # # # # #  # # # # # # # #  # # # # # # # #  # # # # # # # #  



# 楼盘浏览表消重后的明细
res,columnNames = con.query("SELECT a0.imei,a0.newest_id,a0.city_id,a0.county_id,a0.visit_date period FROM dwb_db.dwb_customer_browse_log a0  WHERE a0.visit_date >= '"+start_date+"' and a0.visit_date<='"+end_date+"'")
df = pd.DataFrame([list(i) for i in res],columns=columnNames)
df.period = pd.to_datetime(df.period)
df['period'] = pd.PeriodIndex(df.period, freq='Q')
df.drop_duplicates(inplace=True)
df = df.reset_index(drop=True)
df = df.astype('str')

# 关联准入表楼盘id进行过滤，减小数据量
res,columnNames = con.query("SELECT newest_id,period FROM dws_db.dws_newest_period_admit WHERE dr=0")
df1 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df2 = df.merge(df1,how='inner',on=['newest_id','period'])
df2



# 个体标签明细
res,columnNames = con.query("SELECT  imei, CASE WHEN social_prefer IS NOT NULL THEN 1 END prefer_num, '社交类型' tag_value  FROM dwb_db.dwb_customer_imei_tag WHERE social_prefer IS NOT NULL  AND  period = '"+period_value+"'  UNION ALL   SELECT  imei, CASE WHEN read_prefer IS NOT NULL THEN 1 END prefer_num, '阅读偏好' tag_value  FROM dwb_db.dwb_customer_imei_tag WHERE read_prefer IS NOT NULL  AND  period = '"+period_value+"'  UNION ALL   SELECT  imei, CASE WHEN game_prefer IS NOT NULL THEN 1 END prefer_num, '游戏偏好' tag_value  FROM dwb_db.dwb_customer_imei_tag WHERE game_prefer IS NOT NULL  AND  period = '"+period_value+"'  UNION ALL   SELECT  imei, CASE WHEN live_prefer IS NOT NULL THEN 1 END prefer_num, '直播偏好' tag_value  FROM dwb_db.dwb_customer_imei_tag WHERE live_prefer IS NOT NULL  AND  period = '"+period_value+"'  UNION ALL   SELECT  imei, CASE WHEN bank_prefer IS NOT NULL THEN 1 END prefer_num, '银行偏好' tag_value  FROM dwb_db.dwb_customer_imei_tag WHERE bank_prefer IS NOT NULL  AND  period = '"+period_value+"'  UNION ALL   SELECT  imei, CASE WHEN loan_prefer IS NOT NULL THEN 1 END prefer_num, '贷款偏好' tag_value  FROM dwb_db.dwb_customer_imei_tag WHERE loan_prefer IS NOT NULL  AND  period = '"+period_value+"'  UNION ALL   SELECT  imei, CASE WHEN invest_prefer IS NOT NULL THEN 1 END prefer_num, '投资偏好' tag_value  FROM dwb_db.dwb_customer_imei_tag WHERE invest_prefer IS NOT NULL  AND  period = '"+period_value+"'  UNION ALL   SELECT  imei, CASE WHEN habit IS NOT NULL THEN 1 END prefer_num, '生活行为' tag_value  FROM dwb_db.dwb_customer_imei_tag WHERE habit IS NOT NULL  AND  period = '"+period_value+"'  UNION ALL   SELECT  imei, CASE WHEN consume_prefer IS NOT NULL THEN 1 END prefer_num, '购物行为' tag_value  FROM dwb_db.dwb_customer_imei_tag WHERE consume_prefer IS NOT NULL  AND  period = '"+period_value+"' ")
df3 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df3


# 拼接个体标签
df4 = df2.merge(df3,how='inner',on='imei')

# 统计每个标签的TGI分子的分子
df5 = df4.groupby(['newest_id','city_id','tag_value'])['prefer_num'].sum().reset_index()
# 统计每个标签的TGI分子的分母
df6 = df4[['imei','newest_id','prefer_num']]
df6.drop_duplicates(inplace=True)
df7 = df6.groupby(['newest_id'])['prefer_num'].sum().reset_index()
# 拼接TGI分子部分
df8 = df5.merge(df7,how='left',on='newest_id')

# 统计每个标签TGI分母的分子
df9 = df4.groupby(['tag_value'])['prefer_num'].sum().to_frame('value3').reset_index()
# 统计每个标签TGI分母的分母
value4 = df7['prefer_num'].sum()


# 拼接成结果表
# 准入表
res,columnNames = con.query("SELECT newest_id,period FROM dws_db.dws_newest_period_admit WHERE dr=0 AND period = '"+period_value+"'")
zr = pd.DataFrame([list(i) for i in res],columns=columnNames)
# 拼接
re = zr.merge(df8,how='left',on=['newest_id']).merge(df9,how='left',on='tag_value')
re['value4'] = value4
re['value5'] = (re['prefer_num_x']/re['prefer_num_y'])/(re['value3']/re['value4'])
re['tag_name'] = '兴趣偏好TGITOP10'
re = re.rename(columns={'prefer_num_x':'value1','prefer_num_y':'value2'})
re = re[['city_id','newest_id','tag_value','value1','value2','value3','value4','value5','tag_name','period']]
re = re[(re['city_id'].notnull())]
# re.to_sql('dws_tag_purchase_prefer_bak_20210622',engine,index=False,if_exists='append')
print('done')



# %%
# # # # # # # # # # # # # # # # # # # # # # # # APP一级偏好 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# IMEI对应的季度与1级2级标签
res,columnNames = con.query(" SELECT a0.imei,a0.period,a0.app_prefer,a1.type1 FROM dwb_db.dwb_customer_imei_tag a0 LEFT JOIN dwb_db.app_type a1 ON a1.type2=a0.app_prefer WHERE a0.app_prefer IS NOT NULL AND a0.period ='"+period_value+"' ")
df = pd.DataFrame([list(i) for i in res],columns=columnNames)

# 浏览日志全局消重,筛选admin中楼盘
res,columnNames = con.query(" SELECT distinct imei,newest_id,concat(year(visit_date),'Q',quarter(visit_date)) period FROM dwb_db.dwb_customer_browse_log where visit_date >= '"+start_date+"' and visit_date<='"+end_date+"' ")
log = pd.DataFrame([list(i) for i in res],columns=columnNames)

# admin表过滤
res,columnNames = con.query(" select newest_id from dws_db.dws_newest_period_admit WHERE dr=0 and period = '"+period_value+"' ")
admin = pd.DataFrame([list(i) for i in res],columns=columnNames)

log = log.merge(admin,how='inner',on='newest_id')

# 日志拼接1级2级标签
tag = log.merge(df,how='inner',on=['imei','period'])

# 统计每个楼盘1级标签数量（top10）
tag1 = tag.groupby(['newest_id','period','type1'])['imei'].count().reset_index()
tag1.sort_values(['newest_id','period','imei'],ascending=False,inplace=True)

# top10每个标签对应的楼盘与人数
tag1top10 = tag1.groupby(['newest_id','period']).head(10).reset_index(drop=True)
# 目标楼盘所有有标签的IMEI总数
tagonly = tag.groupby(['newest_id','period']).agg({'imei':pd.Series.nunique}).reset_index()
# 本次统计周期内所有单个标签的类型的总IMEI数
tag1top10all = tag1top10.groupby('type1')['imei'].sum().reset_index()
# 本次统计周期内所有有标签的总人数
tagonlyall = tag['imei'].drop_duplicates().reset_index(drop=True)
tagonlyall1 = tagonlyall.count()

# 拼接
merge = tag1top10.merge(tagonly,how='inner',on=['newest_id','period']).merge(tag1top10all,how='inner',on='type1')
merge['all'] = tagonlyall1
# 添加city_id
res,columnNames = con.query(" SELECT newest_id,city_id FROM dws_db.dws_newest_info ")
city = pd.DataFrame([list(i) for i in res],columns=columnNames)
merge = merge.merge(city,how='inner',on=['newest_id'])
merge.rename(columns={'type1':'tag_value','imei_x':'value1','imei_y':'value2','imei':'value3','all':'value4'},inplace=True)
merge['value5'] = (merge['value1']/merge['value2'])/(merge['value3']/merge['value4'])
merge['tag_name'] = 'APP一级偏好'
merge = merge[['city_id','newest_id','tag_value','value1','value2','value3','value4','value5','tag_name','period']]

# merge.to_sql('dws_tag_purchase_prefer_bak_20210622',engine,index=False,if_exists='append')



#%%
# # # # # # # # # # # # # # # # # # # # # # # # APP二级偏好 # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

tag2 = tag.groupby(['newest_id','period','app_prefer','type1'])['imei'].count().reset_index()
prefer2 = tag2.merge(merge[['newest_id','period','tag_value','city_id']],how='inner',left_on=['newest_id','period','type1'],right_on=['newest_id','period','tag_value'])
prefer2 =prefer2[['newest_id','period','app_prefer','type1','imei','city_id']]
prefer2.sort_values(['newest_id','period','imei'],ascending=False,inplace=True)
prefer2 = prefer2.groupby(['newest_id','period']).head(10).reset_index(drop=True)
prefer2_sum = prefer2.groupby(['newest_id','period'])['imei'].sum().reset_index()
prefer2_merge = prefer2.merge(prefer2_sum,how='inner',on=['newest_id','period'])
prefer2_merge['value4'] = prefer2_merge['imei_x']/prefer2_merge['imei_y']
prefer2_merge['tag_name'] = 'APP偏好类型二级分类占比TOP10'
prefer2_merge.rename(columns={'app_prefer':'tag_value','type1':'value1','imei_x':'value2','imei_y':'value3'},inplace=True)
prefer2_re = prefer2_merge[['city_id','newest_id','tag_value','value1','value2','value3','value4','tag_name','period']]

prefer2_re.to_sql('dws_tag_purchase_prefer2',engine,index=False,if_exists='append')


#%%



