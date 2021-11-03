# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Seq 15 17:44:47 2021
"""
import configparser
import os
from pandas.core.frame import DataFrame
import pymysql
import pandas as pd
from sqlalchemy import create_engine

##读取配置文件##
pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

##设置变量初始值##
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
database = 'dws_db_prd'
table_name = 'dws_newest_layout_price_copy'
dadte_quarter = '2021Q2'


##mysql连接配置##
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
        data = pd.DataFrame([list(i) for i in res],columns=columnNames)
        cur.close()
        return data
    def close(self):
        self.conn.close()

##mysql写入执行##
def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)
#创建数据库连接
con = MysqlClient(db_host,database,user,password)


# In[]:
# dws_newest_layout_price 
newest_avg_price=con.query("select city_id,newest_id,city_level_desc,quarter,`interval`,percent,`type` from dws_newest_layout_price where `type` = 'resi_district'")


# In[]:
df1 = newest_avg_price.groupby(['city_id','newest_id','city_level_desc','type'])['quarter'].max().reset_index()
df1 = pd.merge(df1,newest_avg_price,how='left',on=['city_id','newest_id','city_level_desc','type','quarter'])



# In[]:
df_quarter = newest_avg_price[newest_avg_price['quarter'] == dadte_quarter]
df_no_quarter = df1[~df1['newest_id'].isin(df_quarter['newest_id'])]
df_no_quarter['quarter'] = dadte_quarter
df_no_quarter[['city_id']] = df_no_quarter[['city_id']].astype(float).astype(int)


# In[]

#数据加载到mysql表里去
to_dws(df_no_quarter,table_name)
print('>>>>Done')

# In[11]:
# dws_newest_period_admit
admit=con.query("select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0")
lost_newest_avg_price = admit[~admit['newest_id'].isin(newest_avg_price['newest_id'])]





