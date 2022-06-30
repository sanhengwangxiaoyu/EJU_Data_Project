import sys
import pandas as pd
import numpy as np
import time
import configparser
import os
from pandas.core import groupby
import pymysql
import re



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
engine = create_engine('mysql+mysqldb://'+'mysql'+':'+'egSQ7HhxajHZjvdX'+'@'+'172.28.56.90'+':'+'3306'+'/'+'dws_db')# 初始化引擎
con = MysqlClient(db_host,database,user,password)

# 取用户都浏览过哪些楼盘
res,columnNames = con.query('''
SELECT imei,newest_id,visit_date FROM dwb_db.dwb_customer_browse_log
''')
df = pd.DataFrame([list(i) for i in res],columns=columnNames)
df.visit_date = pd.to_datetime(df.visit_date)
df['quarter'] = pd.PeriodIndex(df.visit_date, freq='Q')
df1 = ['imei','newest_id','quarter']
df2 = pd.DataFrame(df,columns = df1).drop_duplicates().reset_index(drop=True)
df2

# 省份
res,columnNames = con.query('''
SELECT province_id,province_name,lat,lng from dws_db.dim_geography  where is_del =0 and province_id is not null and grade=2 
''')
df5 = pd.DataFrame([list(i) for i in res],columns=columnNames).drop_duplicates().reset_index(drop=True)
df5
# 常驻省份
res,columnNames = con.query('''
SELECT imei,resi_province from dwb_db.dwb_customer_imei_tag where resi_province is not null 
''')
df3 = pd.DataFrame([list(i) for i in res],columns=columnNames)

df4 = df3.drop('resi_province',axis=1).join(df3['resi_province'].str.split(',',expand=True).stack().reset_index(level=1,drop=True).rename('resi_province')).reset_index(drop=True)
df4['province'] = 'Unknown'
df4.loc[df4['resi_province']=='重庆市','province'] = '重庆'
df4.loc[df4['resi_province']=='北京市','province'] = '北京'
df4.loc[df4['resi_province']=='上海市','province'] = '上海'
df4.loc[df4['resi_province']=='天津市','province'] = '天津'
df4.loc[df4['province']=='Unknown','province'] = df4['resi_province']
df4
df5
df6 = pd.merge(df4,df5,how='left',left_on='province',right_on='province_name')
df7 = ['imei','province','province_id','lat','lng','area_type']
df8 = pd.DataFrame(df6,columns=df7)
df8['area_type']='area_province'
df8


# 城市
res,columnNames = con.query('''
SELECT city_id,city_name,lat,lng from dws_db.dim_geography  where is_del =0 and city_id is not null and grade=3 
''')
df55 = pd.DataFrame([list(i) for i in res],columns=columnNames).drop_duplicates().reset_index(drop=True)
df55
# 常驻省份
res,columnNames = con.query('''
SELECT imei,resi_city from dwb_db.dwb_customer_imei_tag where resi_city is not null 
''')
df33 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df33

df44 = df33.drop('resi_city',axis=1).join(df33['resi_city'].str.split(',',expand=True).stack().reset_index(level=1,drop=True).rename('resi_city')).reset_index(drop=True)
df44['city']=df44['resi_city'].apply(lambda x:re.split('-',str(x))[-1])
df44
df66 = pd.merge(df44,df55,how='left',left_on='city',right_on='city_name')
df66

df77 = ['imei','city','city_id','lat','lng','area_type']
df88 = pd.DataFrame(df66,columns=df77)
df88['area_type']='area_city'
df88


# 县区
res,columnNames = con.query('''
SELECT region_id,region_name,lat,lng from dws_db.dim_geography  where is_del =0 and region_id is not null and grade=4 
''')
df51 = pd.DataFrame([list(i) for i in res],columns=columnNames).drop_duplicates().reset_index(drop=True)
df51
# 常驻区县
res,columnNames = con.query('''
SELECT imei,resi_county from dwb_db.dwb_customer_imei_tag where resi_county is not null 
''')
df31 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df31

df41 = df31.drop('resi_county',axis=1).join(df31['resi_county'].str.split(',',expand=True).stack().reset_index(level=1,drop=True).rename('resi_county')).reset_index(drop=True)
df41['county']=df41['resi_county'].apply(lambda x:re.split('-',str(x))[-1])
df41
df61 = pd.merge(df41,df51,how='left',left_on='county',right_on='region_name')
df61

df71 = ['imei','county','region_id','lat','lng','area_type']
df81 = pd.DataFrame(df61,columns=df71)
df81['area_type']='area_county'
df81


df8.rename(columns={'province': 'name','province_id': 'id'}, inplace=True)
df8

df88.rename(columns={'city': 'name','city_id': 'id'}, inplace=True)
df88

df81.rename(columns={'county': 'name','region_id': 'id'}, inplace=True)
df81

# 拼接省市区
df100 = df8.append(df88).reset_index(drop=True)
df101 = df100.append(df81).reset_index(drop=True)

# 拼接用户浏览楼盘记录与常驻区域
dfre = pd.merge(df2,df101,how='inner',on='imei')
dfre1 = dfre.groupby(['newest_id','quarter','area_type','name','id','lat','lng'])['imei'].count().reset_index()
dfre1.sort_values(['newest_id','quarter','area_type','imei'],ascending=False,inplace=True)

dfre22 = dfre1.groupby(['newest_id','quarter','area_type','imei']).head(30).reset_index(drop=True)
dfre33 = dfre22.groupby(['newest_id','quarter','area_type'])['imei'].sum().reset_index()
dfre44 = pd.merge(dfre22,dfre33,how='inner',on=['newest_id','quarter','area_type'])
dfre44['percentage']=round(dfre44['imei_x']/dfre44['imei_y'],4)
dfre44=dfre44.astype('str')
dfre44
dfre44.to_sql('dws_tag_resident_area',engine,index=False,if_exists='append')



