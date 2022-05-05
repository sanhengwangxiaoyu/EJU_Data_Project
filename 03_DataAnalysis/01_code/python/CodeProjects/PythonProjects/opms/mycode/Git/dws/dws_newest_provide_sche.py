#In[]
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Mar 26 16:44:47 2021
dws_newest_provide_sche

"""
import configparser,os,sys,pymysql,pandas as pd,getopt
from pandas.tseries.offsets import QuarterBegin
from sqlalchemy import create_engine

pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
date_quarter = '2021Q4'   # 季度
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



#In[]
# ##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:d:c:",["city_id","database=","table=","quarter="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table": # 获取输入参数 -t或者--table 后的值
    table_name = arg
  elif opts=="-q" or opts=="--quarter":  # 获取输入参数 -1或者--quarter 后的值
    date_quarter = arg
  elif opts=="-d" or opts=="--database":  # 获取输入参数 -1或者--quarter 后的值
    database = arg
  elif opts=="-c" or opts=="--city_id":  # 获取输入参数 -1或者--quarter 后的值
    city_id = arg



# In[1]:
#设置格式
conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
def connect_mysql(conn):
#判断链接是否正常
 conn.ping(True)
#建立操作游标
 cursor=conn.cursor()
#设置数据输入输出编码格式
 cursor.execute('set names utf8')
 return cursor
# 建立链接游标
cur=connect_mysql(conn)
update_sql = "insert into dws_db_prd.dws_newest_provide_sche (newest_id,`date`,period,provide_title,provide_sche) select newest_id ,`date` ,period ,provide_title ,provide_sche from dwb_db.dwb_newest_provide_sche where dr = 0 and length(provide_title)>2 and length(provide_sche)>2 and period ='"+date_quarter+"'"
cur.execute(update_sql)
conn.commit() # 提交记
conn.close() # 关闭数据库链接



# In[1]:
#设置格式
conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
def connect_mysql(conn):
#判断链接是否正常
 conn.ping(True)
#建立操作游标
 cursor=conn.cursor()
#设置数据输入输出编码格式
 cursor.execute('set names utf8')
 return cursor
# 建立链接游标
cur=connect_mysql(conn)
update_sql = "update dws_db_prd.dws_newest_provide_sche set provide_sche = replace(provide_sche,'。0','.0') where period = '"+date_quarter+"'"
cur.execute(update_sql)
conn.commit() # 提交记
conn.close() # 关闭数据库链接



# In[1]:
#设置格式
conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
def connect_mysql(conn):
#判断链接是否正常
 conn.ping(True)
#建立操作游标
 cursor=conn.cursor()
#设置数据输入输出编码格式
 cursor.execute('set names utf8')
 return cursor
# 建立链接游标
cur=connect_mysql(conn)
update_sql = "update dws_db_prd.dws_newest_provide_sche set provide_sche = replace(provide_sche,'、.','') where period = '"+date_quarter+"'"
cur.execute(update_sql)
conn.commit() # 提交记
conn.close() # 关闭数据库链接



# In[1]:
#设置格式
conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
def connect_mysql(conn):
#判断链接是否正常
 conn.ping(True)
#建立操作游标
 cursor=conn.cursor()
#设置数据输入输出编码格式
 cursor.execute('set names utf8')
 return cursor
# 建立链接游标
cur=connect_mysql(conn)
update_sql = "update dws_db_prd.dws_newest_provide_sche set provide_sche = REPLACE(REPLACE(REPLACE(REPLACE( provide_sche,'(','' ),'（',''),')',''),'）','') where (LENGTH(provide_sche) - LENGTH(REPLACE(REPLACE( provide_sche,'(','' ),'（',''))) != (LENGTH(provide_sche) - LENGTH(REPLACE(REPLACE(provide_sche,')','' ),'）',''))) and period = '"+date_quarter+"'"
cur.execute(update_sql)
conn.commit() # 提交记
conn.close() # 关闭数据库链接



# In[1]:
#设置格式
conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
def connect_mysql(conn):
#判断链接是否正常
 conn.ping(True)
#建立操作游标
 cursor=conn.cursor()
#设置数据输入输出编码格式
 cursor.execute('set names utf8')
 return cursor
# 建立链接游标
cur=connect_mysql(conn)
update_sql = "update dws_db_prd.dws_newest_provide_sche set provide_sche = REPLACE(REPLACE(provide_sche,'[','' ),']','') where (LENGTH(provide_sche) - LENGTH(REPLACE(provide_sche,'[','' ))) != (LENGTH(provide_sche) - LENGTH(REPLACE(provide_sche,']','' ))) and period = '"+date_quarter+"'"
cur.execute(update_sql)
conn.commit() # 提交记
conn.close() # 关闭数据库链接



# In[1]:
#设置格式
conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
def connect_mysql(conn):
#判断链接是否正常
 conn.ping(True)
#建立操作游标
 cursor=conn.cursor()
#设置数据输入输出编码格式
 cursor.execute('set names utf8')
 return cursor
# 建立链接游标
cur=connect_mysql(conn)
update_sql = "update dws_db_prd.dws_newest_provide_sche set provide_title = REPLACE(REPLACE(provide_title,'[','' ),']','') where (LENGTH(provide_title) - LENGTH(REPLACE(provide_title,'[','' ))) != (LENGTH(provide_title) - LENGTH(REPLACE(provide_title,']','' ))) and period = '"+date_quarter+"'"
cur.execute(update_sql)
conn.commit() # 提交记
conn.close() # 关闭数据库链接



# In[1]:
#设置格式
conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
def connect_mysql(conn):
#判断链接是否正常
 conn.ping(True)
#建立操作游标
 cursor=conn.cursor()
#设置数据输入输出编码格式
 cursor.execute('set names utf8')
 return cursor
# 建立链接游标
cur=connect_mysql(conn)
update_sql = "update dws_db_prd.dws_newest_provide_sche set provide_sche = replace(provide_sche,'，一房一价','于2021年') where period = '"+date_quarter+"'"
cur.execute(update_sql)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>>> Done')



# In[1]:
#设置格式
conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
def connect_mysql(conn):
#判断链接是否正常
 conn.ping(True)
#建立操作游标
 cursor=conn.cursor()
#设置数据输入输出编码格式
 cursor.execute('set names utf8')
 return cursor
# 建立链接游标
cur=connect_mysql(conn)
update_sql = "delete from dws_db_prd.dws_newest_provide_sche where provide_sche in ('预售证','特价房','com.') or provide_title like '%全部*' or provide_title = '[售楼处'"
cur.execute(update_sql)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>>> Done')




#In[]
dws_newest_sche = con.query("select *,substr(provide_sche,1,30) clean_sche from dws_db_prd.dws_newest_provide_sche where period = '"+date_quarter+"'")

dws_newest_sche['clean_sche'] = dws_newest_sche['clean_sche'].str.replace('[^\u4e00-\u9fa5]', '')
df_dws_sche = dws_newest_sche
df_dws_sche = df_dws_sche.groupby(['newest_id','clean_sche'])['date','period','provide_title','provide_sche'].max().reset_index()
df_dws_sche = df_dws_sche[['newest_id','date','period','provide_title','provide_sche']]



conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
def connect_mysql(conn):
#判断链接是否正常
 conn.ping(True)
#建立操作游标
 cursor=conn.cursor()
#设置数据输入输出编码格式
 cursor.execute('set names utf8')
 return cursor
# 建立链接游标
cur=connect_mysql(conn)
update_sql1 = "delete from  dws_db_prd.dws_newest_provide_sche where period = '"+date_quarter+"'"
cur.execute(update_sql1)
conn.commit() # 提交记
conn.close() # 关闭数据库链接


to_dws(df_dws_sche,'dws_newest_provide_sche')
print('>>> ETL load data from dws_newest_provide_sche to dws_newest_provide_sche Done!!!!!!!!!')



#In[]
import re

dws_newest_sche = con.query("select *from dws_db_prd.dws_newest_provide_sche where period = '"+date_quarter+"'")

dws_newest_sche1 =dws_newest_sche.loc[dws_newest_sche['provide_sche'].apply(lambda s: re.search(r'均价\d{2}元', s) != None)].reset_index(drop=True)
dws_newest_sche2 =dws_newest_sche1.loc[dws_newest_sche1['provide_sche'].apply(lambda s: re.search(r'均价\d{1}元', s) != None)].reset_index(drop=True)
result = pd.merge(dws_newest_sche,dws_newest_sche2,how='left',on=['id','newest_id','date','period'])

result = result[result['provide_title_y'].isna()]
result = result[['id','newest_id','date','period','provide_title_x','provide_sche_x']]
result.columns=['id','newest_id','date','period','provide_title','provide_sche']


#In[]


conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
def connect_mysql(conn):
#判断链接是否正常
 conn.ping(True)
#建立操作游标
 cursor=conn.cursor()
#设置数据输入输出编码格式
 cursor.execute('set names utf8')
 return cursor
# 建立链接游标
cur=connect_mysql(conn)
update_sql1 = "delete from  dws_db_prd.dws_newest_provide_sche where period = '"+date_quarter+"'"
cur.execute(update_sql1)
conn.commit() # 提交记
conn.close() # 关闭数据库链接


to_dws(result,'dws_newest_provide_sche')
print('>>> load data from dws_newest_provide_sche to dws_newest_provide_sche Done!!!!!!!!!')





