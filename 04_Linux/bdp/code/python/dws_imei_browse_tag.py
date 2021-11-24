# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Aug 309 14:53:31 2021

"""
import configparser
import os
import sys
import pymysql
import pandas as pd
import numpy as np
from collections import Counter
import re
from sqlalchemy import create_engine
import getopt
from dateutil.relativedelta import relativedelta
pd.set_option('display.max_columns',None)

##设置配置信息##
pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
period = '2018Q1'
table_name='dws_imei_browse_tag'

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
con = MysqlClient(db_host,database,user,password)


def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)


# In[]
opts,args=getopt.getopt(sys.argv[1:],"t:q:d:",["database","table=","quarter="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table":
    table_name = arg
  elif opts=="-q" or opts=="--quarter":
    period = arg
  elif opts=="-d" or opts=="--database":
    database = arg

print(database+'.'+table_name+':'+period)

# In[2]:

##重置时间格式
start_date = str(pd.to_datetime(period))[0:10]   #截取成yyyy-MM-dd
end_date =  str(pd.to_datetime(period) + pd.offsets.QuarterEnd(0))[0:10]      #截取成yyyy-MM-dd
"""
正式代码：
  逻辑如下：
    关注:这个人关注的楼盘数量

    意向算法调整 - 调整思路 : 集中度  
    关注3个楼盘以上的用户中,
    若 (用户关注楼盘的最大值-城市平均关注值)/城市平均关注值 >100% 则为意向用户

    迫切算法调整 - 调整思路 : 集中度+高密集集中度
    在意向的基础上,那批人的(用户关注楼盘的最大值-自己关注的中位数)/自己关注的中位数>100% 则为迫切用户
    
    留存：增存量-第一次出现为增量，其他为存量
  
  
  思路如下：
    1、读取季度数据,获取增全量标识
    2、计算关注人数
    3、计算用户关注楼盘的数量
    4、求出城市平均关注值
    5、求出用户关注楼盘次数的中位数
    6、计算意向人数
    7、计算迫切人数
    8、加载到mysql

"""

#imei标签-三度
ori=con.query(" SELECT city_id,newest_id,pv,visit_date,imei, '"+period+"' period FROM dwb_db.a_dwb_customer_browse_log where visit_date>='"+start_date+"' and visit_date<'"+end_date+"' ")


# In[5]:
add_new_code = con.query(" SELECT imei,min(add_new_code) cre,visit_quarter period FROM dwb_db.dwb_customer_add_new_code where dr = '0' and visit_quarter = '"+period+"' group by imei,visit_quarter")

print(+ori)
print('====================')
print(add_new_code)

#In[5]:
### 关注了几个楼盘
df1=ori.groupby(['period','imei'])['newest_id'].count().reset_index()
df1.columns=['period','imei','cou']
df1[['cou']] = df1[['cou']].astype('int')
df1['concern']='关注'


### 用户关注楼盘的次数
#In[5]:
####楼盘被用户关注的次数
df2=ori.groupby(['period','imei','newest_id','city_id'])['pv'].sum().reset_index()
df3_tmp=ori.groupby(['period','imei','newest_id','city_id'])['visit_date'].count().reset_index()
df3=pd.merge(df2,df3_tmp,how='left',on=['period','imei','newest_id','city_id'])
df3['cu']=df3['pv']+df3['visit_date']
#截取列
df3=df3[['period','imei','newest_id','city_id','cu']]
df3[['cu']] = df3[['cu']].astype('int')
###用户关注度最高的楼盘次数
df4 = df3.groupby(['period','city_id','imei'])['cu'].max().reset_index()
df4.columns=['period','city_id','imei','max_cu']
df4[['max_cu']] = df4[['max_cu']].astype('int')
###城市关注度平均次数
df5 = df3_tmp.groupby(['period','city_id'])['visit_date'].mean().reset_index()
#获取平均值
df5.columns=['period','city_id','avg_cu']
####用户关注度中位数
df3_tmp.sort_values(['imei','visit_date'],ascending=[1,0],inplace=True)
df6 = df3_tmp.groupby(['period','imei'])['visit_date'].median().reset_index()
df6.columns=['period','imei','median_cu']
####合并
df7 = pd.merge(df4,df5,how='left',on=['period','city_id'])
df7 = pd.merge(df7,df6,how='left',on=['period','imei'])
df7 = df7.groupby(['period','imei'])['max_cu','avg_cu','median_cu'].max().reset_index()

# df4[df4['max_cu']<40].groupby(df4['max_cu']).count()
# df1['cou'].groupby(df1['cou']).count()

#In[6]:
###意向
imei_browse_tag = df1[['period','imei','concern','cou']]
imei_browse_tag['intention']=np.nan
imei_browse_tag['urgent']=np.nan
imei_browse_tag=pd.merge(imei_browse_tag,df7,how='left',on=['period','imei'])
imei_browse_tag.at[(imei_browse_tag['cou']>=2)&((imei_browse_tag['max_cu']-imei_browse_tag['avg_cu'])/imei_browse_tag['avg_cu']>0.4) ,'intention']='意向'
#迫切
imei_browse_tag.at[(imei_browse_tag['intention'] == '意向')&((imei_browse_tag['max_cu']-imei_browse_tag['median_cu'])/imei_browse_tag['median_cu']>2),'urgent']='迫切'
# imei_browse_tag[imei_browse_tag['max_cu']==2]
# imei_browse_tag[~imei_browse_tag['intention'].isna()]
# imei_browse_tag[~imei_browse_tag['urgent'].isna()]
#合并增全量标识
imei_browse_tag=pd.merge(imei_browse_tag,add_new_code,how='left',on=['period','imei'])
imei_browse_tag.at[imei_browse_tag['cre'].isna(),'cre'] = 0
imei_browse_tag.at[imei_browse_tag['cre']==1,'cre']='活跃'
imei_browse_tag.at[imei_browse_tag['cre']==0,'cre']='增长'
imei_browse_tag=imei_browse_tag[['period','imei','concern','intention','urgent','cre']]


# imei_browse_tag[~imei_browse_tag['intention'].isna()]
# imei_browse_tag[~imei_browse_tag['urgent'].isna()]

print(imei_browse_tag)

# In[9]:
#imei_browse_tag.columns=['period','customer','concern','intention','urgent','cre']
to_dws(imei_browse_tag,table_name)
print('>>> Done')

# In[ ]:
# df4.to_csv('C:\\Users\\86133\\Desktop\\df4.csv')




# In[ ]:





