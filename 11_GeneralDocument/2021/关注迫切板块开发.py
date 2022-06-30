import sys
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

period_value = '2020Q4'
start_date = '2020-10-01'
end_date = '2020-12-31'

# period_value = '2021Q1'
# start_date = '2021-01-01'
# end_date = '2021-03-31'


# 取用户都浏览过哪些楼盘
res,columnNames = con.query(" SELECT city_id,imei,newest_id,visit_date FROM dwb_db.dwb_customer_browse_log where visit_date >= '"+start_date+"' and visit_date<='"+end_date+"' ")
df = pd.DataFrame([list(i) for i in res],columns=columnNames)
df.visit_date = pd.to_datetime(df.visit_date)
df['quarter'] = pd.PeriodIndex(df.visit_date, freq='Q')

df0 = ['city_id','imei','newest_id','quarter']
df0 = pd.DataFrame(df,columns=df0)

df0.drop_duplicates(subset=['city_id','imei','newest_id','quarter'],keep=False,inplace=True)
df0 = df0.reset_index(drop=True)
df0


# 统计迫切与意向
res,columnNames = con.query('''
select * from dws_db.dws_imei_browse_tag
'''
)
df1 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df1

df0=df0.astype('str')
df1=df1.astype('str')

df2 = df0.merge(df1[['imei','period','intention','urgent']],how='inner',left_on=['imei','quarter'],right_on=['imei','period'])
df2

df3 = df2.groupby(['newest_id','quarter','intention'])['imei'].count().reset_index()
df4 = df3[df3['intention']=='意向']
df5 = df2.groupby(['newest_id','quarter','urgent'])['imei'].count().reset_index()
df6 = df5[df5['urgent']=='迫切']
df7 = pd.merge(df4,df6,how='left',on=['newest_id','quarter'])
df7



res,columnNames = con.query('''
 select distinct imei,'投资型' type  from dwb_db.dwb_customer_imei_tag where is_college_stu = '否' -- and period = '2020Q4'
and marriage = '已婚' and education = '高' and have_child = '有'
''')
df8 = pd.DataFrame([list(i) for i in res],columns=columnNames)

# 从竞对表中挑选出上线楼盘id的list
res,columnNames = con.query(" SELECT DISTINCT newest_id FROM dws_db.dws_compete_list_qua where period = '"+period_value+"'")
dff = pd.DataFrame([list(i) for i in res],columns=columnNames)
dff

dfn = pd.merge(dff,df7,how='left',on='newest_id')
dfn


df9 = ['imei','newest_id','quarter']
df10 = pd.DataFrame(df0,columns = df9).drop_duplicates().reset_index(drop=True)
df10
df11 = pd.merge(df10,df8,how='left',on='imei')
df11.at[df11['type'].isna(),'type']='自住型'
df12 = df11.groupby(['newest_id','quarter','type'])['imei'].count().rename('imei1').reset_index()
df13 = df12[df12['type']=='投资型']
df14 = df12[df12['type']=='自住型']
df15  = dfn.merge(df13,how='left',on=['newest_id','quarter']).merge(df14,how='left',on=['newest_id','quarter'])
df15['investment_rate']=df15['imei1_x']/(df15['imei1_x']+df15['imei1_y'])
df15['owner_rate']=df15['imei1_y']/(df15['imei1_x']+df15['imei1_y'])

res,columnNames = con.query('''
 select newest_id,city_id,county_id from dwb_db.dwb_newest_info
''')
df16 = pd.DataFrame([list(i) for i in res],columns=columnNames)
df17 = df15.merge(df16,how='left',on='newest_id')
df18 = ['quarter','newest_id','imei_x','imei_y','imei1_x','imei1_y','investment_rate','owner_rate','city_id','county_id']
df19 = pd.DataFrame(df17,columns=df18)
df19.rename(columns={'quarter': 'period','imei_x': 'intention','imei_y': 'urgent','imei1_x': 'investment','imei1_y': 'owner'}, inplace=True)
df19 = df19[df19['period'].notnull()]
df19.to_sql('dws_newest_customer_qua',engine,index=False,if_exists='append')












#竞争关系-客户竞争指数
#imei标签-三度

res,columnNames = con.query('''
SELECT city_name,estate_name newest_name,substr(visit_time,1,10) date,customer imei,'2020Q4' period FROM odsdb.cust_browse_log
where idate between '20201001' and '20201231'
''')
ori = pd.DataFrame([list(i) for i in res],columns=columnNames)
# ori=con.query('''SELECT city_name,estate_name newest_name,substr(visit_time,1,10) date,customer imei,'2020Q4' period FROM odsdb.cust_browse_log
# where idate between '20201001' and '20201231' ''')

imei_browse_tag=ori.groupby(['period','imei'])['newest_name'].count().reset_index()
imei_browse_tag.columns=['period','imei','cou']
imei_browse_tag[['cou']] = imei_browse_tag[['cou']].astype('int')
imei_browse_tag['concern']=np.nan
imei_browse_tag['intention']=np.nan
imei_browse_tag['urgent']=np.nan
imei_browse_tag['concern']='关注'
imei_browse_tag.at[imei_browse_tag['cou']>3,'intention']='意向'
imei_browse_tag.at[imei_browse_tag['cou']>10,'urgent']='迫切'
aa = imei_browse_tag[imei_browse_tag['imei']=='35156509082626']
aa 


res,columnNames = con.query('''
SELECT customer imei,count(*) cou_l FROM odsdb.cust_browse_log
where idate between '20200701' and '20200930' group by customer
''')
q3 = pd.DataFrame([list(i) for i in res],columns=columnNames)
# q3=con.query('''SELECT customer imei,count(*) cou_l FROM odsdb.cust_browse_log
# where idate between '20200701' and '20200930' group by customer ''')
imei_browse_tag=pd.merge(imei_browse_tag,q3,on=['imei'],how='left')
imei_browse_tag.at[imei_browse_tag['cou']/imei_browse_tag['cou_l']>2,'intention']=np.nan
imei_browse_tag.at[imei_browse_tag['cou']/imei_browse_tag['cou_l']>2,'urgent']='迫切'
imei_browse_tag=imei_browse_tag.drop('cou_l',axis=1)
imei_browse_tag=imei_browse_tag.drop('cou',axis=1)

#imei标签-增存
his=con.query('''SELECT distinct customer imei,'活跃' cre FROM odsdb.cust_browse_log_202004_202103
where idate between '20200401' and '20200930' ''')
imei_browse_tag=pd.merge(imei_browse_tag,his,on='imei',how='left')
imei_browse_tag.at[imei_browse_tag['cre'].isna(),'cre']='增长'


