# In[]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 16:44:47 2021

@author: admin1
"""
import configparser,os,sys,pymysql,pandas as pd,numpy as np,re,time
from sqlalchemy import create_engine
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta


##读取配置文件##
pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
table_name = 'dwb_customer_add_new_code'
database = 'dwb_db'
date_quater = '2021Q4'



# In[3]:
##重置时间格式
start_time = str(pd.to_datetime(date_quater))[0:10]   #截取成yyyy-MM-dd
stop_time =  str(pd.to_datetime(date_quater) + pd.offsets.QuarterEnd(0))[0:10]      #截取成yyyy-MM-dd
sixmonth_ago_time = str(pd.to_datetime(date_quater) - relativedelta(months=6))[0:10]   #截取成yyyy-MM-dd


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

def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

con = MysqlClient(db_host,database,user,password)

"""
传参
"""
newest_id = '3c2a249ea8fa11ec869c8c8caa44e774'



print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  + '  reading data to : a_dwb_customer_browse_log')


# In[]
#增存量标识#
#获取周维度#
ori=con.query("SELECT id ods_id,'a_dwb_customer_browse_log' ods_table_name,imei,visit_month,concat(substr(visit_month,1,4),'Q',QUARTER(visit_date)) visit_quarter,city_id,visit_date FROM  dwb_db.a_dwb_customer_browse_log where newest_id = '"+newest_id+"' and visit_date >= '"+start_time+"' and visit_date <='"+stop_time+"'")
date_time=con.query("select cal_date visit_date,period visit_week from dws_db_prd.dim_period_date dpd where  cal_date >= '"+start_time+"' and cal_date <='"+stop_time+"'")
ori['visit_date'] = ori['visit_date'].astype(str)
ori=pd.merge(ori,date_time,how='left',on=['visit_date'])

print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  + '  reading data to : dwb_customer_add_new_code')


# In[3]:
#获取历史用户
imeis = "'"+re.sub(r',','\', \'',','.join(ori['imei'].values.tolist()))+"'"
add_new_code=con.query("SELECT ods_table_name,imei,visit_week,visit_month,visit_quarter,city_id,ods_id,'visit_date' FROM  dwb_db.dwb_customer_add_new_code where imei in ("+imeis+") and add_new_code = '0' and dr = '0' and visit_date < '"+start_time+"' and  visit_date >= '"+sixmonth_ago_time+"'")
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  + '  merge data')


#In[]
#去重
ori = ori.groupby(['ods_table_name','imei','visit_week','visit_month','visit_quarter','city_id'])['ods_id','visit_date'].min().reset_index()
ori = ori.append(add_new_code,ignore_index=True)
#按照星期偏移
ori['add_new_code']=ori.sort_values(by=['imei','visit_week']).groupby(['imei'])['visit_week'].shift(1)
#赋值
ori.at[ori['add_new_code'].isna(),'add_new_code']="0"
ori.at[ori['add_new_code']!='0','add_new_code']="1"
#去除历史数据
ori = ori[ori['visit_quarter'] >= date_quater]
ori['dr'] = 0
ori['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
ori['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
ori = ori[['ods_id','ods_table_name','imei','visit_week','visit_month','visit_quarter','add_new_code','dr','create_time','update_time','city_id','visit_date']]


# In[27]:
to_dws(ori,table_name)
print('>>> Done')


