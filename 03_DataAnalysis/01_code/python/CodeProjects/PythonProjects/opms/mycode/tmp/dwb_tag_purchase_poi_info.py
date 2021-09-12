# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Seq 07 17:44:47 2021
  配套信息关联楼盘和计算直线距离

"""
import configparser
import os
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from geopy.distance import geodesic
import time

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
table_name = 'dwb_tag_purchase_poi_info' # 要插入的表名称
database = 'dwb_db'

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
##room_sum清洗逻辑
def sum_str(s):
    new_str = ""  		#创建一个空字符串
    for ch in s:
	    if ch.isdigit():		#字符串中的方法，可以直接判断ch是否是数字
		    new_str += ch
	    else:
		    new_str += " "
    sub_list = new_str.split()   #对新的字符串切片
    num_list = list(map(int, sub_list)) 	#map方法，使列表中的元素按照指定方式转变
    res  = sum(num_list)
    # print(res)
    return res


##正式代码##
"""
1> 获取数据信息：dws_newest_info，dws_newest_period_admit
    通过admit筛选准入楼盘信息，通过dws_newest_info获取楼盘具体信息
"""
con = MysqlClient(db_host,database,user,password)


# In[]:
newest=con.query("select min(id) newest_ods_id,newest_name,newest_id,address newest_address,lnglat newest_lnglat,city_id from odsdb.ori_newest_period_admit_info group by newest_name,newest_id,address,lnglat,city_id")

poi=con.query("select min(id) poi_ods_id,newest_name,newest_address,newest_lnglat,poi_type,poi_index,poi_name,poi_lnglat,file_name,substring_index(poi_lnglat,',',-1) as lat_x,substring_index(poi_lnglat,',',1) as lng_x ,substring_index(newest_lnglat,',',-1) as lat_y,substring_index(newest_lnglat,',',1) as lng_y from odsdb.ori_newest_poi_info where dr = 0 group by newest_name,newest_address,newest_lnglat,poi_type,poi_index,poi_name,poi_lnglat,file_name")


# # In[]:
# poi_1 = poi[poi['file_name'] == '20210906_latlng_rs-31657-31856']
# poi_2 = poi[poi['file_name'] == '20210907_latlng_rs-31857-32057']
# poi_3 = poi[poi['file_name'] == '20210907_latlng_rs-32058-32257']
# newest_1 = newest[newest['newest_ods_id'] <= 31856]
# newest_2 = newest[(newest['newest_ods_id'] <= 32057)&(newest['newest_ods_id'] >= 31857)]
# newest_3 = newest[(newest['newest_ods_id'] <= 32257)&(newest['newest_ods_id'] >= 32058)]


# In[]:
# df_1 = pd.merge(poi_1,newest_1,how='inner',on=['newest_name','newest_address','newest_lnglat'])
# df_2 = pd.merge(poi_2,newest_2,how='inner',on=['newest_name','newest_address','newest_lnglat'])
# df_3 = pd.merge(poi_3,newest_3,how='inner',on=['newest_name','newest_address','newest_lnglat'])
df = pd.merge(poi,newest,how='inner',on=['newest_name','newest_address','newest_lnglat'])


# In[]:
####   df_1 处理
# df_1['pure_distance'] = df_1.apply(lambda x:geodesic((x.lat_y,x.lng_y),(x.lat_x,x.lng_x)).km,axis=1)
# df_2['pure_distance'] = df_2.apply(lambda x:geodesic((x.lat_y,x.lng_y),(x.lat_x,x.lng_x)).km,axis=1)
# df_3['pure_distance'] = df_3.apply(lambda x:geodesic((x.lat_y,x.lng_y),(x.lat_x,x.lng_x)).km,axis=1)
df['pure_distance'] = df.apply(lambda x:geodesic((x.lat_y,x.lng_y),(x.lat_x,x.lng_x)).km,axis=1)

result= df
result['dr']=0
result['create_time']=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
result['update_time']=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
result = result[['newest_ods_id','poi_ods_id','newest_id','newest_lnglat','poi_type','poi_index','poi_name','poi_lnglat','pure_distance','dr','create_time','update_time','city_id']]


# In[10]:
# 加载到新表 dwb_newest_issue_offer
# result.drop_duplicates(inplace=True)
to_dws(result,table_name)
# df_fz.to_csv('C:\\Users\\86133\\Desktop\\df_fz.csv')
# result
print('>>> load data Done')


#In[]
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
update_sql = "update odsdb.ori_newest_poi_info set dr = 1 "
cur.execute(update_sql)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>>> set dr = 1 Done')

