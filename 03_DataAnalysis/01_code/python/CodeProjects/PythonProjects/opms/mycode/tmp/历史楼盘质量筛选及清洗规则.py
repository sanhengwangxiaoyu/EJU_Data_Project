import sys
from typing import Tuple
import pandas as pd
import numpy as np
import time
import configparser
import os
from pandas.core import groupby
import pymysql
import re
from fuzzywuzzy import process

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
engine = create_engine('mysql+mysqldb://'+'mysql'+':'+'egSQ7HhxajHZjvdX'+'@'+'172.28.36.77'+':'+'3306'+'/'+'temp_db')# 初始化引擎
con = MysqlClient(db_host,database,user,password)


# 获取日志表中清洗过的楼盘名与城市对应的客户数量
res,columnNames = con.query('''
SELECT clean_floor_name newest_name,source,concat(city_name,'市') city_name,count(customer) imei_num 
FROM odsdb.cust_browse_log_202004_202103 GROUP BY clean_floor_name,city_name,source
''')
cus_log = pd.DataFrame([list(i) for i in res],columns=columnNames).reset_index(drop=True)
cus_log = cus_log.groupby(['newest_name','city_name'])['imei_num'].sum().to_frame('imei_all').reset_index().merge(cus_log,how='right',on=['newest_name','city_name'])
cus_log

# 获取清洗过的楼盘表
res,columnNames = con.query('''
SELECT uuid newest_id,origin_name newest_name,newest_name newest_name1,city_name FROM odsdb.clean_ori_newest_alias_base GROUP BY uuid,origin_name,city_name,newest_name
''')
clean_newest = pd.DataFrame([list(i) for i in res],columns=columnNames).reset_index(drop=True)
clean_newest

# 关联楼盘id
newest_idmerge = pd.merge(cus_log,clean_newest,how='left',on=['newest_name','city_name'])
newest_idmerge

# 关联到楼盘id的结果
newest_id_notnull = newest_idmerge[newest_idmerge['newest_id'].notnull()]
newest_id_notnull.to_sql('temp_canmg',engine,index=False,if_exists='append')
newest_id_notnull1 = newest_id_notnull[newest_id_notnull['newest_name']=='万科金色悦城金街商铺']
newest_id_notnull1
# 没关联到楼盘id的结果
newest_id_isnull = newest_idmerge[newest_idmerge['newest_id'].isnull()]
newest_id_isnull1 = newest_id_isnull[newest_id_isnull['newest_name']=='万科金色悦城金街商铺']
newest_id_isnull1
newest_id_isnull.to_sql('temp_notcanmg',engine,index=False,if_exists='append')

# 没关联到且用户量大于40个的且来源不是苹果的楼盘
# newest_id_isnull40 = newest_id_isnull[(newest_id_isnull['imei_num']>40)&(newest_id_isnull['source']!='苹果')]
# 取括号后面的字段

# 取前楼盘四个字段
# newest_id_isnull40['newname_4']=newest_id_isnull40['newest_name'].apply(lambda x:re.split(r'）',str(x))[-1]).apply(lambda x:x[:4]).tolist()
# newest_id_isnull40

# newest_id_isnull40

newest_list = clean_newest['newest_name'].drop_duplicates()
newest_list = list(newest_list)
newest_list
# newest_list1 = clean_newest['newest_name'].drop_duplicates().reset_index().to_csv('n.csv')

old_list = newest_id_isnull40['newest_name'].drop_duplicates().reset_index(drop = True).reset_index()
# old_list = old_list[old_list['index']<=2]
old_list_ls = old_list[old_list['newest_name']=='万科金色悦城金街商铺']
old_list_ls['newname_re'] = old_list_ls['newest_name'].apply(lambda x:process.extractOne(x,newest_list))
old_list_ls

old_list['newname_re'] = old_list['newest_name'].apply(lambda x:process.extractOne(x,newest_list))
old_list['re'] = old_list['newname_re'].apply(lambda x:re.split("',",str(x))[0]).apply(lambda x:re.split("'",str(x))[-1])
old_list

list_merge = pd.merge(old_list,clean_newest,how='left',left_on='re',right_on='newest_name')
list_merge






# newest_id_isnull40能匹配到的 newest_name是原始楼盘名 newest_name1是标准名
list_merge1 = newest_id_isnull40.merge(list_merge[['newest_name_x','newest_name1','city_name']],how='left',left_on=['newest_name','city_name'],right_on=['newest_name_x','city_name'])\
    .merge(clean_newest[['newest_id','newest_name1','city_name']],how='left',left_on=['newest_name1','city_name'],right_on=['newest_name1','city_name'])
list_merge1
list_merge2 = list_merge1[list_merge1['newest_id_y'].notnull()].drop_duplicates().reset_index(drop=True)
list_merge2.to_sql('temp_mgnewest40_canmg',engine,index=False,if_exists='append')


# newest_id_isnull40匹配不到的名单 newest_name是原始楼盘名 newest_name1是标准名
list_merge3 = list_merge1[list_merge1['newest_name1'].isnull()]
list_merge3
list_merge3.to_sql('temp_mgnewest40_notcanmg',engine,index=False,if_exists='append')






