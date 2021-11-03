# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Oct 29 15:44:47 2021
"""
import configparser,os,pymysql,pandas as pd,time,re,datetime
import numpy as np
from sqlalchemy import create_engine

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
def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)
con = MysqlClient(db_host,database,user,password)


# In[2]:
# 去除预售和动态信息
# 区分
# 按照格式拼接到ods层表
# 清洗
qua=con.query("select * from dws_db_prd.josn_20211029 where `key` in ('售卖资格','发证时间','date','tag','title','content')")


# %%
#预售证信息
newest_issue = qua[qua['key'].isin(['售卖资格','发证时间'])]
newest_issue = newest_issue[['newest_id','group','key','values']]
#分开
newest_issue_code = newest_issue[newest_issue['key'] == '售卖资格']
newest_issue_date = newest_issue[newest_issue['key'] == '发证时间']
#拼接
newest_issue = pd.merge(newest_issue_code,newest_issue_date,how='inner',on=['newest_id','group'])
#结果
newest_issue = newest_issue[['newest_id','values_x','values_y']]
newest_issue.columns=['newest_id','issue_code','issue_date']
# newest_issue['issue_date'] = newest_issue['issue_date'].apply(lambda x:time.strptime(x, "%Y-%m-%d"))
newest_issue = newest_issue[(newest_issue['issue_date']!='-')&(newest_issue['issue_date']!='暂无信息')]
newest_issue = newest_issue.loc[newest_issue['issue_date'].str.contains("2021")]
newest_issue


#In[]
database = 'dws_db_prd'
to_dws(newest_issue,'dws_newest_issue_code')
print('>>> load data from json_20211029 to dws_newest_issue_code Done!!!!!!!!!')




#In[]
dws_issue_code=con.query("select newest_id,issue_code , issue_code clean_code,dr,create_time from dws_db_prd.dws_newest_issue_code")
newest_id = con.query("select newest_id ,city_id+0 city_id,county_id+0 region_id from dws_db_prd.dws_newest_info where dr = 0")
gra_name = con.query("select city_id+0 city_id,city_name,region_id+0 region_id,region_name from dws_db_prd.dim_geography where grade=4 and region_id is not null group by city_id,city_name,region_id,region_name")
newest_id = pd.merge(newest_id,gra_name,how='inner',on=['city_id','region_id'])


#In[]
df_dws_issue_code = dws_issue_code
df_dws_issue_code['clean_code'] = df_dws_issue_code['clean_code'].str.replace('\D', '')
df_dws_issue_code = df_dws_issue_code.groupby(['newest_id','clean_code','dr'])['issue_code','create_time'].max().reset_index()
df_dws_issue_code = pd.merge(df_dws_issue_code,newest_id,how='left',on=['newest_id'])
df_dws_issue_code

#In[]
df_dws_code = df_dws_issue_code[['city_id','city_name','region_id','region_name','newest_id','issue_code','dr','create_time']]
df_dws_code.columns=['city_id','city_name','county_id','county_name','newest_id','issue_code','dr','create_time']
df_dws_code

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
update_sql1 = "UPDATE dws_db_prd.dws_newest_issue_code SET dr = 1 WHERE dr = 0"
cur.execute(update_sql1)
conn.commit() # 提交记
conn.close() # 关闭数据库链接



to_dws(df_dws_code,'dws_newest_issue_code')
print('>>> load data from dws_newest_issue_code to dws_newest_issue_code Done!!!!!!!!!')








# %%
#楼盘动态信息
newest_sche = qua[qua['key'].isin(['date','tag','title','content'])]
newest_sche = newest_sche[['newest_id','group','key','values']]
#分开
newest_sche_date = newest_sche[newest_sche['key'] == 'date']
newest_sche_tag = newest_sche[newest_sche['key'] == 'tag']
newest_sche_title = newest_sche[newest_sche['key'] == 'title']
newest_sche_content = newest_sche[newest_sche['key'] == 'content']
#拼接
newest_sche = pd.merge(newest_sche_date,newest_sche_tag,how='inner',on=['newest_id','group'])
newest_sche = pd.merge(newest_sche,newest_sche_title,how='inner',on=['newest_id','group'])
newest_sche = pd.merge(newest_sche,newest_sche_content,how='inner',on=['newest_id','group'])
#结果
newest_sche = newest_sche[['newest_id','values_x','values_y']]
newest_sche.columns=['url','provide_date','sche_tag','provide_title','provide_sche']
#对照表格式加列
newest_sche['newest_name']=np.nan
newest_sche['date_clean']=np.nan
newest_sche['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
newest_sche['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
newest_sche = newest_sche[['url','newest_name','sche_tag','provide_title','provide_date','provide_sche','date_clean','create_time','update_time']]
# newest_sche = newest_sche.apply(lambda x:x['provide_date'].replace('年'))
#清洗日期
newest_sche['date_clean'] = newest_sche['provide_date']
newest_sche['date_clean'] = ((newest_sche['date_clean'].apply(lambda x:re.sub("年", "-", x))).apply(lambda x:re.sub("月", "-", x))).apply(lambda x:re.sub("日", "", x))
newest_sche


#In[]
## 加载数据到最终表里去
database = 'odsdb'
to_dws(newest_sche,'ori_newest_provide_sche')
print('>>> load data from json_20211029 to ori_newest_provide_sche Done!!!!!!!!!')



#In[]
dws_newest_sche = con.query("select *,provide_sche clean_sche from dws_db_prd.dws_newest_provide_sche")


#In[]
dws_newest_sche['clean_sche'] = dws_newest_sche['clean_sche'].str.replace('[^\u4e00-\u9fa5]', '')
df_dws_sche = dws_newest_sche
df_dws_sche = df_dws_sche.groupby(['newest_id','clean_sche'])['date','period','provide_title','provide_sche'].max().reset_index()
df_dws_sche = df_dws_sche[['newest_id','date','period','provide_title','provide_sche']]


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
update_sql1 = "delete from  dws_db_prd.dws_newest_provide_sche"
cur.execute(update_sql1)
conn.commit() # 提交记
conn.close() # 关闭数据库链接


to_dws(df_dws_sche,'dws_newest_provide_sche')
print('>>> load data from dws_newest_provide_sche to dws_newest_provide_sche Done!!!!!!!!!')



