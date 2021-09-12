# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on July 28 16:44:47 2021

城市项目总量与意向客户统计 - 维度（季度、月份、城市、区域）  指标（项目各种数量、各种浏览人数量、楼盘均价）

"""
import configparser
import os
import sys
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
import datetime
import getopt
import time
import sqlalchemy

##设置配置信息##
pymysql.install_as_MySQLdb()
#读取配置文件#
cf = configparser.ConfigParser()  
path = os.path.abspath(os.curdir)  
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

##设置变量初始值##
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
database = 'dwb_db'        # 重置数据库
date_quarter = '2021Q2'    # 季度
start_date = '20210401'   # 季度开始时间
stop_date = '20210701'   # 季度结束时间
table_name = 'dwb_newest_city_qua'  # 要插入的表名称


# In[2]:
##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:s:e:d:",["database=","table=","quarter=","startdate=","enddate="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table":
    table_name = arg
  elif opts=="-q" or opts=="--quarter":
    date_quarter = arg
  elif opts=="-s" or opts=="--startdate":
    start_date = arg
  elif opts=="-e" or opts=="--enddate":
    stop_date = arg
  elif opts=="-d" or opts=="database":
        database = arg


# In[3]:
##重置时间格式
start_date_DF = datetime.datetime.strptime(start_date, "%Y%m%d")  #转换为yyyy-MM-dd HH:mm:ss 的时间格式
end_date_DF = datetime.datetime.strptime(stop_date, "%Y%m%d")     #转换为yyyy-MM-dd HH:mm:ss 的时间格式
pre_start_date = str(start_date_DF)[0:10]   #截取成yyyy-MM-dd
pre_end_date =  str(end_date_DF)[0:10]      #截取成yyyy-MM-dd

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

con = MysqlClient(db_host,database,user,password)


# In[4]:
##正式代码##
"""
1> 获取数据信息： dwb_customer_browse_log  dws_newest_info  dws_newest_period_admit dws_imei_browse_tag
    通过admit筛选准入楼盘信息，通过dws_newest_info获取楼盘具体信息，通过browse获取客户数量，通过dws_imei_browse_tag获取客户增全量
2> 城市项目总量与意向客户统计 # dws_newest_city_qua
      读取当前季度的城市，楼盘信息：基础数据  城市id，区域，周期，均价
      统计各个状态的楼盘数量
      统计关注人数--->意向人数--->迫切人数
      统计当季新增 当季留存
      再同样的流程添加城市的，上边是按照区域的
2> 统计月份的关注人数
      去重后获取每个人关注的月份，再进行sum
"""
# dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
#                                                      imei          客户号
#                                                      county_id     区县日期  
#                                                      city_id       城市id 
#                                                      newest_id     楼盘id 
ori=con.query("SELECT newest_id,city_id,county_id,imei,visit_date FROM dwb_db.dwb_customer_browse_log where visit_date>='"+pre_start_date+"' and visit_date<'"+pre_end_date+"'")
# sql = "SELECT newest_id,city_id,county_id,imei,visit_date FROM dwb_db.dwb_customer_browse_log where visit_date>='"+pre_start_date+"' and visit_date<'"+pre_end_date+"'"

# In[5]:
#dws_newest_info  新房楼盘
#                                                      city_id         城市id  
#                                                      county_id       区县id  
#                                                      unit_price      均价
#                                                      newest_id       楼盘id 
#                                                      sales_state     销售状态
newest_id=con.query('''SELECT  sales_state,newest_id,city_id, county_id, unit_price  FROM dws_db.dws_newest_info where dr=0''')
# newest_id=con.query('''SELECT  sales_state,newest_id,city_id, county_id, unit_price  FROM dws_db.bak_20210728_dws_newest_info where dr=0''')

# In[6]:
# dws_newest_period_admit  准入楼盘表
#                                                      newest_id       楼盘id

admit = con.query('''select distinct newest_id from dws_db.dws_newest_period_admit where period = "'''+date_quarter+'''"  and dr = 0''')
# dws_imei_browse_tag  客户浏览标签结果表
#                                   imei            类似客户号的东西
#                                   concern        关注
#                                   intention      意向
#                                   urgent         迫切
#                                   cre            增存
ime = con.query('''select imei,concern,intention,urgent,cre from dws_db.dws_imei_browse_tag where period = "'''+date_quarter+'''"''')


# In[7]:
# 获取楼盘的城市和区县，以及浏览基本情况
grouped = pd.merge(admit, newest_id, how='left', on=['newest_id'])
grouped1 = grouped.groupby(['city_id','county_id'])['unit_price'].mean().reset_index()
grouped1.at[grouped1['unit_price'].isna(),'unit_price']=0
# 这里做了修改，把该过滤条件去掉
grouped1[['unit_price']] = grouped1[['unit_price']].astype('int')
# id为空值的数据
grouped002 = grouped
grouped002.at[grouped002['city_id'].isna(),'city_id']='0'
grouped002.at[grouped002['county_id'].isna(),'county_id']='0'
grouped2 = grouped002.groupby(['sales_state','city_id','county_id'])['newest_id'].count().reset_index()


# In[8]:
# 统计各个状态的楼盘数量
grouped2_for_sale= grouped2[grouped2['sales_state']=='待售']
grouped2_for_sale.rename(columns={'newest_id':'for_sale'},inplace=True)
grouped2_on_sale= grouped2[grouped2['sales_state']=='在售']
grouped2_on_sale.rename(columns={'newest_id':'on_sale'},inplace=True)
# 拼接合并
grouped3 = pd.merge(grouped1, grouped2_for_sale, how='left', on=['city_id', 'county_id'])
grouped3 = pd.merge(grouped3, grouped2_on_sale, how='left', on=['city_id', 'county_id'])
# 售罄都为0
grouped3['sell_out'] = '0'
# 去列
grouped3 = grouped3[['city_id','county_id','unit_price','for_sale','on_sale','sell_out']]
# 为空替换为0 
grouped3.at[grouped3['for_sale'].isna(),'for_sale']=0
grouped3.at[grouped3['on_sale'].isna(),'on_sale']=0
grouped3.at[grouped3['sell_out'].isna(),'sell_out']=0
# 获取项目总数
grouped3["total_count"] = grouped3['for_sale']+grouped3['on_sale']
# 转换为int类型
grouped3[['for_sale', 'on_sale', 'sell_out','total_count']] = grouped3[['for_sale', 'on_sale', 'sell_out','total_count']].astype('int')


# In[9]:
#统计关注人数--->意向人数--->迫切人数
#关联出city_id,county_id,状态
grouped4 = pd.merge(ori,ime ,how='left' ,on=['imei'])
grouped4 = grouped4[['newest_id','imei','concern','intention','urgent','cre','visit_date']]
#获取city_id和county_id
grouped5 = pd.merge(grouped4,grouped ,how='left' ,on=['newest_id'])
grouped5 = grouped5[['newest_id','imei','concern','intention','urgent','cre','city_id','county_id','visit_date']]


# In[10]:
# 关注人数
grouped5_concern_tmp =  grouped5[grouped5['concern'] == '关注']
grouped5_concern_tmp = grouped5_concern_tmp[['city_id','county_id','imei']].drop_duplicates()
grouped5_concern = grouped5_concern_tmp.groupby(['city_id','county_id'])['imei'].count().reset_index()
# 意向人数
grouped5_intention_tmp =  grouped5[grouped5['intention'] == '意向']
grouped5_intention_tmp = grouped5_intention_tmp[['city_id','county_id','imei']].drop_duplicates()
grouped5_intention = grouped5_intention_tmp.groupby(['city_id','county_id'])['imei'].count().reset_index()
# 迫切人数
grouped5_urgent_tmp =  grouped5[grouped5['urgent'] == '迫切']
grouped5_urgent_tmp = grouped5_urgent_tmp[['city_id','county_id','imei']].drop_duplicates()
grouped5_urgent = grouped5_urgent_tmp.groupby(['city_id','county_id'])['imei'].count().reset_index()
# 增量人数
grouped5_increase_tmp =  grouped5[grouped5['cre'] == '增长']
grouped5_increase_tmp = grouped5_increase_tmp[['city_id','county_id','imei']].drop_duplicates()
grouped5_increase = grouped5_increase_tmp.groupby(['city_id','county_id'])['imei'].count().reset_index()
# 留存人数
grouped5_retained_tmp =  grouped5[grouped5['cre'] == '活跃']
grouped5_retained_tmp = grouped5_retained_tmp[['city_id','county_id','imei']].drop_duplicates()
grouped5_retained = grouped5_retained_tmp.groupby(['city_id','county_id'])['imei'].count().reset_index()
# 修改列名
grouped5_concern.columns = ['city_id','county_id','concern']
grouped5_intention.columns = ['city_id','county_id','intention']
grouped5_urgent.columns = ['city_id','county_id','urgent']
grouped5_increase.columns = ['city_id','county_id','increase']
grouped5_retained.columns = ['city_id','county_id','retained']
# 合并拼接
grouped6_1 = pd.merge(grouped3,grouped5_concern,how='left',on=['city_id','county_id'])
grouped6_2 = pd.merge(grouped6_1,grouped5_intention,how='left',on=['city_id','county_id'])
grouped6_3 = pd.merge(grouped6_2,grouped5_urgent,how='left',on=['city_id','county_id'])
grouped6_4 = pd.merge(grouped6_3,grouped5_increase,how='left',on=['city_id','county_id'])
grouped6_5 = pd.merge(grouped6_4,grouped5_retained,how='left',on=['city_id','county_id'])
# 赋值季度
grouped6_5['quarter']=date_quarter
# 改列
result = grouped6_5[['city_id','county_id','quarter','for_sale','on_sale','sell_out','total_count','concern','intention','urgent','increase','retained','unit_price']]
result.columns = ['city_id','county_id','quarter','for_sale','on_sale','sell_out','total_count','follow','intention','urgent','increase','retained','unit_price']
# 去空
result = result.dropna(subset=['quarter'])
result = result.drop_duplicates()
result.at[result['follow'].isna(),'follow']=0
result.at[result['intention'].isna(),'intention']=0
result.at[result['urgent'].isna(),'urgent']=0
result.at[result['increase'].isna(),'increase']=0
result.at[result['retained'].isna(),'retained']=0
result

# # ================================================添加城市的，上边是按照区域的================================================


# In[11]:
# 添加城市的，上边是按照区域的
# 获取楼盘的城市和区县，以及浏览基本情况
grouped01 = grouped.groupby(['city_id'])['unit_price'].mean().reset_index()
# 空为0
grouped01.at[grouped01['unit_price'].isna(),'unit_price']=0
# 转换数据类型
grouped01[['unit_price']] = grouped01[['unit_price']].astype('int')
# 计算项目总数
grouped02 = grouped.groupby(['sales_state','city_id'])['newest_id'].count().reset_index()


# In[12]:
# 统计各个状态的楼盘数量
grouped02_for_sale= grouped02[grouped02['sales_state']=='待售']
grouped02_for_sale.rename(columns={'newest_id':'for_sale'},inplace=True)
# 在售
grouped02_on_sale= grouped02[grouped02['sales_state']=='在售']
grouped02_on_sale.rename(columns={'newest_id':'on_sale'},inplace=True)
# 合并拼接
grouped03 = pd.merge(grouped01, grouped02_for_sale, how='left', on=['city_id'])
grouped03 = pd.merge(grouped03, grouped02_on_sale, how='left', on=['city_id'])
# 售罄为0
grouped03['sell_out'] = '0'
grouped03 = grouped03[['city_id','unit_price','for_sale','on_sale','sell_out']]
# 空为0
grouped03.at[grouped03['for_sale'].isna(),'for_sale']=0
grouped03.at[grouped03['on_sale'].isna(),'on_sale']=0
grouped03.at[grouped03['sell_out'].isna(),'sell_out']=0
# 项目总量
grouped03["total_count"] = grouped03['for_sale']+grouped03['on_sale']
# 转换数据类型
grouped03[['for_sale', 'on_sale', 'sell_out','total_count']] = grouped03[['for_sale', 'on_sale', 'sell_out','total_count']].astype('int')
grouped03


# In[13]:
#先运行  In[7]、In[9]   !!!!!!!!!!!
#统计关注人数--->意向人数--->迫切人数   
# 关注人数
grouped05_concern_tmp =  grouped5[grouped5['concern'] == '关注']
grouped05_concern_tmp = grouped05_concern_tmp[['city_id','imei']].drop_duplicates()
grouped05_concern = grouped05_concern_tmp.groupby(['city_id'])['imei'].count().reset_index()
# 意向人数
grouped05_intention_tmp =  grouped5[grouped5['intention'] == '意向']
grouped05_intention_tmp = grouped05_intention_tmp[['city_id','imei']].drop_duplicates()
grouped05_intention = grouped05_intention_tmp.groupby(['city_id'])['imei'].count().reset_index()
# 迫切人数
grouped05_urgent_tmp =  grouped5[grouped5['urgent'] == '迫切']
grouped05_urgent_tmp = grouped05_urgent_tmp[['city_id','imei']].drop_duplicates()
grouped05_urgent = grouped05_urgent_tmp.groupby(['city_id'])['imei'].count().reset_index()
# 增量客户
grouped05_increase_tmp =  grouped5[grouped5['cre'] == '增长']
grouped05_increase_tmp = grouped05_increase_tmp[['city_id','imei']].drop_duplicates()
grouped05_increase = grouped05_increase_tmp.groupby(['city_id'])['imei'].count().reset_index()
# 存量客户
grouped05_retained_tmp =  grouped5[grouped5['cre'] == '活跃']
grouped05_retained_tmp = grouped05_retained_tmp[['city_id','imei']].drop_duplicates()
grouped05_retained = grouped05_retained_tmp.groupby(['city_id'])['imei'].count().reset_index()
# 修改列名
grouped05_concern.columns = ['city_id','concern']
grouped05_intention.columns = ['city_id','intention']
grouped05_urgent.columns = ['city_id','urgent']
grouped05_increase.columns = ['city_id','increase']
grouped05_retained.columns = ['city_id','retained']
# 合并拼接
grouped06_1 = pd.merge(grouped03,grouped05_concern,how='left',on=['city_id'])
grouped06_2 = pd.merge(grouped06_1,grouped05_intention,how='left',on=['city_id'])
grouped06_3 = pd.merge(grouped06_2,grouped05_urgent,how='left',on=['city_id'])
grouped06_4 = pd.merge(grouped06_3,grouped05_increase,how='left',on=['city_id'])
grouped06_5 = pd.merge(grouped06_4,grouped05_retained,how='left',on=['city_id'])
# 获取季度
grouped06_5['quarter']=date_quarter
grouped06_5['county_id'] = ''
# 改列
result0 = grouped06_5[['city_id','county_id','quarter','for_sale','on_sale','sell_out','total_count','concern','intention','urgent','increase','retained','unit_price']]
result0.columns = ['city_id','county_id','quarter','for_sale','on_sale','sell_out','total_count','follow','intention','urgent','increase','retained','unit_price']
# 去空
result0 = result0.dropna(subset=['quarter'])
result0 = result0.drop_duplicates()
result0.at[result0['follow'].isna(),'follow']=0
result0.at[result0['intention'].isna(),'intention']=0
result0.at[result0['urgent'].isna(),'urgent']=0
result0.at[result0['increase'].isna(),'increase']=0
result0.at[result0['retained'].isna(),'retained']=0
result0


# In[14]:
# # ================================================================================================================================================
# # ================================================添加月份的，上边是按照季度的================================================
###城市月度关注人数    先运行In[7] 、In[9] ！！！！！
# 获取关注客户
# grouped005_m_c =  grouped5[grouped5['concern'] == '关注']
# # 获取月份
# grouped005_m_c['visit_date_month']=grouped005_m_c['visit_date'].apply(lambda x:str(x)[0:7])
# # 去重，去空
# grouped005_m_c =  grouped005_m_c.groupby(['city_id','imei','visit_date_month'])['visit_date'].max().reset_index()
# # 城市月度关注人数
# grouped005_m_r = grouped005_m_c.groupby(['city_id','visit_date_month'])['imei'].count().reset_index()
# # 添加区县字段
# grouped005_m_r['county_id'] = grouped005_m_r['city_id']
# # 获取列
# grouped005_r = grouped005_m_r[['city_id','county_id','visit_date_month','imei']]


# In[15]:
###区域月度关注人数
# 获取关注客户
grouped006_m_c =  grouped5[grouped5['concern'] == '关注']
# 获取月份
grouped006_m_c['visit_date_month']=grouped006_m_c['visit_date'].apply(lambda x:str(x)[0:7])
# 去重，去空
grouped006_m_c =  grouped006_m_c.groupby(['city_id','county_id','imei','visit_date_month'])['visit_date'].max().reset_index()
# 城市月度关注人数
grouped006_r = grouped006_m_c.groupby(['city_id','county_id','visit_date_month'])['imei'].count().reset_index()
# grouped006_r_test = grouped006_r[grouped006_r['county_id'] == '110102']


# In[16]:
# 合并季度数据
result = result.append(result0,ignore_index=True)
# 合并月份数据
# grouped007 = grouped005_r.append(grouped006_r,ignore_index=True)
grouped007 = grouped006_r
# 填充列
result['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
result['dr'] = 0
result['month'] = ''
grouped007['quarter'] = date_quarter
grouped007['for_sale'] = 0
grouped007['on_sale'] = 0
grouped007['sell_out'] = 0
grouped007['total_count'] = 0
grouped007['intention'] = 0
grouped007['urgent'] = 0
grouped007['increase'] = 0
grouped007['retained'] = 0
grouped007['unit_price'] = 0
grouped007['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
grouped007['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
grouped007['dr'] = 0
# 字段整理和改名
grouped007_r = grouped007[['city_id','county_id','quarter','for_sale','on_sale','sell_out','total_count','imei','intention','urgent','increase','retained','unit_price','create_time','update_time','dr','visit_date_month']]
grouped007_r.columns=['city_id','county_id','quarter','for_sale','on_sale','sell_out','total_count','follow','intention','urgent','increase','retained','unit_price','create_time','update_time','dr','month']
grouped007_r['month'] = grouped007_r['month'].str.replace('-', '')

# grouped007_r_test = grouped007_r.groupby(['city_id'])['follow'].count().reset_index()




# In[15]:
#合并
r = result.append(grouped007_r,ignore_index=True)
#  加载数据dwb_newest_city_qua
to_dws(r,table_name)

# In[15]:
########单独插入月份区域数据
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
update_sql1 = "UPDATE "+database+"."+table_name+" SET dr = 1 WHERE dr = 0 and month is not null and quarter = '"+date_quarter+"'"
cur.execute(update_sql1)
# update_sql3 = "UPDATE "+database+"."+table_name+" SET month = NULL WHERE month = = 'NULL' OR month = ''"
# cur.execute(update_sql3)
conn.commit() # 提交记
conn.close() # 关闭数据库链接


to_dws(grouped007_r,table_name)


# In[16]:
#更新空值
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
update_sql1 = "UPDATE "+database+"."+table_name+" SET dr = 1 WHERE dr = 0 and month is not null and city_id in ('442000','441900') and quarter = '"+date_quarter+"'"
cur.execute(update_sql1)
# update_sql3 = "UPDATE "+database+"."+table_name+" SET month = NULL WHERE month = = 'NULL' OR month = ''"
# cur.execute(update_sql3)
conn.commit() # 提交记
conn.close() # 关闭数据库链接


# In[17]:
# grouped007_r_test.to_csv('C:\\Users\\86133\\Desktop\\grouped007_r_test.csv')


# In[18]:
#更新空值
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
update_sql1 = "UPDATE "+database+"."+table_name+" SET county_id = NULL WHERE county_id = 'NULL' OR county_id = ''"
cur.execute(update_sql1)
update_sql2 = "UPDATE "+database+"."+table_name+" SET unit_price = NULL WHERE unit_price = 0"
cur.execute(update_sql2)
# update_sql3 = "UPDATE "+database+"."+table_name+" SET month = NULL WHERE month = = 'NULL' OR month = ''"
# cur.execute(update_sql3)
conn.commit() # 提交记
conn.close() # 关闭数据库链接
print('>> Done!') #完毕

