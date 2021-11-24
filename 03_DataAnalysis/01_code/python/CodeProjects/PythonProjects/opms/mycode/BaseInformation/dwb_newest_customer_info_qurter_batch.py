# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jul 12 17:44:47 2021
  楼盘 意向人数 定向人数 迫切人数 增量人数  存量人数

"""
import configparser,getopt,os,sys,pymysql,pandas as pd,numpy as np,time
from datetime import date
from collections import Counter
from sqlalchemy import create_engine

##设置配置信息##
pymysql.install_as_MySQLdb()
##读取配置文件##
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块
##设置变量初始值##
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
# date_quarter = '2020Q4'   # 季度
table_name = 'dwb_newest_customer_info' # 要插入的表名称
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


##正式代码##
"""
1> 获取数据信息：dws_newest_info，dws_newest_period_admit
    通过admit筛选准入楼盘信息，通过dws_newest_info获取楼盘具体信息
"""
con = MysqlClient(db_host,database,user,password)




# In[2]:
##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
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


# In[4]:
for date_quarter in ['2021Q2','2021Q3']:
      
  ##重置时间格式
  start_date = str(pd.to_datetime(date_quarter))[0:7]   #截取成yyyy-MM-dd
  end_date =  str(pd.to_datetime(date_quarter) + pd.offsets.QuarterEnd(0))[0:7]      #截取成yyyy-MM-dd
  start_date = start_date.replace('-','')
  end_date = end_date.replace('-','')
  # a_dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
  #                                                      imei          客户号
  #                                                      newest_id     楼盘id  
  #                                                      visit_month   浏览月份 
  #                                                      visit_date    浏览日期 
  # newest_imei=con.query("SELECT newest_id,imei,visit_month,visit_date FROM dwb_db.a_dwb_customer_browse_log where visit_date>='"+start_date+"' and visit_date<='"+end_date+"'")
  newest_imei=con.query("SELECT newest_id,imei,visit_month FROM dwb_db.dwb_customer_lookest_list_m where visit_month>='"+start_date+"' and visit_month<='"+end_date+"'")
  # newest_imei=con.query("SELECT id,newest_id FROM dwb_db.a_dwb_customer_browse_log where visit_date>='"+start_date+"' and visit_date<='"+end_date+"' and newest_id = 'dc79f4b4efc0fab54d5b42f9c2f913e9'")


  # In[5]:
  # dws_imei_browse_tag  客户浏览标签结果表
  #                                   imei            客户号
  #                                   concern        关注
  #                                   intention      意向
  #                                   urgent         迫切
  #                                   cre            增存
  browse_tag = con.query('''select imei,concern,intention,urgent,cre from dws_db_prd.dws_imei_browse_tag where period = "'''+date_quarter+'''"''')
  # 合并表
  df = pd.merge(newest_imei,browse_tag,how='left',on=['imei'])


  # In[6]:
  ########################===============季度=============#######################
  # 意向人数
  df1_intention = df[['newest_id','imei']].drop_duplicates()
  df1_intention = df1_intention.groupby(['newest_id'])['imei'].count().reset_index()
  # 定向人数
  df1_orien =  df[df['intention'] == '意向']
  df1_orien = df1_orien[['newest_id','imei']].drop_duplicates()
  df1_orien = df1_orien.groupby(['newest_id'])['imei'].count().reset_index()
  # 迫切人数
  df1_urgent =  df[df['urgent'] == '迫切']
  df1_urgent = df1_urgent[['newest_id','imei']].drop_duplicates()
  df1_urgent = df1_urgent.groupby(['newest_id'])['imei'].count().reset_index()
  # 增量人数
  df1_increase =  df[df['cre'] == '增长']
  df1_increase = df1_increase[['newest_id','imei']].drop_duplicates()
  df1_increase = df1_increase.groupby(['newest_id'])['imei'].count().reset_index()
  # 存量人数
  df1_retained =  df[df['cre'] == '活跃']
  df1_retained = df1_retained[['newest_id','imei']].drop_duplicates()
  df1_retained = df1_retained.groupby(['newest_id'])['imei'].count().reset_index()
  # 修改列名
  df1_intention.columns = ['newest_id','intention']
  df1_orien.columns = ['newest_id','orien']
  df1_urgent.columns = ['newest_id','urgent']
  df1_increase.columns = ['newest_id','increase']
  df1_retained.columns = ['newest_id','retained']
  # 合并拼接
  df1 = pd.merge(df1_intention,df1_orien,how='left',on=['newest_id'])
  df1 = pd.merge(df1,df1_urgent,how='left',on=['newest_id'])
  df1 = pd.merge(df1,df1_increase,how='left',on=['newest_id'])
  df1 = pd.merge(df1,df1_retained,how='left',on=['newest_id'])
  # 列赋值
  df1['period']=date_quarter
  df1['quarter']=date_quarter
  df1['dr'] = 0
  df1['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
  df1['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 


  # In[7]:
  ########################===============月份=============#######################
  # 意向人数
  df2_intention = df[['newest_id','visit_month','imei']].drop_duplicates()
  df2_intention = df2_intention.groupby(['newest_id','visit_month'])['imei'].count().reset_index()
  # 定向人数
  df2_orien =  df[df['intention'] == '意向']
  df2_orien = df2_orien[['newest_id','visit_month','imei']].drop_duplicates()
  df2_orien = df2_orien.groupby(['newest_id','visit_month'])['imei'].count().reset_index()
  # 迫切人数
  df2_urgent =  df[df['urgent'] == '迫切']
  df2_urgent = df2_urgent[['newest_id','visit_month','imei']].drop_duplicates()
  df2_urgent = df2_urgent.groupby(['newest_id','visit_month'])['imei'].count().reset_index()
  # 增量人数
  # df2_increase =  df[df['cre'] == '增长']
  df2_increase =  df[df['cre'] == 'NULL']
  df2_increase = df2_increase[['newest_id','visit_month','imei']].drop_duplicates()
  df2_increase = df2_increase.groupby(['newest_id','visit_month'])['imei'].count().reset_index()
  # 存量人数
  # df2_retained =  df[df['cre'] == '活跃']
  df2_retained =  df[df['cre'] == 'NULL']
  df2_retained = df2_retained[['newest_id','visit_month','imei']].drop_duplicates()
  df2_retained = df2_retained.groupby(['newest_id','visit_month'])['imei'].count().reset_index()
  # 修改列名
  df2_intention.columns = ['newest_id','period','intention']
  df2_orien.columns = ['newest_id','period','orien']
  df2_urgent.columns = ['newest_id','period','urgent']
  df2_increase.columns = ['newest_id','period','increase']
  df2_retained.columns = ['newest_id','period','retained']
  # 合并拼接
  df2 = pd.merge(df2_intention,df2_orien,how='left',on=['newest_id','period'])
  df2 = pd.merge(df2,df2_urgent,how='left',on=['newest_id','period'])
  df2 = pd.merge(df2,df2_increase,how='left',on=['newest_id','period'])
  df2 = pd.merge(df2,df2_retained,how='left',on=['newest_id','period'])
  # 列赋值
  df2['quarter']=date_quarter
  df2['dr'] = 0
  df2['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
  df2['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 


  # In[8]:
  #季度月度合并
  df1 = df1[['newest_id','period','quarter','intention','orien','urgent','increase','retained','dr','create_time','update_time']]
  df2 = df2[['newest_id','period','quarter','intention','orien','urgent','increase','retained','dr','create_time','update_time']]
  result = df1.append(df2,ignore_index=True)
  # result = df1
  #空为0 
  result.at[result['intention'].isna(),'intention']=0
  result.at[result['orien'].isna(),'orien']=0
  result.at[result['urgent'].isna(),'urgent']=0
  result.at[result['increase'].isna(),'increase']=0
  result.at[result['retained'].isna(),'retained']=0


  # In[9]:
  # 加载到新表 dws_newest_investment_pop_rownumber_quarter
  result.drop_duplicates(inplace=True)
  to_dws(result,table_name)
  # grouped.to_csv('C:\\Users\\86133\\Desktop\\dws_newest_investment_pop_top30_quarter.csv')
  # result
  print(date_quarter+'  >>>>>>>Done')

