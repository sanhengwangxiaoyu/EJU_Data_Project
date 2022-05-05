# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 16:44:47 2021
@author: admin1
"""
import configparser,os,sys,pymysql,pandas as pd,datetime,getopt
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta

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

def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database)
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

"""
传参
"""
date_quarter = '2021Q4'    #  获取季度（统计周期）
table_name = 'dws_customer_sum'
database = 'dws_db_prd'
newest_ids = ['3c2a249ea8fa11ec869c8c8caa44e774']


# In[]
# '2018Q1','2018Q2','2018Q3','2018Q4','2019Q1','2019Q2','2019Q3','2019Q4','2020Q2','2020Q3','2020Q4','2021Q1','2021Q2',
for date_quarter in ['2021Q4']:
    

  #意向客户总量#
  con = MysqlClient(db_host,database,user,password)
  # dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
  #                                                      imei           客户号
  #                                                      visit_date     浏览日期  
  #                                                      newest_id      楼盘id
  #                                                      pv             浏览次数  
  ori=con.query("select newest_id,intention,orien,urgent,increase,retained from dwb_db.dwb_newest_customer_info where dr = 0 and period='"+date_quarter+"'")
  # dws_newest_info  新房楼盘
  #                                                      newest_id       楼盘id
  #                                                      city_id         城市id  
  #                                                      county_id       区县id  
  # admit=con.query("select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 and period='"+date_quarter+"'")
  newest_id=con.query("select newest_id,city_id from dws_db_prd.dws_newest_info where newest_id is not null and city_id in ('110000','120000','130100','130200','130600','210100','220100','310000','320100','320200','320300','320400','320500','320600','321000','330100','330200','330300','330400','330500','330600','331100','340100','350100','350200','360100','360400','360700','370100','370200','370300','370600','370800','410100','420100','430100','440100','440300','440400','440500','440600','441200','441300','441900','442000','450100','460100','460200','500000','510100','520100','530100','610100','610300','610400') and county_id is not null and county_id != '' and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 and period='"+date_quarter+"') group by newest_id,city_id,county_id")
  # 获取指定季度的楼盘id,和城市id，区县id
  # admit = pd.merge(admit,newest_id, how='inner', on=['newest_id'])

  # newest_id = newest_id[newest_id['newest_id'] == 'ac3c524e6c6dafd5bd81cf10f3e9c89f']

  #In[]
  # 获取楼盘的城市和区县，以及浏览基本情况
  df = pd.merge(newest_id, ori, how='inner', on=['newest_id'])
  # 修改列名
  df.rename(columns={'newest_id':'newest'},inplace=True)
  cus_sum = df[['city_id','newest','intention']]
  # 修改列名
  cus_sum.columns=['city_name','newest_name','cou_imei']
  # 对城市名字分组，求取关注客户的平均值
  cus_sum_city=cus_sum.groupby('city_name')['cou_imei'].mean().reset_index()
  # 修改列名
  cus_sum_city.columns=['city_name','city_avg']
  # 均值对比：保留2位小数
  cus_sum_city['city_avg']=round(cus_sum_city['city_avg'],2)
  # 合并关注客户总量和关注客户均量比
  cus_sum_res=pd.merge(cus_sum,cus_sum_city,how="left",on=['city_name'])
  #  新增列tatio ，目标楼盘与城市平均比较  ==》 关注客户总量/关注客户的均量
  cus_sum_res['ratio']=round(cus_sum_res['cou_imei']/cus_sum_res['city_avg']-1,4)
  #  新增列，指定周期值
  cus_sum_res['period']= date_quarter
  #  截取指定列
  cus_sum_res=cus_sum_res[['city_name','newest_name','period','cou_imei','city_avg','ratio']]
  #  重命名列名
  cus_sum_res.columns=['city_id','newest_id','period','cou_imei','city_avg','ratio']
  cus_sum_res = cus_sum_res[cus_sum_res['newest_id'].isin(newest_ids)]

  # In[19]:

  #  插入数据到dws_customer_sum表中
  to_dws(cus_sum_res,table_name)#画像首页意向用户数总量


  print('>> Done!' + date_quarter) #完毕


  