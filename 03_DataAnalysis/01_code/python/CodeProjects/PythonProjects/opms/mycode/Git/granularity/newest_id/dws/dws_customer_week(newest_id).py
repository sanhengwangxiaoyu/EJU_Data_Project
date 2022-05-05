#In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 16:44:47 2021
@author: admin1
"""
import configparser,os,sys,pymysql,pandas as pd,getopt, re
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
date_quarter = '2021Q3'    #  获取季度（统计周期）
table_name = 'dws_customer_week'
database = 'dws_db_prd'

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


"""
正式代码
     根据增存量标识表，获取原表id，关联浏览日志表获取楼盘id，根据楼盘id关联楼0盘表获取城市id。并限制城市
"""


#In[3]:
# '2018Q1','2018Q2','2018Q3','2018Q4','2019Q1','2019Q2','2019Q3','2019Q4','2020Q2','2020Q3','2020Q4','2021Q1','2021Q2',
for date_quarter in ['2021Q4']:
      
  start_date = str(pd.to_datetime(date_quarter))[0:10]   #截取成yyyy-MM-dd
  end_date =  str(pd.to_datetime(date_quarter) + pd.offsets.QuarterEnd(0))[0:10]      #截取成yyyy-MM-dd

  #当月意向客户#
  # dwb_customer_browse_log  客户浏览楼盘日志表（每日增量） 
  #                                                      imei           客户号
  #                                                      visit_date     浏览日期  
  #                                                      newest_id      楼盘id
  ori=con.query("SELECT imei,newest_id FROM dwb_db.a_dwb_customer_browse_log where newest_id = '"+newest_id+"' and visit_date>='"+start_date+"' and visit_date<='"+end_date+"' ")


  # In[5]:
  #当前季度准入楼盘#
  #dws_newest_info  新房楼盘
  #                                                      newest_id       楼盘id
  #                                                      city_id         城市id  
  #                                                      county_id       区县id  
  admit=con.query("select newest_id from dws_db_prd.dws_newest_period_admit where newest_id = '"+newest_id+"' and dr = 0 and period='"+date_quarter+"'")
  newest_id=con.query("select newest_id,city_id from dws_db_prd.dws_newest_info where newest_id = '"+newest_id+"' and newest_id is not null and city_id in ('110000','120000','130100','130200','130600','210100','220100','310000','320100','320200','320300','320400','320500','320600','321000','330100','330200','330300','330400','330500','330600','331100','340100','350100','350200','360100','360400','360700','370100','370200','370300','370600','370800','410100','420100','430100','440100','440300','440400','440500','440600','441200','441300','441900','442000','450100','460100','460200','500000','510100','520100','530100','610100','610300','610400') and county_id is not null and county_id != '' group by newest_id,city_id,county_id")
  # 获取指定季度的楼盘id,和城市id，区县id
  admit = pd.merge(admit,newest_id, how='inner', on=['newest_id'])


  # In[3]:

  # dws_imei_browse_tag  客户浏览标签结果表
  #                                   imei            类似客户号的东西
  #                                   concern        关注
  #                                   intention      意向
  #                                   urgent         迫切
  #                                   cre            增存
  imeis = "'"+re.sub(r',','\', \'',','.join(ori['imei'].values.tolist()))+"'"
  ime = con.query(" SELECT imei,min(add_new_code) cre,visit_week period FROM dwb_db.dwb_customer_add_new_code where imei in ("+imeis+") and dr = '0' and visit_quarter = '"+date_quarter+"' group by imei,visit_week")


  # In[6]:
  # 获取楼盘的城市和区县，以及浏览基本情况#
  # ime[['id']]=ime['id'].astype(int)
  # ori[['id']]=ori['id'].astype(int)
  df = pd.merge(ime,ori ,how='left' ,on=['imei'])
  df = pd.merge(admit, df, how='inner', on=['newest_id'])
  # 修改列名
  df = df[['city_id','newest_id','period','imei','cre']]
  df.columns = ['city','newest_id','visit_week','imei','cre']


  # In[7]:
  df1_in_tmp =  df[df['cre'] == 0]
  df1_in_tmp = df1_in_tmp[['city','newest_id','visit_week','imei']].drop_duplicates()
  df1_increase = df1_in_tmp.groupby(['city','visit_week','newest_id'])['imei'].count().reset_index()
  df1_increase['exist'] = "增量"
  df1_re_tmp =  df[df['cre'] == 1]
  df1_re_tmp = df1_re_tmp[['city','newest_id','visit_week','imei']].drop_duplicates()
  df1_retained = df1_re_tmp.groupby(['city','newest_id','visit_week'])['imei'].count().reset_index()
  df1_retained['exist'] = "存量"


  # In[8]:
  cus_mon_res = pd.concat([df1_increase,df1_retained])
  cus_mon_res['period'] = date_quarter
  cus_mon_res = cus_mon_res[['city','newest_id','visit_week','exist','imei','period']]
  cus_mon_res.columns = ['city_id','newest_id','week','exist','imei_num','period']


  # In[9]:
  # cus_mon_res
  # 加载数据到dws_customer_month
  to_dws(cus_mon_res,table_name)

  print('>> load data Done!') #完毕


  #In[]
  ime_df1 = con.query("select city_id,newest_id,week,exist,sum(imei_num) imei_num,max(period) period from dws_db_prd.dws_customer_week where week in ('2021-26','2021-13','2020-53','2020-40','2020-27','2019-40','2019-1','2018-26','2018-13') group by city_id,newest_id,week,exist")

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
  update_sql1 = "delete from dws_db_prd.dws_customer_week where week in ('2021-26','2021-13','2020-53','2020-40','2020-27','2019-40','2019-1','2018-26','2018-13')"
  cur.execute(update_sql1)
  conn.commit() # 提交记
  conn.close() # 关闭数据库链接

  to_dws(ime_df1,'dws_customer_week')

  print('>>>ETL Done!!!!!!' + date_quarter)





