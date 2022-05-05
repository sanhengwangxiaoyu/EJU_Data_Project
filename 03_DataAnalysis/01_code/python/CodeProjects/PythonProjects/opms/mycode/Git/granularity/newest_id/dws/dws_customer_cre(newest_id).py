# In[1]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Jun 26 16:44:47 2021
Update on 2021-06-15 17:48
        修改imei号的状态来源，修改前自己判断、修改该后dws_imei_browse_tag
        修改城市id和区县id，修改前自己 dwb_newest_info ，修改后 dws_newest_info
        
"""
import configparser,os,sys,pymysql,pandas as pd,getopt
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
#意向客户总量#
con = MysqlClient(db_host,database,user,password)

"""
传参
"""
date_quarter = '2021Q4'    #  获取季度（统计周期）
table_name = 'dws_customer_cre'
newest_id = '3c2a249ea8fa11ec869c8c8caa44e774'


#In[3]:
# for date_quarter in ['2018Q1','2018Q2','2018Q3','2018Q4','2019Q1','2019Q2','2019Q3','2019Q4','2020Q2','2020Q3','2020Q4','2021Q1','2021Q2','2021Q3']:
for date_quarter in ['2021Q4']:
  # start_date = str(pd.to_datetime(date_quarter))[0:10]   #截取成yyyy-MM-dd
  # end_date =  str(pd.to_datetime(date_quarter) + pd.offsets.QuarterEnd(0))[0:10]      #截取成yyyy-MM-dd
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
  update_sql1 = "insert into dws_db_prd.dws_customer_cre(city_id,newest_id,exist,imei_num,period) select b.city_id,a.* from (select newest_id,'增量' exist,increase imei_num,period from dwb_db.dwb_newest_customer_info where dr=0 and period='"+date_quarter+"' and newest_id = '"+newest_id+"' union all select newest_id,'存量' exist,retained imei_num,period from dwb_db.dwb_newest_customer_info where dr=0 and period='"+date_quarter+"' and newest_id = '"+newest_id+"' ) a inner join (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where period = '"+date_quarter+"' group by period,newest_id) and city_id in ('110000','120000','130100','130200','130600','210100','220100','310000','320100','320200','320300','320400','320500','320600','321000','330100','330200','330300','330400','330500','330600','331100','340100','350100','350200','360100','360400','360700','370100','370200','370300','370600','370800','410100','420100','430100','440100','440300','440400','440500','440600','441200','441300','441900','442000','450100','460100','460200','500000','510100','520100','530100','610100','610300','610400') and county_id is not null and county_id != '' group by newest_id,city_id,county_id) b on a.newest_id=b.newest_id where b.city_id is not null"
  cur.execute(update_sql1)
  conn.commit() # 提交记
  conn.close() # 关闭数据库链接
  print('>>> Done' + date_quarter)


