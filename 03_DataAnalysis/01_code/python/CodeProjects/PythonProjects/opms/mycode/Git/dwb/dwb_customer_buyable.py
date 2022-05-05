# In[]
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Oct 27 15:44:47 2021
    买房类型表: 本意是作为投资型榜单重跑节约时间

"""
import configparser,os,sys,pymysql,pandas as pd,getopt,time
from sqlalchemy import create_engine

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
database = 'dwb_db'
date_quarter = '2021Q4'   # 季度
table_name = 'dwb_customer_buyable' # 要插入的表名称
input_table_name = 'b_dwb_customer_imei_tag' # 要插入的表名称

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
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database)
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)
con = MysqlClient(db_host,database,user,password)   # 创建mysql链接


# In[]
##通过输入的参数的方式获取变量值##  如果不需要使用输入参数的方式，可以不用这段
opts,args=getopt.getopt(sys.argv[1:],"t:q:d:it:",["database=","table=","quarter=","input_table="])
for opts,arg in opts:
  if opts=="-t" or opts=="--table": # 获取输入参数 -t或者--table 后的值
    table_name = arg
  elif opts=="-q" or opts=="--quarter":  # 获取输入参数 -1或者--quarter 后的值
    date_quarter = arg
  elif opts=="-d" or opts=="database":
    database = arg
  elif opts=="-it" or opts=="input_table":
    input_table_name = arg




##################################正式代码###############################################
##################################  || ###############################################
##################################  V  ###############################################


# In[]:
# dwb_customer_imei_tag   客户单体标签
#                                                      imei       客户号
#                                                      buyable  type       投资类型
#                                                      is_college_stu    在校大学生
#                                                      period      季度
#                                                      marriage    婚姻状态
#                                                      education   教育水平
#                                                      have_child  有孩子
tag = con.query("select max(id) ods_id,'dwb_db.b_dwb_customer_imei_tag' ods_table_name, imei,'"+date_quarter+"' visit_quarter,0 buyable,0 dr,now() create_time,now() update_time from dwb_db."+input_table_name+" where is_college_stu = '否' and marriage = '已婚' and education = '高' and have_child = '有' group by imei")
tag['ods_table_name'] = "dwb_db."+input_table_name
tag['visit_quarter'] = date_quarter



# In[4]:
to_dws(tag,table_name)
# grouped.to_csv('C:\\Users\\86133\\Desktop\\dws_newest_investment_pop_top30_quarter.csv')
print('>> Done!')


