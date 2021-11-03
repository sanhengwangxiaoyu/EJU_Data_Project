
import requests,json,requests,json,pandas as pd,os,pymysql
import configparser
from sqlalchemy import create_engine

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
con = MysqlClient(db_host,database,user,password)

def to_dws(result,table):
    engine = create_engine("mysql+pymysql://yangzhen:6V5_0rviExpxBzHj@172.28.36.77:3306/dws_db_prd?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)



newest = con.query("SELECT DISTINCT a.newest_id,a.newest_name,b.city_name,a.address FROM dws_newest_info a JOIN dws_db_prd.dim_geography b ON b.grade=3 AND b.city_id=a.city_id WHERE a.newest_id IN (SELECT DISTINCT newest_id FROM dws_newest_period_admit WHERE dr=0)  and a.newest_id not in (select newest_id from city_detail_baidu ) and newest_id  in (select newest_id from temp_db.newest_city_fialed_info)")

addresss = {}

url = 'https://restapi.amap.com/v3/geocode/geo'   # 输入API问号前固定不变的部分
params = { 'key': '61d7e8cf769016c6904b3cea7b719e3d',
           'address': '碧桂园星荟',
           'city': '贵阳市' }                # 将两个参数放入字典
res = requests.get(url, params)
res.text
jd = json.loads(res.text)      # 将json数据转化为Python字典格式
