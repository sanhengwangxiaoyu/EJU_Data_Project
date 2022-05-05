# test
# %%

import pandas as pd, numpy as np ,os, pymysql, configparser, requests, warnings, random, re
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings("ignore")
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
    engine = create_engine("mysql+pymysql://wanganming:NDR_AhfzXT3MSxfh@172.28.36.77:3306/dwb_db?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

dic ={
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
}


#In[]

df_sche_q = con.query("select newest_id from dwb_db.dwb_bk_web_issue")

df_null_newest = con.query("select newest_id from odsdb.bk_newest_null")

df_newest = con.query("select newest_id from dws_db_prd.dws_newest_info dni group by newest_id")

df_55city = con.query("select city_name, city_py city from dwb_db.dwb_dim_geography_55city ddgc group by city_name, city_py")

df = con.query("select newest_id, newest_name, href, bk_newest_name, city_name from odsdb.bk_newest_basics_history bnbh group by newest_id, newest_name, href, bk_newest_name, city_name")

df = df[(~df['newest_id'].isin(df_newest['newest_id']))&(df['city_name'].isin(df_55city['city_name']))&(~df['newest_id'].isin(df_null_newest['newest_id']))&(~df['newest_id'].isin(df_sche_q['newest_id']))]

df2 = pd.merge(df, df_55city, how='left', on=['city_name'])


print("剩余未爬取楼盘动态"+str(len(df)))



#In[]
# df2 = df2[df2['newest_name'] == '城投保利创智中心']
# df2 = df2.head(2)
   

    
# 解析预售证 class table 类型 直接用pandas爬 返回一对多关系 数据放置table3中
def respGet(url,headers,data,res_type,proxies) :
    response = requests.get(url,headers=headers,data=data,proxies=proxies,timeout=15,verify=False) #读取网页源码
    if (response.text).find('人机身份认证') != -1:
            proxiess.remove(proxies)
    print("当前代理 "+re.sub('{\'https\': \'http://|\'},|\'}','',str(proxies))+" 已经失效,并且删除列表中的当前代理. 剩余代理有效个数"+str(len(proxiess)))
    response.encoding='UTF-8'#解码
    response.close()
    if res_type == 'json' :
        return response.json()
    else :
        return response.text

def load_data_from_web(i):
    if len(proxiess) == 0 :
        print("代理已用完")
    proxies = random.choice(proxiess)
    data = {}
    table3 = pd.DataFrame()
    try:
        newest_name = df2.iloc[i].newest_name
        bk_newest_name = df2.iloc[i].bk_newest_name
        newest_id = df2.iloc[i].newest_id
        href = df2.iloc[i].href
        dff = pd.read_html(respGet(href+"xiangqing",dic,data,'text',proxies))[0]
        dff['newest_id'] = newest_id
        dff['href'] = href
        dff['newest_name'] = newest_name
        dff['bk_newest_name'] = bk_newest_name
        print('预售证    '+newest_name)
        table3 = table3.append(dff)
        table3.drop('网上销控表',axis=1,inplace=True)
        table3.rename(columns={'售卖资格':'issue_code','发证时间':'issue_date','绑定楼栋':'sale_building','href':'url','bk_newest_name':'search_result'},inplace=True)
        table3.drop_duplicates(inplace=True)  
        table3['quater'] = table3.apply(lambda x: pd.Period(x['issue_date'],'Q'),axis=1)
        table3 = table3[(table3['issue_date']!='暂无信息')]
        to_dws(table3,'dwb_bk_web_issue')
        print('bk_newest_issue 结束')
        
    except Exception as e: 
        print(e)
        proxiess.remove(proxies)
        # time.sleep(10)
        print('没预售证    '+newest_name)

    



if __name__ == '__main__':
    proxiess = [{'https': 'http://59.59.165.169:4340'},
 {'https': 'http://125.78.227.151:4331'},
 {'https': 'http://171.95.154.175:4384'},
 {'https': 'http://27.159.190.83:4331'},
 {'https': 'http://117.31.29.164:4331'},
 {'https': 'http://120.39.142.182:4313'},
 {'https': 'http://115.208.184.65:4331'},
 {'https': 'http://113.239.152.220:4378'},
 {'https': 'http://113.243.33.121:4331'},
 {'https': 'http://115.209.73.142:4326'},
 {'https': 'http://183.165.248.164:4310'},
 {'https': 'http://113.124.219.113:4334'},
 {'https': 'http://114.99.6.72:4331'},
 {'https': 'http://121.233.206.55:4312'},
 {'https': 'http://58.46.251.146:4331'},
 {'https': 'http://120.34.216.245:4313'},
 {'https': 'http://120.34.17.214:4354'},
 {'https': 'http://59.59.162.24:4316'},
 {'https': 'http://125.78.219.0:4320'},
 {'https': 'http://60.20.198.2:4313'},
 {'https': 'http://124.161.204.131:4321'},
 {'https': 'http://59.58.148.180:4345'},
 {'https': 'http://113.241.139.30:4331'},
 {'https': 'http://59.58.47.53:4313'},
 {'https': 'http://125.111.151.202:4345'},
 {'https': 'http://120.37.2.83:4352'},
 {'https': 'http://125.87.93.26:4378'},
 {'https': 'http://42.177.138.180:4331'},
 {'https': 'http://115.209.175.116:4356'},
 {'https': 'http://115.209.108.197:4313'},
 {'https': 'http://182.204.183.42:4331'},
 {'https': 'http://49.88.149.50:4331'},
 {'https': 'http://223.156.86.211:4331'},
 {'https': 'http://1.31.97.159:4331'},
 {'https': 'http://183.162.159.179:4335'},
 {'https': 'http://183.165.251.26:4354'},
 {'https': 'http://113.218.239.37:4331'},
 {'https': 'http://114.239.210.235:4331'},
 {'https': 'http://175.162.223.65:4315'},
 {'https': 'http://117.34.192.15:4330'},
 {'https': 'http://113.243.33.60:4343'},
 {'https': 'http://115.207.18.64:4378'},
 {'https': 'http://182.204.158.221:4331'},
 {'https': 'http://27.156.199.40:4331'},
 {'https': 'http://115.213.236.188:4331'},
 {'https': 'http://106.32.10.177:4331'},
 {'https': 'http://114.99.8.113:4378'},
 {'https': 'http://113.218.243.18:4331'},
 {'https': 'http://175.151.103.38:4331'},
 {'https': 'http://115.208.231.136:4313'},
 {'https': 'http://110.87.249.208:4345'},
 {'https': 'http://121.29.81.23:4331'},
 {'https': 'http://175.155.50.241:4358'},
 {'https': 'http://175.0.112.212:4331'},
 {'https': 'http://125.111.146.200:4313'},
 {'https': 'http://27.156.213.202:4332'},
 {'https': 'http://222.77.212.241:4306'},
 {'https': 'http://222.242.136.146:4335'},
 {'https': 'http://59.58.49.109:4313'},
 {'https': 'http://116.115.208.101:4331'},
 {'https': 'http://121.29.82.238:4331'},
 {'https': 'http://59.58.43.174:4335'},
 {'https': 'http://106.110.198.100:4345'},
 {'https': 'http://121.205.218.254:4358'},
 {'https': 'http://114.99.197.184:4316'},
 {'https': 'http://175.149.62.123:4316'},
 {'https': 'http://60.20.202.168:4313'},
 {'https': 'http://171.95.154.149:4384'},
 {'https': 'http://125.87.81.55:4378'},
 {'https': 'http://113.221.13.17:4331'},
 {'https': 'http://125.87.83.69:4313'},
 {'https': 'http://120.33.7.13:4331'},
 {'https': 'http://113.218.244.207:4331'},
 {'https': 'http://111.175.84.23:4331'},
 {'https': 'http://114.99.201.11:4389'},
 {'https': 'http://113.231.39.214:4350'},
 {'https': 'http://60.185.23.56:4345'},
 {'https': 'http://183.150.81.246:4331'},
 {'https': 'http://36.62.243.95:4345'},
 {'https': 'http://118.123.41.139:4331'},
 {'https': 'http://49.89.87.123:4345'},
 {'https': 'http://125.87.95.175:4378'},
 {'https': 'http://120.42.150.237:4352'},
 {'https': 'http://113.241.137.48:4330'},
 {'https': 'http://125.87.85.233:4356'},
 {'https': 'http://110.83.12.44:4331'},
 {'https': 'http://121.226.39.173:4331'},
 {'https': 'http://113.243.32.184:4343'},
 {'https': 'http://113.141.223.60:4331'},
 {'https': 'http://117.93.115.219:4332'},
 {'https': 'http://182.204.181.32:4331'},
 {'https': 'http://114.106.136.53:4345'},
 {'https': 'http://117.34.192.244:4330'},
 {'https': 'http://182.128.45.71:4331'},
 {'https': 'http://175.146.70.178:4331'},
 {'https': 'http://101.205.82.49:4331'},
 {'https': 'http://182.204.158.43:4331'},
 {'https': 'http://122.241.223.10:4331'},
 {'https': 'http://117.92.125.116:4360'},
 {'https': 'http://183.165.195.54:4317'},
 {'https': 'http://121.226.39.99:4331'},
 {'https': 'http://115.211.32.181:4331'}]
    

 
    pool = ThreadPoolExecutor(max_workers=100)
    with pool as t :
        for i in range(len(df2.index)):
            t.submit(load_data_from_web,i)
            # table3 = table3.append()







