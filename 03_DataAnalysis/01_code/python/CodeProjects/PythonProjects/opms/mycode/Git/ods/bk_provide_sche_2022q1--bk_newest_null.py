#In[]
import sys, pandas as pd, numpy as np, os, re, pymysql, configparser, requests, time, random, warnings
from turtle import onclick
from typing import Tuple
from pandas.core.algorithms import isin
from pandas.core.frame import DataFrame
from pandas.io import html
from requests.api import head
from sqlalchemy import create_engine
from difflib import SequenceMatcher#导入库
from lxml import etree 

warnings.filterwarnings("ignore")
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
dic ={ "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36" }

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

"""
2022-03-28 动态爬取
   1、 查询出odsdb中不存在newest_info中的楼盘
   2、 爬取这些楼盘的动态
"""
con = MysqlClient(db_host,database,user,password)


#In[]  动态爬取
# df00 = con.query("select distinct newest_id from dws_db_prd.dws_newest_period_admit where dr=0 and period='2021Q4' and browse='A'")
# df_list0 = list(df00.newest_id)

# df11 = con.query("select newest_id from dws_db_prd.dws_customer_sum where period='2021Q4' and cou_imei>=60")
# df_list1 = list(df11.newest_id)

# df22 = con.query("select newest_id from odsdb.bk_provide_sche_q4")
# df_list2 = list(df22.newest_id)

# df_list3 = con.query("select newest_id from odsdb.bk_newest_null")
# df_list3 = list(df33.newest_id)

# df = con.query(''' 
# select distinct a.newest_id,a.newest_name,b.city_py city,b.city_name
# from dws_db_prd.dws_newest_info a
# left join dwb_db.dwb_dim_geography_55city b on b.city_id=a.city_id
# ''')
# df = df[(df['newest_id'].isin(df_list0))&(df['newest_id'].isin(df_list1))&(-df['newest_id'].isin(df_list2))&(-df['newest_id'].isin(df_list3))]


df_sche_q = con.query("select newest_id from odsdb.bk_provide_sche_2022q1")

df_null_newest = con.query("select newest_id from odsdb.bk_newest_null")

df_newest = con.query("select newest_id from dws_db_prd.dws_newest_info dni group by newest_id")

df_55city = con.query("select city_name, city_py city from dwb_db.dwb_dim_geography_55city ddgc group by city_name, city_py")

df = con.query("select newest_id, newest_name ,city_name from odsdb.bk_newest_basics_history bnbh group by newest_id, newest_name, city_name")

df = df[(~df['newest_id'].isin(df_newest['newest_id']))&(df['city_name'].isin(df_55city['city_name']))&(~df['newest_id'].isin(df_null_newest['newest_id']))&(~df['newest_id'].isin(df_sche_q['newest_id']))]

df = pd.merge(df, df_55city, how='left', on=['city_name'])


print("剩余未爬取楼盘动态"+str(len(df)))



#In[]
# aa = "剩余未爬取楼盘动态"+str(len(df))
# index_list = ['%d' %i for i in range(df.shape[0]) if i>56655 ]
# df = df.iloc[index_list]
# df = df.head(1)
df_erros = pd.DataFrame(columns=('proxies','comm'))
table_null = pd.DataFrame()
df1 = pd.DataFrame(columns=('newest_id','newest_name','bk_name','dongtai','provide_title','date','provide_sche'))
return_name = ""
dongtai_url = ""
proxiess = [{"https": "http://121.29.81.179:4331"},
{"https": "http://182.204.158.172:4331"},
{"https": "http://140.224.152.189:4313"},
{"https": "http://140.224.185.218:4326"},
{"https": "http://115.209.175.62:4356"},
{"https": "http://175.174.182.67:4343"},
{"https": "http://182.204.181.6:4331"},
{"https": "http://220.185.0.157:4334"},
{"https": "http://42.57.91.241:4331"},
{"https": "http://42.179.150.17:4331"},
]


for i in range(len(df.index)):
    try :
        if len(proxiess) == 0 :
            print("代理已用完")
            break
        proxies = random.choice(proxiess)
        print(df.iloc[i].newest_id+"    "+df.iloc[i].newest_name+"    第"+str(i+1)+"个楼盘,使用代理----> "+str(proxies))
        requests.adapters.DEFAULT_RETRIES = 5 # 增加重连次数
        s = requests.session()
        s.keep_alive = False # 关闭多余连接
        newest_id = df.iloc[i].newest_id
        newest_name = df.iloc[i].newest_name
        city_name = df.iloc[i].city_name
        city = df.iloc[i].city
        newest_name = df.iloc[i].newest_name
        url = 'https://'+city+'.fang.ke.com/loupan/rs'+newest_name+'/'
        resp = requests.get(url,headers=dic,verify=False,proxies=proxies,timeout=10)
        # resp = requests.get(url,headers=dic,verify=False,timeout=10)
        # time.sleep(8)
        if (resp.text).find('人机身份认证') != -1:
            proxiess.remove(proxies)
            print("当前代理 "+re.sub('{\'https\': \'http://|\'},|\'}','',str(proxies))+" 已经失效,并且删除列表中的当前代理. 剩余代理有效个数"+str(len(proxiess)))
            continue
        html = etree.HTML(resp.text)
        
        num = html.xpath('/html/body/div[6]/div[2]/span[2]/text()')[0]
        # print(num)
        #拿到每个找到的类,由于准确找到与猜你喜欢都存放在同一类下,所以不可直接循环,需先找到指定长度,指定长度采用前端返回的数值做循环定长
        # try:
        if num == '0':
            # num = [0 for x in range(0,int(num[0]))]
            pass
            print('未找到该楼盘'+" "+newest_id+"    "+newest_name)
            df_re11 = pd.DataFrame([[newest_id,newest_name,city_name]],columns=('newest_id','newest_name','city_name'))
            table_null = table_null.append(df_re11)
            # time.sleep(7)
        for v in range(int(num[0])) :
            v = str(v+1)
            # print('找到'+v)
            #找到查到的疑似目标楼盘的href
            # href = html.xpath('/html/body/div[6]/ul[2]/li['+v+']/a/@href')
            bk_name = html.xpath("/html/body/div[6]/ul[2]/li['"+v+"']/div/div[1]/a/text()")[0]
            href = html.xpath("/html/body/div[6]/ul[2]/li['"+v+"']/div/div[1]/a/@href")[0]
            dongtai = 'https://'+city+'.fang.ke.com'+href+'dongtai'
            # url = 'sh.fang.ke.com/loupan/p_xdthscyzbjyir/dongtai'
            resp1 = requests.get(dongtai,headers=dic,proxies=proxies,timeout=20)
            return_name = bk_name
            dongtai_url = dongtai
            # resp1 = requests.get(dongtai,headers=dic,timeout=20)
            # time.sleep(10)
            # requests.adapters.DEFAULT_RETRIES = 5 # 增加重连次数
            # s = requests.session()
            # s.keep_alive = False # 关闭多余连接
            html1 = etree.HTML(resp1.text)
            urls = html1.xpath('/html/body/div[2]/div/div')[2:]
            if len(urls) == 0 :
                print("无动态,跳过此楼盘"+dongtai)
                provide_title = '暂无'
                date = '暂无'
                provide_sche = '暂无'
                dff = pd.DataFrame([[newest_id,newest_name,bk_name,dongtai,provide_title,date,provide_sche]],columns=('newest_id','newest_name','bk_name','dongtai','provide_title','date','provide_sche'))
                df1 = df1.append(dff)
                break
            if v == '1' :
                print(newest_id+"    "+newest_name+"    楼盘动态url: "+dongtai)
            for div in urls:
                if len(div.xpath('./a/span[2]/text()')) == 0 :
                    continue 
                elif len(div.xpath('./a/span[3]/text()')) == 0:
                    continue
                elif re.sub('年|月','-',div.xpath('./a/span[3]/text()')[0]).replace('日','') <= '2018-01-01':
                    print("此动态时间在2018年之前,继续读取下一条")
                    continue
                try:
                    provide_title = div.xpath('./a/span[2]/text()')[0]
                    date = div.xpath('./a/span[3]/text()')[0]
                    provide_sche = div.xpath('./div/div[1]/p/text()')[0]
                    dff = pd.DataFrame([[newest_id,newest_name,bk_name,dongtai,provide_title,date,provide_sche]],columns=('newest_id','newest_name','bk_name','dongtai','provide_title','date','provide_sche'))
                    df1 = df1.append(dff)
                    resp1.close()
                except Exception as e: 
                    print("出现异常: "+str(e)) 
                    # time.sleep(2)
            resp.close()
            resp1.close()
        ##没有新增楼盘动态的数据
        if len(df1[df1['newest_id'].isin([newest_id])]) == 0 :
            print("网页无相关信息,填充暂无,放入表中")
            provide_title = "暂无"
            date = "暂无"
            provide_sche = "暂无"
            dff = pd.DataFrame([[newest_id,newest_name,return_name,dongtai_url,provide_title,date,provide_sche]],columns=('newest_id','newest_name','bk_name','dongtai','provide_title','date','provide_sche'))
            df1 = df1.append(dff)
            # print(df1)
    except Exception as e :
        
        # s = re.sub(r'\W|[a-zA-Z]|\d','',str(e))
        if len(re.findall(r'[， , . 。！ !][^， , . 。！ !]*Err.*?',str(e))) == 0:
            print("异常跳过 : ["+str(e)+"] ")
        else :
            s = re.findall(r'[， , . 。！ !][^， , . 。！ !]*Err.*?',str(e))[0]
            print("异常跳过 : ["+s+"] ")
            df_error_new = pd.DataFrame([[proxies,str(e)]],columns=('proxies','comm'))
            df_erros = df_erros.append(df_error_new)
            df_erros_proxies = df_erros[df_erros['proxies']==proxies]
        if len(df_erros_proxies)>5:
            print("异常次数太多,随认为代理失效")
            proxiess.remove(proxies)
        time.sleep(3)
        print("代理有效个数"+str(len(proxiess)))
        
            
# def to_dws(result,table):
#     engine = create_engine("mysql+pymysql://yangzhen:6V5_0rviExpxBzHj@172.28.36.77:3306/odsdb?charset=utf8")
#     result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)


#In[]
df1 = df1.drop_duplicates()
database = 'odsdb'
to_dws(df1,'bk_provide_sche_2022q1')
to_dws(table_null,'bk_newest_null')



