# -*- coding:utf-8 -*-  
__author__ = 'zhangxinming'
__date__ = '2021/4/9 20:21'

import requests,os
import re
import pandas as pd
from lxml import etree
from fake_useragent import UserAgent
from urllib.request import Request, urlopen
import time
import json
import urllib.parse
import random
from pandas import Series,DataFrame
import html
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


from threading import Thread
from queue import Queue
import traceback

urls = Queue()
okurl = set()
listfile='shenyang_url.txt'
outfile='shenyang.txt'

class sy_spider():
    def __init__(self):
        self.pool = requests.session()
        self.pool.mount('http://', HTTPAdapter(pool_connections=100, pool_maxsize=200,max_retries=Retry(total=10,backoff_factor=1,method_whitelist=frozenset(['GET', 'POST']))))
    def trim(self,word):
        return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')   
    def parse_one(self):
        for ii in range(1,145):
            ua = UserAgent()
            headers ={
                'User-Agent': ua.random,
                'Connection': 'close',
            }
            self.pool.headers.update(headers)
            lburl = 'http://124.95.133.164/work/xjlp/new_building.jsp?page='+str(ii)
            
            r = self.pool.get(lburl,timeout=60, headers=headers)
            time.sleep(0.1)
            # print(r.text)
            table1 = etree.HTML(r.content.decode('gbk'))
            table2 = table1.xpath('//table//table[3]//table//tr[position()>2]')
            print(len(table2))
            for x in table2:
                try:
                    url8 = ''.join(x.xpath(".//td[1]//a//@href"));
                    url2 = 'http://124.95.133.164'+url8
                    dict2 = {
                        "URL":url2 ,
                        "城市": '沈阳',
                        "项目名称": ''.join(x.xpath(".//td[1]//text()")).replace('\r\n','').replace('\t',''),
                        "坐落位置": ''.join(x.xpath(".//td[2]//text()")),
                        "开发企业": ''.join(x.xpath(".//td[3]//text()")),
                        "预售许可证编号": '',
                        "发证日期": '',
                        "开盘日期": ''.join(x.xpath(".//td[4]//text()")),
                        "预售证准许销售面积": '',
                        "销售状态": '',
                        "销售楼号": '',
                        "套数": '',
                        "面积": '',
                        "拟售价格": '',
                        "售楼电话": '',
                        "售楼地址": '',
                        "房号": '',
                        "房屋建筑面积": '',
                        "房屋销售状态": '',
                    }
                    print(url2)
                    headers = {
                        'User-Agent': ua.random,
                        'Connection': 'close',
                    }
                    self.pool.headers.update(headers)
                    r2 = self.pool.get(url2, timeout=60, headers=headers)
                    table3 = etree.HTML(r2.content.decode('gbk'))
                    table4 = table3.xpath('//table//table//table//tr[position()>1]')
                    for n in table4:
                        dict2["销售楼号"]=''.join(n.xpath(".//td[1]//text()")).replace("\xa0","")
                        dict2["套数"]=''.join(n.xpath(".//td[5]//text()")).replace("\xa0","")
                        url4 = ''.join(n.xpath(".//td[1]//a//@href"));
                        print(url4)
                        houseid = re.findall(r'houseid=(.*?)&', url4, re.I)[0]
                        url3='http://124.95.133.164/work/xjlp/door_list2.jsp?houseid='+houseid
                        print(url3)
                        headers = {
                        'User-Agent': ua.random,
                        'Connection': 'close',
                        }
                        self.pool.headers.update(headers)
                        r3 = self.pool.get(url3, timeout=60, headers=headers)
                        time.sleep(1)
                        table5 = etree.HTML(r3.content.decode('gbk'))
                        table6 = table5.xpath('//table//td')
                        ###
                        dict4={}
                        for m in table6:
                            url9=''.join(m.xpath(".//a/@href"))
                            if(url9==''):
                                continue
                            xszt = url9.split('&')[1].replace('xszt=','')
                            r4 = self.pool.get('http://124.95.133.164'+url9, timeout=60, headers=headers)
                            table7 = etree.HTML(r4.content.decode('gbk'))
                            table8 = table7.xpath('//table//table//table[2]//tr[8]//td[2]/text()')
                            dict3 = {
                               "房号": ''.join(m.xpath(".//text()")).replace('\r\n','').replace('\t','').strip(),
                               "房屋建筑面积": ''.join(table8).replace("\xa0",""),
                               "房屋销售状态": xszt,
                            }
                            dict4 = dict2.copy()
                            dict4.update(dict3)
                            df = DataFrame(dict4, index=[0])
                            df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                        if len(dict4)==0:
                            df = DataFrame(dict2, index=[0])
                            df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    
                except Exception as e:
                    print(e)
                    print("异常")
                    continue
    def main(self):
        self.parse_one()
        time.sleep(10)
run = sy_spider()
run.main()






##开始时间
print('start time -->   '+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
##指定输出
dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="guiyang_"+ dt + ".txt"
##指定本地的useragent，并创建useragent对象用作浏览
location = os.getcwd() + '\\fake_useragent.json'
ua = UserAgent(path=location)
##创建session对象
pool = requests.session()
##设置连接池大小
pool.mount('http://',HTTPAdapter(
  pool_connections=100, #连接池数量
  pool_maxsize=200,     #连接池允许的最大长连接数量
  max_retries=          #设置重试次数
    Retry(total=10,     #手动指定重试次数
      backoff_factor=1, #重试间隔时间:urllib3的backoff_factor算法
      method_whitelist=frozenset(['GET','POST']) # 设置 post()方法进行重访问
)))
# HTTP “请求头信息”
headers ={'User-Agent': ua.random,'Connection': 'close',}
pool.headers.update(headers)
lburl = 'http://124.95.133.164/work/xjlp/new_building.jsp?page=100'
r = pool.get(lburl,timeout=60, headers=headers)
# 转译编码
table1 = etree.HTML(r.content.decode('gbk'))
table2 = table1.xpath('//table//table[3]//table//tr[position()>2]')
table2[1].xpath(".//td[1]//a//@href")