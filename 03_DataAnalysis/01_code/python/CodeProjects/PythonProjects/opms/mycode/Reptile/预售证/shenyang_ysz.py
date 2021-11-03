# -*- coding:utf-8 -*-  
__author__ = 'zhangxinming'
__date__ = '2021/4/10 9:43'

import requests
import re
import pandas as pd
# import urllib.parse as urlparse
from lxml import etree
from fake_useragent import UserAgent
# from urllib.request import Request, urlopen
import time
import json
# import urllib.parse
import random
from pandas import Series,DataFrame
import html
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait

from threading import Thread
from queue import Queue
import traceback,os

urls = Queue()
yszlist = set()
listfile='list/shenyang_ysz_url.txt'
outfile='data/shenyang_ysz_new.txt'
okfile='data/shenyang_ysz.txt'

class nb_spider():
    # def __init__(self):
        # self.pool = requests.session()
        # self.pool.mount('http://', HTTPAdapter(pool_connections=100, pool_maxsize=200,max_retries=Retry(total=10,backoff_factor=1,method_whitelist=frozenset(['GET', 'POST']))))
    def getHtml(self,link):
        html=""
        # print(link)
        try: #使用try except方法进行各种异常处理
            header = {
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'+str(random.random()),
                'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2' ,
            } 
            res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
            #解码
            # print(link)
            if res.encoding=='utf-8' or res.encoding=='UTF-8':
                    res.encoding='UTF-8'
            else:
                    m = re.compile('<meta .*(http-equiv="?Content-Type"?.*)?charset="?([a-zA-Z0-9_-]+)"?', re.I).search(res.text)
                    if m and m.lastindex == 2:
                        charset = m.group(2).upper()
                        res.encoding=charset
                    else:
                        res.encoding='GBK'
            html=res.text
        except Exception as e:
            print(traceback.format_exc())
        finally:
            return html
    def trim(self,word):
        return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')  
    def parse_one(self):
        if os.path.exists(outfile):
            with open(outfile,'r',encoding='utf-8') as txtData: 
                for line in txtData.readlines():
                    yszlist.add(line.split('\t')[5])
        if os.path.exists(okfile):
            with open(okfile,'r',encoding='utf-8') as txtData: 
                for line in txtData.readlines():
                    if len(line.split('\t'))>5:
                        yszlist.add(line.split('\t')[5])
        listlist=[]
        if os.path.exists(listfile):
            with open(listfile,'r',encoding='utf-8') as txtData: 
                for line in txtData.readlines():
                    listlist.append(line.strip().split('\t')[0])    
                               
        ###
        of = open(listfile,'a+', encoding='utf-8') #保存结果文件
        #翻页获取项目列表range(1,185):125
        end='no'
        for ii in range(1,450):
            print('pages=',ii)
            try:
                ua = UserAgent()
                headers ={
                    'User-Agent': ua.random,
                    'Connection': 'close',
                }
                self.pool.headers.update(headers)
                lburl = 'http://124.95.133.164/work/ysxk/query_xukezheng.jsp?cur_page='+str(ii)
                while True:
                    # r = self.pool.get(lburl,timeout=60, headers=headers)
                    h1=self.getHtml(lburl)
                    if h1.find("请休息一会儿再访问")>0:
                        time.sleep(5)
                        print('lburl-sleep5')
                        continue
                    else:
                        break
                time.sleep(1)
                e1 = etree.HTML(h1)
                table2 = e1.xpath('//tr[@bgcolor="white"]')                
                print(lburl+'：'+str(len(table2)))
                for x in table2:                
                    url2 = 'http://124.95.133.164'+''.join(x.xpath('.//a/@href'))
                    sp=''.join(x.xpath('./td[2]/div/text()')).strip()
                    ysz=''.join(x.xpath('./td[3]/div/text()')).strip()
                    if ysz=='':
                        continue
                    if ysz in yszlist:
                        end='yes'
                        print('end',ysz)
                        break
                    if url2 in listlist:
                        continue
                    listlist.append(url2)
                    print('add',ysz)
                    of.write(url2+'\t'+sp+'\t'+ysz+'\n')
                    of.flush()
                ##########
                if end=='yes':
                    break
            except Exception as e:
                print(traceback.format_exc())
                continue
        #2多线程爬项目
        repeatlist=[]
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                if a[0] in repeatlist:
                    continue
                if len(a)==3 and a[2] not in yszlist:
                    urls.put(a[0])
                    repeatlist.append(a[0])
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
        ths = []
        for i in range(1):
            t = Thread(target=self.run, args=())
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
            # time.sleep(1111)
    def run(self):
        while urls.qsize() != 0:
            print("qsize less="+str(urls.qsize()))   
            url2=urls.get()
            print('url2=',url2)
            try:
                dict2 = {
                    "URL":url2,
                    "城市": '沈阳',
                    "项目名称": '',
                    "坐落位置": '',
                    "开发企业": '',
                    "预售许可证编号": '',
                    "发证日期": '',
                    "开盘日期": '',
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
                
                while True:
                    time.sleep(3)
                    h2=self.getHtml(url2)
                    if h2.find("请休息一会儿再访问")>0:
                        print('url2 sleep 5')
                        time.sleep(5)
                        continue
                    else:
                        break
                h2=h2.replace('&nbsp;','')
                tab=etree.HTML(h2)
                pro=''.join(re.findall('项目名称.*?FFFFFF">(.*?)<',h2,re.S)).strip()
                addr=''.join(re.findall('项目地址.*?FFFFFF">(.*?)<',h2,re.S)).strip()
                ldmc=''.join(tab.xpath('//table[@border="1"]/tr[5]/td[4]/text()')).strip()
                # beizhu=''.join(re.findall('备注: \S+.*?FFFFFF">(.*?)<',h2,re.S)).strip()
                beizhu=''.join(tab.xpath('//table[@border="1"]/tr[8]/td[2]/text()')).strip()
                beizhu=re.sub('\s+','|',beizhu)
                print(beizhu)
                dict2['项目名称']=beizhu
                if '见备注' in pro:
                    pro=beizhu
                if '见备注' in addr:
                    addr=beizhu
                if '见备注' in ldmc:
                    ldmc=beizhu
                dict2['坐落位置']=addr
                dict2['项目名称']=pro
                dict2['发证日期']=''.join(tab.xpath('//table[@border="1"]/tr[8]/td[4]/text()')).strip()
                dict2['预售证准许销售面积']=''.join(tab.xpath('//table[@border="1"]/tr[5]/td[2]/text()')).strip()
                dict2['销售楼号']=ldmc
                dict2['套数']=''.join(tab.xpath('//table[@border="1"]/tr[5]/td[5]/text()')).strip()
                df = DataFrame(dict2, index=[0])
                df.to_csv(outfile,sep='\t', mode='a', index=False, header=None)
                # time.sleep(12211)    
            except Exception as e:
                print(traceback.format_exc())
                continue

    def main(self):
        self.parse_one()
run = nb_spider()
run.main()