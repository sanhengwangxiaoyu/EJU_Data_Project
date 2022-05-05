# -*- coding:utf-8 -*-  
__date__ = '2021/4/8 19:37'

import requests
import re
import pandas as pd
from lxml import etree
from fake_useragent import UserAgent
from urllib.request import Request, urlopen
import time,os
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
listfile='jiaxing_url.txt'
outfile='jiaxing.txt'

class jx_spider():
    # def __init__(self):
        # self.pool = requests.session()
        # self.pool.mount('http://', HTTPAdapter(pool_connections=100, pool_maxsize=200,max_retries=Retry(total=10,backoff_factor=1,method_whitelist=frozenset(['GET', 'POST']))))
    def postHtml(self,url,data):
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
            'Content-Type':'application/json',
            'Referer':url,
            'Cache-Control':'no-cache'
        }
        try:
            res=requests.post(url=url,data=data,headers=header,verify=False)
            res.encoding='UTF-8'
            return res.text
        except Exception as e:
            print(traceback.format_exc())
        return ''
    def parse_one(self):
        #1翻页获取项目列表range(1,310):
        page
        for nn  in range(1,1):
            #http://220.191.220.105/web/WebRegistration_selectProject.jspx?cq=&keywords=&page=2
            dictdata={
                'cq':'',
                'keywords':'',
                'page': nn
            }
            ua = UserAgent()
            headers ={
                'User-Agent': ua.random,
                'Connection': 'close',
            }
            self.pool.headers.update(headers)
            lburl = 'http://220.191.220.105/web/WebRegistration_selectProject.jspx'
            r = self.pool.post(lburl,dictdata,timeout=60, headers=headers).json().get("list")
            # time.sleep(0.1)
            for i in r:
                # print(i)
                try:
                    print(i.get("xmmc",""))
                    dict2 = {
                        "URL":'http://220.191.220.105/WebRegistration_toProjectinfo.jspx?projecetid='+str(i.get("projecetid","")),
                        "城市": '嘉兴',
                        "项目名称": i.get("xmmc",""),
                        "坐落位置": i.get("address",""),
                        "开发企业": i.get("developername",""),
                        "预售许可证编号": i.get("presellname",""),
                        "发证日期": i.get("applydate",""),
                        "开盘日期": '',
                        "预售证准许销售面积":'',
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
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(listfile, sep='\t' ,mode='a', encoding = "utf-8", index=False, header=None)
                    
                except Exception as e:
                    print(traceback.format_exc())
                    print("异常")
                    continue
        #2多线程爬项目
        if os.path.exists(outfile):
            with open(outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip().split('\t')
                    okurl.add(a[0])
           
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.strip().split('\t')
                if len(a) ==7 and a[0] not in okurl:
                    dict2 = {
                        "URL":a[0],
                        "城市":a[1],
                        "项目名称":a[2],
                        "坐落位置":a[3],
                        "开发企业": a[4],
                        "预售许可证编号":a[5],
                        "发证日期": a[6],
                        "开盘日期": '',
                        "预售证准许销售面积":'',
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
                    urls.put(dict2)
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
            try:
                dict2=urls.get()
                projecetid =dict2['URL'].split('=')[1]
                ua = UserAgent()
                headers = {
                    'User-Agent': ua.random,
                    'Connection': 'close',
                }
                self.pool.headers.update(headers)
                url2='http://220.191.220.105/web/WebFdcsctjxx_selecYszxxByProjecetid.jspx?projecetid='+str(projecetid)
                print('pro url='+url2)
                r2 = self.pool.post(url2, timeout=60, headers=headers).json().get("list")
                # time.sleep(0.1)
                totalconstructionarea=''
                presellid=''
                totalsuite=''
                #3楼栋
                for m in r2:
                    totalconstructionarea = m.get("totalconstructionarea")
                    presellid = m.get("presellid")
                    totalsuite = m.get("totalsuite")
                    print(presellid)
                    dict2['预售证准许销售面积']=totalconstructionarea
                    dict2['套数'] = totalsuite
                    #4户室翻页
                    url3 = 'http://220.191.220.105/web/WebFdcsctjxx_selecWebFyxx.jspx?presellid=' +str(presellid)
                    r3 = self.pool.post(url3, timeout=60, headers=headers).json()
                    # time.sleep(0.1)
                    r4 = r3.get("list")
                    if len(r4)==0:
                        df = DataFrame(dict2, index=[0])
                        df.to_csv(outfile, sep='\t' ,mode='a', encoding = "utf-8",index=False, header=None)
                        break
                    for j in r4:
                        dict4 = {
                            "销售楼号": j.get("ld",""),
                            "房号": j.get("fh",""),
                            "房屋建筑面积": j.get("jzmj",""),
                            "拟售价格":j.get("nsqk",""),
                            "房屋销售状态":j.get("baqkname","")
                        }
                        dict5 = dict2.copy()
                        dict5.update(dict4)
                        df = DataFrame(dict5, index=[0])
                        df.to_csv(outfile, sep='\t' ,mode='a', encoding = "utf-8",index=False, header=None)
                    headers = {
                        'User-Agent': ua.random,
                        'Connection': 'close',
                    }
                    self.pool.headers.update(headers)
                    # time.sleep(0.1)
                    page = r3.get("pageinfo")
                    pageAll = re.findall(r'onclick="doPage\(.*?\);', page, re.I)
                    print(r3)
                    print(page)
                    pagenum = int(pageAll[len(pageAll)-1].replace("(","").replace(")",""))
                    
                    for x in range(2,pagenum):
                        dict11 = {
                            'cohYszxx.presellid': presellid,
                            'ld':'',
                            'page': x
                        }
                        url6 = 'http://220.191.220.105/web/WebFdcsctjxx_selecWebFyxx.jspx?presellid=' + str(presellid)
                        r7 = self.pool.post(url6, dict11,timeout=60, headers=headers).json()
                        r4 = r7.get("list")
                        
                        for j in r4:
                            # print(j)
                            dict7 = {
                                "销售楼号": j.get("ld",""),
                                "房号": j.get("fh",""),
                                "房屋建筑面积": j.get("jzmj",""),
                                "拟售价格": j.get("nsqk",""),
                                "房屋销售状态":j.get("baqkname","")
                            }
                            # print(dict7)
                            dict8 = dict2.copy()
                            dict8.update(dict7)
                            df = DataFrame(dict8, index=[0])
                            df.to_csv(outfile, sep='\t', mode='a',encoding = "utf-8", index=False, header=None)
            except Exception as e:
                print(traceback.format_exc())
                print("异常")
                continue
    def main(self):
        self.parse_one()
    
run = jx_spider()
run.main()