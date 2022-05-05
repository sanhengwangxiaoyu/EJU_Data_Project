# -*- coding:utf-8 -*-
__author__ = 'zhangxinming'
__date__ = '2021/4/14 21:51'


import requests
import re
import pandas as pd
import urllib.parse as urlparse
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
import traceback,os

urls = Queue()
okurl = set()
listfile='list/nanjing_url.txt'
outfile='data/nanjing_new.txt'
okfile='data/nanjing.txt'

class nj_spider():
    # def __init__(self):
        # self.pool = requests.session()
        # self.pool.mount('https://', HTTPAdapter(pool_connections=100, pool_maxsize=200,
                                                # max_retries=Retry(total=10, backoff_factor=1,
                                                                  # method_whitelist=frozenset(['GET', 'POST']))))    

    def parse_one(self):
        # of = open(listfile,'a+', encoding='utf-8') #保存结果文件
        if os.path.exists(outfile):
            with open(outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip().split('\t')
                    okurl.add(a[0])
        if os.path.exists(okfile):
            with open(okfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip().split('\t')
                    okurl.add(a[0])
        iii=0
        # for iii in range(1, 1):#527
        end='no'
        while True:
            iii+=1
            try:
                print("已抓取"+str(iii)) 
                # headers = {
                    # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.02',
                    # 'Connection': 'keep-alive',
                # }
                # self.pool.headers.update(headers)
                lburl = 'https://www.njhouse.com.cn/spf/persalereg?use=0&dist=&saledate=0&per_name=&perno=&dev_comp=&page='+str(iii)
                # # r = self.pool.get(lburl,timeout=60, headers=headers)
                # time.sleep(0.1)r.content.decode('UTF-8')
                h1=self.getHtml(lburl)
                table1 = etree.HTML(h1).xpath('//div[@class="spl_table"]//table')
                if len(table1)==0:
                    break
                print(len(table1))
                for kk in table1:
                    kprq=''.join(kk.xpath('.//tr[4]//td[2]//text()'))
                    if kprq.find('2020')>0:
                        end='yes'
                        break
                    url1='https://www.njhouse.com.cn/'+''.join(kk.xpath('.//tr[3]//td[2]//a//@href')).replace("detail","sales")
                    dict2 = {
                        "url": url1,
                        "城市": '南京',
                        "项目名称": ''.join(kk.xpath('.//tr[3]//td[2]//text()')),
                        "坐落位置": ''.join(kk.xpath('.//tr[2]//td[4]//text()')),
                        "开发企业": ''.join(kk.xpath('.//tr[1]//td[4]//text()')),
                        "预售许可证编号": ''.join(kk.xpath('.//tr[1]//td[2]//text()')),
                        "发证日期": '',
                        "开盘日期": kprq,
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
                        "房屋销售状态": ''
                    }
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(listfile, sep='\t', mode='a', index=False, header=None)
                if end=='yes':
                    break
            except Exception as e:
                print(traceback.format_exc())
                continue
        #2多线程爬项目
        if os.path.exists(outfile):
            with open(outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip().split('\t')
                    okurl.add(a[0])
        repeatlist=[]
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                if a[0] not in repeatlist and a[0] not in okurl:
                    urls.put(a)
                    repeatlist.append(a[0])
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
        ths = []
        for i in range(5):
            t = Thread(target=self.run, args=())
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
            # time.sleep(1111)
    def run(self):
        while urls.qsize() != 0:
            dicts=urls.get()
            print("qsize less="+str(urls.qsize()))  
            url1=dicts[0]
            #https://www.njhouse.com.cn/spf/sales?prjid=109169
            print(url1)
            ua = UserAgent()
            headers = {
                'User-Agent': ua.random,
                'Connection': 'keep-alive',
            }
            rslist=[]
            try: 
                dict2 = {
                    "url": url1,
                    "城市": '南京',
                    "项目名称": dicts[2],
                    "坐落位置": dicts[3],
                    "开发企业": dicts[4],
                    "预售许可证编号": dicts[5],
                    "发证日期": '',
                    "开盘日期":dicts[7],
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
                    "房屋销售状态": ''
                }
                print(dict2)
                
                # r1 = self.pool.get(url1, timeout=60, headers=headers) r1.content.decode('UTF-8')
                h1=self.getHtml(url1)
                table3 = etree.HTML(h1)
                table4 = table3.xpath('//li[@class="clearfix"]')
                # table4 = table3.xpath('//table[@class="fdxs_right"]')
                print(len(table4))
                if len(table4)==0:
                    rslist.append(dict2)
                for mm in table4:
                    try:
                        ld=''.join(mm.xpath('.//div[@class="fdxs_left"]//a//text()'))
                        # print(ld)
                        kprq1 = ''.join(mm.xpath('.//table[@class="fdxs_right"]//tbody//tr[2]//td[1]//text()'))
                        # print(kprq1)
                        headers = {
                            'User-Agent': ua.random,
                            'Connection': 'keep-alive',
                        }
                        if kprq1 == dicts[7]:
                            dict3=dict2.copy()
                            dict3["套数"]=''.join(mm.xpath('.//tbody//tr[2]//td[2]//text()'))
                            dict3["销售楼号"] = ld
                            url2='https://www.njhouse.com.cn/'+''.join(mm.xpath('.//div[@class="fdxs_left"]//a//@href'))
                            print(url2)
                            # r2 = self.pool.get(url2, timeout=60, headers=headers)
                            h1=self.getHtml(url2)
                            time.sleep(0.5)
                            table5 = etree.HTML(h1)
                            table6 = table5.xpath('//table[@class="ck_table"]//tr')
                            print(len(table6))
                            rooms=table5.xpath('//table[@class="ck_table"]//tr/td')
                            if len(rooms)==0:
                                rslist.append(dict3)
                                continue
                            #####
                            for room in rooms:
                                state=''.join(room.xpath('./@class'))
                                #层数过滤
                                if state=='td_h':
                                    continue
                                #
                                dict4=dict3.copy()
                                dict4["房号"]=''.join(room.xpath('.//a[1]//text()')).strip()
                                dict4["房屋销售状态"] = self.getstate(state)
                                #
                                td=room.xpath('string()')
                                m=re.split('面积：|价格：',td)
                                
                                if len(m)==3:
                                    dict4["房屋建筑面积"]=m[1]
                                    dict4["拟售价格"] =m[2]
                                #   
                                # df = DataFrame(dict3, index=[0])
                                # df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                                rslist.append(dict4)
                            '''
                            for rr in table6:
                                table7=rr.xpath('.//td[position()>1]')
                                for ww in table7:
                                    dict2["房号"]=''.join(ww.xpath('.//a[1]//text()'))
                                    state=''.join(ww.xpath('.//@class'))
                                    if(len(ww.xpath('.//a[2]//text()'))>1):
                                        dict2["房屋建筑面积"]=''.join(ww.xpath('.//a[2]//text()')[0]).replace("面积：","")
                                        dict2["拟售价格"] = ''.join(ww.xpath('.//a[2]//text()')[1]).replace("价格：","")
                                        dict2["房屋销售状态"] = self.getstate(state)
                                        df = DataFrame(dict2, index=[0])
                                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                                        time.sleep(0.1)
                                        '''
                    except Exception as e1:
                        print(traceback.format_exc())
                        continue
                for c in rslist:
                    df = DataFrame(c, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
            except Exception as e:
                print(traceback.format_exc())
                continue
    def getstate(self, state):
        zt=state
        if (state=='ks'):
            zt = "未售"
        elif (state=='rg'):
            zt = "已认购"
        elif (state == 'qy'):
            zt = "已签约"
        elif (state=='ba'):
            zt = "已备案"
        elif (state=='az'):
            zt = "拆迁安置"
        return zt
    def getHtml(self,link):
        num=0
        html="<html></html>"
        # print(link)
        if link.find('code=X')>0:
            return html
        try: #使用try except方法进行各种异常处理
            header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
            res = requests.get(link,headers=header,timeout=120,verify=False) #读取网页源码
            #解码
            if res.encoding=='utf-8' or res.encoding=='UTF-8' or res.text.find('charset="utf-8"')>0:
                res.encoding='utf-8'
            else:
                res.encoding='GBK'
            html=res.text
        except Exception as e:
            print(traceback.format_exc())
        finally:
            return html
    def main(self):
        self.parse_one()
        time.sleep(10)
run = nj_spider()
run.main()