# -*- coding:utf-8 -*-  
__author__ = 'zhangxinming'
__date__ = '2021/4/10 23:24'

import requests
import re
import pandas as pd
import urllib.parse as urlparse
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
listfile='list/nantong_url.txt'
outfile='data/nantong_new.txt'
okfile='data/nantong.txt'

class wx_spider():
    # def __init__(self):
        # self.pool = requests.session()
        # self.pool.mount('http://', HTTPAdapter(pool_connections=100, pool_maxsize=200,
                                                # max_retries=Retry(total=10, backoff_factor=1,
                                                                  # method_whitelist=frozenset(['GET', 'POST']))))
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
    def parse_one(self):
        ####历史已爬虫
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
        #####
        #翻页获取项目列表range(1,13):
        of = open(listfile,'a+', encoding='utf-8') #保存结果文件
        page=0
        while True:
        # for iii in range(1, 1):
            page+=1
            try:
                # ua = UserAgent()
                # headers = {
                    # 'User-Agent': ua.random,
                    # 'Connection': 'close',
                # }
                # self.pool.headers.update(headers)
                lburl = 'http://newhouse.ntfdc.net/house_open.aspx?p='+str(page)
                # r = self.pool.get(lburl,timeout=60, headers=headers)
                # table1 = etree.HTML(r.content.decode('utf-8'))
                time.sleep(10)
                table1 =etree.HTML(self.getHtml(lburl))
                table2 = table1.xpath('//div[@class="layer-bd tb-style1"]//tr[position()>1]')
                print(page,len(table2))
                print(table2)
                urllist=[]
                if len(table2)==0:
                    break
                for n in table2:
                    url1=''.join(n.xpath('.//td//@href'))
                    if url1 in okfile:
                        print('pass')
                        continue
                    urllist.append(url1)
                of.write('\n'.join(urllist)+'\n')
                of.flush()
            except Exception as e:
                print(e)
                print("异常1")
                continue
        #2多线程爬项目
        repeatlist=[]
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.strip()
                if a not in repeatlist and a not in okurl:
                    urls.put(a)
                    repeatlist.append(a)
        print(urls.qsize())           
        
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
            print(urls.qsize())
            url1=urls.get()
            print(url1)
            try:
                # ua = UserAgent()
                # headers = {
                    # 'User-Agent': ua.random,
                    # 'Connection': 'close',
                # }
                # self.pool.headers.update(headers)
                # r2 = self.pool.get(url1, timeout=60, headers=headers)
                # table3 = etree.HTML(r2.content.decode('utf-8'))
                table3 = etree.HTML(self.getHtml(url1))
                table4 = table3.xpath('//div[@class="box1 mb10" and position()<2]')[0]
                table5 = table3.xpath('//div[@class="box1 mb10" and position()>1]')[0]
                table7 = table3.xpath('//div[@class="house_info2"]//div[4]')[0]
                print(len(table7))
                dict2 = {
                    "URL": url1,
                    "城市": '南通',
                    "项目名称": ''.join(table4.xpath('.//li[1]//text()')).replace('备 案 名：',''),
                    "坐落位置": ''.join(table4.xpath('.//li[7]//text()')).replace('项目地址：',''),
                    "开发企业": ''.join(table4.xpath('.//li[5]//text()')).replace('开 发 商：',''),
                    "预售许可证编号": '',
                    "发证日期": '',
                    "开盘日期": ''.join(table7.xpath('.//li[1]//text()')).replace('开盘时间：',''),
                    "预售证准许销售面积": '',
                    "销售状态": '',
                    "销售楼号": '',
                    "套数": ''.join(table4.xpath('.//li[3]//text()')).replace('规划户数：',''),
                    "面积": ''.join(table7.xpath('.//li[4]//text()')).replace('建筑面积：',''),
                    "拟售价格": ''.join(table5.xpath('//span[@class="jjdd"]//text()')),
                    "售楼电话": ''.join(table3.xpath('//div[@class="number_t"]//text()')),
                    "售楼地址": ''.join(table7.xpath('.//div[@class="w w4"]//text()')),
                    "房号": '',
                    "房屋建筑面积": '',
                    "房屋销售状态": '',
                }

                url2 = url1+"/gongshi/"
                print(url2)
                time.sleep(0.1)
                # headers = {
                    # 'User-Agent': ua.random,
                    # 'Connection': 'close',
                # }
                # self.pool.headers.update(headers)
                # r3 = self.pool.get(url2, timeout=60, headers=headers
                # table11 = etree.HTML(r3.content.decode('utf-8'))
                table11 = etree.HTML(self.getHtml(url2))
                table22 = table11.xpath('//div[@class="left1"]//*')
                dict = {}
                list1 = []
                ysz = ''
                print(len(table22))
                for kk in table22:
                    if (kk.tag=='dt'):
                        print("111111111")
                        list1 = []
                        ysz = ''.join(kk.xpath('.//text()'))
                        print(ysz)
                    if (kk.tag=='dd'):
                        dict[ysz] = list1
                        print("22222222")
                        list1.append((''.join(kk.xpath('.//text()')),''.join(kk.xpath('.//@href'))))
                if len(dict)==0:
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    continue
                rslist=[]
                for key in dict:
                    try:
                        dict2["预售许可证编号"]=key
                        for jj in dict[key]:
                            dict2["销售楼号"] = jj[0]
                            # headers = {
                                # 'User-Agent': ua.random,
                                # 'Connection': 'close',
                            # }
                            # self.pool.headers.update(headers)
                            # r4 = self.pool.get('http://newhouse.ntfdc.net/'+jj[1], timeout=60, headers=headers)
                            url4='http://newhouse.ntfdc.net/'+jj[1]
                            table33 = etree.HTML(self.getHtml(url4))
                            table44 = table33.xpath('//div[@class="louh"]//div')
                            for ff in table44:
                                dict4=dict2.copy()
                                dict4["房号"] = ''.join(ff.xpath('.//a//text()'))
                                dict4["房屋销售状态"] = self.getstate(''.join(ff.xpath('.//@class')))
                                rslist.append(dict4)
                                # df = DataFrame(dict2, index=[0])
                                # df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    except Exception as e:
                        print(e)
                        print("异常1")
                        time.sleep(5)
                        continue
                ###save
                for c in rslist:
                    df = DataFrame(c, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    # time.sleep(11110)
            except Exception as e:
                print(e)
                print("异常3")
                continue
    def getstate(self,color):
        state = color
        if(color=='fh1'):
            state='可售'
        elif(color=='fh3'):
            state = '已售已备案'
        return state
    def main(self):
        self.parse_one()
run = wx_spider()
run.main()