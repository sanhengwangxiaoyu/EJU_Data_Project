# -*- coding:utf-8 -*-  
__author__ = 'zhangxinming'
__date__ = '2021/4/10 9:43'

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

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait

from threading import Thread
from queue import Queue
import traceback,os

urls = Queue()
okurl = set()
listfile='list/ningbo_url.txt'
outfile='data/ningbo_new.txt'
okfile='data/ningbo.txt'

class nb_spider():
    def __init__(self):
        # self.pool = requests.session()
        # self.pool.mount('https://', HTTPAdapter(pool_connections=100, pool_maxsize=200,max_retries=Retry(total=10,backoff_factor=1,method_whitelist=frozenset(['GET', 'POST']))))
        self.driver = webdriver.PhantomJS(executable_path=r'D:\tools\phantomjs-2.1.1-windows\bin\phantomjs.exe')
        self.wait = WebDriverWait(self.driver, 10)
        self.driver.set_window_size(1400, 900)
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
                    okurl.add(line.strip().split('\t')[0])
        if os.path.exists(okfile):
            with open(okfile,'r',encoding='utf-8') as txtData: 
                for line in txtData.readlines():
                    okurl.add(line.strip().split('\t')[0])
        ################
        of = open(listfile,'a+', encoding='utf-8') #保存结果文件
        #翻页获取项目列表range(1,185):
        # for ii in range(1,1):
        page=0
        while True:
            break
            page+=1
            try:
                # ua = UserAgent()
                # headers ={
                    # 'User-Agent': ua.random,
                    # 'Connection': 'close',
                # }
                # self.pool.headers.update(headers)
                lburl = 'https://newhouse.cnnbfdc.com/project/page_'+str(page)
                print('page=',page)
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
                table1 = etree.HTML(h1)
                table2 = table1.xpath('//li[@class="listbody__main__row"]')
                print(lburl+'：'+str(len(table2)))
                if len(table2)==0:
                    break
                for x in table2:
                    url2 = 'https://newhouse.cnnbfdc.com/'+''.join(x.xpath('.//div[@class="group-left"]//a//@href'))
                    if url2 in okurl:
                        print(page,'pass',url2)
                    okurl.add(url2)
                    of.write(url2+'\n')
                    of.flush()
                
            except Exception as e:
                print(traceback.format_exc())
                continue
        #2多线程爬项目
       
        repeatlist=[]
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.strip()
                if a not in repeatlist and a not in okurl:
                    urls.put(a)
                    repeatlist.append(a)
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
        self.driver.close()
    def run(self):
        '''
            proxy_host = 'dyn.horocn.com'
            proxy_port = 50000
            proxy_username = 'Q43H1702141252837413' #'隧道代理订单号'
            proxy_pwd = 'LWjXGlfUWnATnENL' #"密码（用户中心-我的订单页面可查）"
            proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
                "host": proxy_host,
                "port": proxy_port,
                "user": proxy_username,
                "pass": proxy_pwd,
            }
            proxies = {
                'http': proxyMeta,
                'https': proxyMeta,
            }
        '''
        
        while urls.qsize() != 0:
            print("qsize-------------------------------------------------- less="+str(urls.qsize()))   
            url2=urls.get()
            print(url2)
            xsid=url2.split('/')[-1]
            try:
                ua = UserAgent()
                dict2 = {
                    "URL":url2,
                    "城市": '宁波',
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
                # headers = {
                    # 'User-Agent': ua.random,
                    # 'Connection': 'close',
                # }
                # self.pool.headers.update(headers)
                while True:
                    time.sleep(3)
                    self.driver.get(url2)#self.pool.get(url2, timeout=60, headers=headers)
                    h2 = self.driver.page_source
                    if h2.find("请休息一会儿再访问")>0:
                        print('url2 sleep 5')
                        time.sleep(2)
                        continue
                    else:
                        break
                # print(r2)
                # time.sleep(1111)
                table3 = etree.HTML(h2)#r2.content.decode('utf-8'))
                # text=r2#.content.decode('utf-8')
                table4 = table3.xpath('//div[@class="latest-news-list__item"]//div[@class="latest-news-list__item__right"]')
                # print(text)
                dict2['项目名称']=''.join(table3.xpath('//a[@name="brdadcrumbProjectName"]/text()')).strip()
                dict2['坐落位置']=''.join(re.findall("projectAddress = '(.*?)'",h2,re.S)).strip()
                dict2['开发企业']=''.join(re.findall('开&nbsp;&nbsp;发&nbsp;&nbsp;商.*?">(.*?)</a>',h2,re.S)).strip() 
                dict2['拟售价格']=''.join(table3.xpath('//div[@class="project-index__right__center"]/ul/li[1]/p/big/text()')).strip()+'元/㎡'
                print('url2='+url2+':许可证数量= '+str(len(table4)))
               
                if len(table4)==0:
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    continue
                # print(dict2)
                # time.sleep(1111)
                #1 根据预售证链接进入-楼栋-户室
                for nn in table4:
                    dict2["预售许可证编号"] = ''.join(nn.xpath('.//ul[1]//li[1]//text()')).replace("\r\n","").replace("许可证号：","").strip()
                    dict2["发证日期"] = ''.join(nn.xpath('.//ul[1]//li[2]//text()')).replace("许可日期：","")
                    # dict2["套数"] = ''.join(nn.xpath('.//ul[1]//li[3]//text()')) 图片 未抓取
                    # dict2["拟售价格"] = ''.join(nn.xpath('.//ul[1]//li[5]//text()'))
                    dict2["销售状态"] = ''.join(nn.xpath('.//ul[1]//li[6]//text()')).replace("销售状态：","")
                    dict2["面积"] = ''.join(nn.xpath('.//ul[2]//li[1]//text()')).replace("\xa0","").replace("建筑面积：","")
                    dict2["售楼电话"] = ''.join(nn.xpath('.//ul[2]//li[3]//text()'))
                    dict2["售楼地址"] = ''.join(nn.xpath('.//ul[2]//li[2]//text()'))
                    #具体许可证链接地址
                    url3='https://newhouse.cnnbfdc.com/'+''.join(nn.xpath('.//ul[1]//li[1]//a//@href'))
                    # headers = {
                        # 'User-Agent': ua.random,
                        # 'Connection': 'close',
                    # }
                    # self.pool.headers.update(headers)
                    #打开具体某个许可证链接
                    while True:
                        time.sleep(3)
                        self.driver.get(url3)
                        r3 =self.driver.page_source
                        if r3.find("请休息一会儿再访问")>0:
                            print('r3 sleep 4')
                            time.sleep(2)
                            continue
                        else:
                            break
                    print('url3='+url3)
                    time.sleep(0.1)
                    table5 = etree.HTML(r3)#.content.decode('utf-8'))
                    #2获取楼栋列表链接
                    table8 = table5.xpath('//div[@class="switch-building" and position()>1]//li/a')
                    ####多楼栋
                    #没有分页信息时，获取楼栋
                    if len(table8)==0:
                        df = DataFrame(dict2, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                        continue
                    ####列表模式要分页。使用视图模式不分页
                    print(dict2["预售许可证编号"] +': 楼栋数量='+str(len(table8)))
                    ld_num=0
                    for kk in table8:
                        louhao=self.trim(kk.xpath('./text()')[0])
                        print(louhao)
                        url5 = kk.xpath('./@href')
                        for pp in url5:
                            guid = pp[-36:]
                            '''
                            #列表模型获取分页
                            url14 = 'https://newhouse.cnnbfdc.com/project/buildtablelist/' + xsid + '?buildingId=' + guid
                            print('url14='+url14)
                            headers = {
                                'User-Agent': ua.random,
                                'Connection': 'close',
                            }
                            self.pool.headers.update(headers)
                            text5=''
                            while True:
                                time.sleep(3)
                                self.driver.get(url14)#
                                #self.pool.get(url14, timeout=60, headers=headers)
                                text5= self.driver.page_source
                                if text5.find("请休息一会儿再访问")>0:
                                    print('r5 sleep 4')
                                    time.sleep(5)
                                    continue
                                else:
                                    break
                            time.sleep(0.1)
                            page=re.findall('page=(\d+)',text5,re.S)
                            print(page)
                            pageAll ='0'
                            if len(page)==0:
                                df = DataFrame(dict2, index=[0])
                                df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                                continue
                            elif len(page)>1:
                                pageAll=page[-1]
                                if int(page[-1])<int(page[-2]):
                                    pageAll=page[-2]
                            else:
                                pageAll=page[0]
                            print('pageAll='+pageAll)
                            
                            buildings=[]
                            for ww in range(1,int(pageAll)+1):
                            '''
                            #示图模式
                            buildings=[]
                            url15='https://newhouse.cnnbfdc.com'+pp.replace('&amp;','&')
                            print('url15='+url15)
                            # headers = {
                                # 'User-Agent': ua.random,
                                # 'Connection': 'close',
                            # }
                            # self.pool.headers.update(headers)
                            while True:
                                if ld_num==0:
                                    text5=r3
                                    ld_num+=1
                                    break
                                time.sleep(3)
                                self.driver.get(url15)
                                text5= self.driver.page_source
                                if text5.find("请休息一会儿再访问")>0:
                                    time.sleep(5)
                                    print('r18 sleep 4')
                                    continue
                                else:
                                    break
                            table17 = etree.HTML(text5)
                            table10 = table17.xpath('//table[@class="buildingtable"]/tbody/tr[position()>1]')
                            print('ceng=',len(table10))
                            # time.sleep(11111)
                            for tr in table10:
                                tds=tr.xpath('./td[position()>1]')
                                if len(tds)==0:
                                    continue
                                for td in tds:
                                    dict_room={}
                                    dict_room["房号"] = self.trim(''.join(td.xpath('./a/p[3]/text()')))
                                    dict_room["房屋建筑面积"] = self.trim(''.join(td.xpath('./a/p[2]/text()')))
                                    #room_url='https://newhouse.cnnbfdc.com/unit/details/'+td.xpath('./a/@data-guid')
                                    #dict_room["拟售价格"] = self.trim(''.join(td.xpath('.//td[7]//text()')))
                                    
                                    sale=self.trim(''.join(td.xpath('./@class')))
                                    if 'selfroom' in sale:
                                        sale='企业自持房'
                                    elif 'modelroom' in sale:
                                        sale='样板房'
                                    elif 'unsale' in sale:
                                        sale='未网签'
                                    elif 'book' in sale:
                                        sale='已预定'
                                    elif 'sold' in sale:
                                        sale='已网签'
                                    elif 'limited' in sale:
                                        sale='已限制'
                                    dict_room["房屋销售状态"] = sale
                                    buildings.append(dict_room)
                                    # print(dict_room)
                                    # time.sleep(11111)
                            '''
                                #table模式
                                url15 = 'https://newhouse.cnnbfdc.com/project/buildtablelist/' + xsid + '?buildingId=' + guid+"&page="+str(ww)
                                print('url15='+url15)
                                headers = {
                                    'User-Agent': ua.random,
                                    'Connection': 'close',
                                }
                                self.pool.headers.update(headers)
                                while True:
                                    time.sleep(3)
                                    self.driver.get(url15)#
                                    text5= self.driver.page_source
                                    #r18 = self.pool.get(url15, timeout=60, headers=headers)
                                    if text5.find("请休息一会儿再访问")>0:
                                        time.sleep(5)
                                        print('r18 sleep 4')
                                        continue
                                    else:
                                        break
                                
                                table17 = etree.HTML(text5)
                                table10 = table17.xpath('//table//tbody//tr')
                                for hh in table10:
                                    dict_room={}
                                    dict_room["房号"] = self.trim(''.join(hh.xpath('.//td[2]//text()')))
                                    dict_room["房屋建筑面积"] = self.trim(''.join(hh.xpath('.//td[4]//text()')))
                                    dict_room["拟售价格"] = self.trim(''.join(hh.xpath('.//td[7]//text()')))
                                    dict_room["房屋销售状态"] = self.trim(''.join(hh.xpath('.//td[9]//text()')))
                                    buildings.append(dict_room)
                            '''
                            #write
                            dict4=dict2.copy()
                            dict4['销售楼号']=louhao
                            if len(buildings)==0:
                                df = DataFrame(dict4, index=[0])
                                df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                            else:
                                dict4['套数']=str(len(buildings))
                                for dictroom in buildings:
                                    dict4.update(dictroom)
                                    df = DataFrame(dict4, index=[0])
                                    df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                                buildings=[]
                # time.sleep(12211)    
            except Exception as e:
                print(traceback.format_exc())
                continue
        self.driver.close()

    def main(self):
        self.parse_one()
run = nb_spider()
run.main()