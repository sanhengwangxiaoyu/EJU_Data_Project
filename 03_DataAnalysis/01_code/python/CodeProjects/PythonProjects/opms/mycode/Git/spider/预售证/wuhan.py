# -*- coding:utf-8 -*-  
#按URL过滤

import requests
import re
import pandas as pd
import urllib.parse as urlparse
from lxml import etree
import time
import json
import urllib.parse
from pandas import Series,DataFrame
import html

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait

from threading import Thread
from queue import Queue
import traceback,os

urls = Queue()
okurl = set()
listfile='list/wuhan_url.txt'
outfile='data/wuhan_new.txt'
okfile='data/wuhan.txt'

class wh_spider():
    def getHtml(self,link):
        html=""
        try: #使用try except方法进行各种异常处理
            header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.02' }
            res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
            #解码
            res.encoding='UTF-8'
            html=res.text
            if res.text.find('因为发生内部服务器错误')==-1:
                res.encoding='GBK'
                html=res.text
            
        except Exception as e:
            print(e)
        finally:
            return html
    def postHtml(self,url,data):
        #使用try except方法进行各种异常处理
        header = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.02',
                'Content-Type':'application/x-www-form-urlencoded',
                'Accept': '*/*'
                }
        #data=json.loads(js)
        try:
            res=requests.post(url=url,data=data,headers=header,verify=False)
            res.encoding='GBK'
            return res.text
        except Exception as e:
            print(traceback.format_exc())
        return ''
    def parse_one(self):
         ##已爬URL列表
        if os.path.exists(outfile):
            with open(outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    okurl.add(a[0])
        if os.path.exists(okfile):
            with open(okfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    okurl.add(a[0])
        #已有URL
        repeatlist=[]
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                repeatlist.append(a[0])
        ######################
        dict = {
            "__VIEWSTATE": "",
            "__VIEWSTATEGENERATOR": "",
            "__EVENTTARGET": "AspNetPager1",
            "__EVENTARGUMENT": "",
            "__EVENTVALIDATION": "",
            "tbtxt": "",
            "xmmc: ": "",
            "xmdz": "",
            "kfs": "",
            "AspNetPager1_input": "",
        }
        of = open(listfile,'a+', encoding='utf-8') #保存结果文件
        #翻页获取项目列表
        page=0
        total=0
        while True:
            break
            page+=1
            try:
                dict["__EVENTARGUMENT"]=str(page)
                dict["AspNetPager1_input"] = str(page-1)
                lburl = 'http://119.97.201.22:8083/search/spfxmcx/spfcx_index.aspx'
                time.sleep(4)
                if(page==1):
                    h1 = self.getHtml(lburl)
                else:
                    h1  = self.postHtml(lburl,dict)#self.pool.post(lburl,dict,timeout=60, headers=headers)
                if total==0:
                    total=int(re.findall('共(\d+)条记录',h1,re.S)[0])
                table1 = etree.HTML(h1)
                table2 = table1.xpath('//table[@id="tables"]//tr[position()>1]')
                dict["__VIEWSTATEGENERATOR"]=''.join(table1.xpath('//input[@name="__VIEWSTATEGENERATOR"]//@value'))
                dict["__EVENTVALIDATION"] = ''.join(table1.xpath('//input[@name="__EVENTVALIDATION"]//@value'))
                dict["__VIEWSTATE"] = ''.join(table1.xpath('//input[@id="__VIEWSTATE"]//@value'))
                # print(dict)
                print('page=',page,len(table2))
                if len(table2)==0:
                    break
                num=0
                for kk in table2:
                    url1 = ''.join(kk.xpath(".//td[1]//@href"))
                    url11='http://119.97.201.22:8083/search/spfxmcx/spfcx_mx.aspx?DengJH='+urllib.parse.quote(url1.split('?')[1].split("=")[1].encode('gbk'))
                    if url11 in repeatlist or url11 in okurl:
                        continue
                    num+=1
                    print(num)
                    repeatlist.append(url11)
                    pro=''.join(kk.xpath(".//td[1]")[0].xpath('string(.)'))
                    ts=''.join(kk.xpath(".//td[2]")[0].xpath('string(.)'))
                    mj=''.join(kk.xpath(".//td[4]//text()"))
                    of.write(url11+'\t'+pro+'\t'+ts+'\t'+mj+'\n')
                    of.flush()
            except Exception as e:
                print(traceback.format_exc())
            if total<=page*20:
                break
        #2多线程爬项目
        repeatlist=[]
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                if a[0].strip() not in repeatlist and a[0].strip() not in okurl:
                    urls.put(a)
                    okurl.add(a[0])
                    # self.run()
                    # time.sleep(11111)
        print("qsize="+str(urls.qsize()))           
        time.sleep(3)
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
                dicts=urls.get()
                url11=dicts[0].strip()
                print("qsize less="+str(urls.qsize()))
                dict2 = {
                        "URL":url11,
                        "城市": '武汉',
                        "项目名称": dicts[1],
                        "坐落位置":'',
                        "开发企业": '',
                        "预售许可证编号":'',
                        "发证日期":'',
                        "开盘日期":'',
                        "预售证准许销售面积":'',
                        "销售状态": '',
                        "销售楼号": '',
                        "套数": dicts[2],
                        "面积": '',
                        "拟售价格": '',
                        "售楼电话": '',
                        "售楼地址": '',
                        "房号": '',
                        "房屋建筑面积": '',
                        "房屋销售状态": '',
                }
                while True:
                    h1=self.getHtml(url11)
                    if h1.find('403 Forbidden')>0:
                        time.sleep(60)
                    else:
                        break
                dict2['坐落位置']=''.join(re.findall('txt_xmzl">(.*?)</span>',h1,re.S)).strip()
                dict2['开发企业']=''.join(re.findall('txt_kfqy">(.*?)</span>',h1,re.S)).strip()
                dict2['售楼电话']=''.join(re.findall('txt_lxdh">(.*?)</span>',h1,re.S)).strip()
                ##
                print(url11)
                id=url11.split("=")[1]
                url2= url11.replace('spfcx_mx.aspx','spfcx_lpb.aspx')#'http://119.97.201.22:8083/search/spfxmcx/spfcx_lpb.aspx?DengJh='+id
                while True:
                    h2=self.getHtml(url2)
                    if h2.find('403 Forbidden')>0:
                        print('wait  url2')
                        time.sleep(60)
                    else:
                        break
                time.sleep(3)
                table4 = etree.HTML(h2).xpath('//table[@class="table_lp"]//tbody//tr')
                if h2.find('因为发生内部服务器错误')>0:
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a',encoding = "utf-8", index=False, header=None)
                    print('因为发生内部服务器错误,save')
                    continue
                if len(table4)==0:
                    print('table4=0,back')
                    continue
                print('table4',len(table4))
                ##楼栋列表
                rslist=[]
                for jj in table4:
                    ld=''.join(jj.xpath('.//td[1]/text()')).strip()
                    ts=''.join(jj.xpath('.//td[3]/text()')).strip()
                    mj=''.join(jj.xpath('.//td[4]/text()')).strip()
                    
                    dict3=dict2.copy()
                    dict3['销售楼号']=ld
                    dict3['套数']=ts
                    dict3['面积']=mj
                    param = ''.join(jj.xpath(".//td[1]//@href")).split("&")
                    #没有下级链接时，保存此楼基本信息
                    if str(param).find('houseDengJh')<1:
                        rslist.append(dict3)
                        continue
                    param1 = param[0].split("=")[1]
                    param2 = param[1].split("=")[1]
                    param3 =urllib.parse.quote(param1.encode('gbk'))
                    param4 =urllib.parse.quote(param2.encode('gbk'))
                    url5 = 'http://119.97.201.22:8083/search/spfxmcx/spfcx_fang.aspx?'+'dengJH='+param3+'&houseDengJh='+param4
                    print('url5',url5)
                    while True:
                        h5=self.getHtml(url5)
                        if h5.find('403 Forbidden')>0:
                            print('wait  url5')
                            time.sleep(60)
                        else:
                            break
                    time.sleep(2)
                    table6 = etree.HTML(h5).xpath('//table[@class="tab_style"]//tr')
                    #下级为空时，保存此楼基本信息
                    if(len(table6)<1):
                        rslist.append(dict3)
                        continue
                    ##户室行列表room list
                    for pp in table6:
                        lhs = pp.xpath('.//td') #室号列表
                        if len(lhs)<4:
                            continue
                        danyuan=lhs[1].xpath("string(.)")
                        louceng=lhs[2].xpath("string(.)")
                        #每行的户室列表
                        for num in range(3,len(lhs)):
                            dict4=dict3.copy()
                            huhao=''.join(lhs[num].xpath("string(.)")).strip()
                            dict4['房号']=danyuan+'(单元)/'+louceng+'(层)/'+huhao
                            style=''.join(lhs[num].xpath("./@style"))
                            dict4['房屋销售状态']=self.getstate(style)
                            try:
                                urlxq = ''.join(lhs[num].xpath(".//@href"))
                                if urlxq!='http://119.97.201.22:8080/TimeFL.aspx?gid=00000000-0000-0000-0000-000000000000' and huhao.find('车')==-1:
                                    continue
                                    time.sleep(1)
                                    while True:
                                        hxq=self.getHtml(urlxq)
                                        if hxq.find('403 Forbidden')>0:
                                            print('wait  urlxq')
                                            time.sleep(60)
                                        else:
                                            break
                                    table7 = etree.HTML(hxq)
                                    xkz = ''.join(table7.xpath('//table[@class="table table-condensed table-hover"]//tbody//tr[2]//td[3]//text()'))
                                    jzmj = ''.join(table7.xpath('//table[@class="table table-condensed table-hover"]//tbody//tr[3]//td[3]//text()'))
                                    jg = ''.join(table7.xpath('//table[@class="table table-condensed table-hover"]//tbody//tr[4]//td[3]//text()'))
                                    dict4["预售许可证编号"] = xkz
                                    dict4["拟售价格"] = jg
                                rslist.append(dict4)
                                # print(dict4)
                                # time.sleep(9999)
                            except Exception as e1:
                                print(traceback.format_exc())
                                continue
                if len(rslist)==0:
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a',encoding = "utf-8", index=False, header=None)
                else:
                    for c in rslist:
                        df = DataFrame(c, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a',encoding = "utf-8", index=False, header=None)
            except Exception as e:
                print(traceback.format_exc())
                # time.sleep(9999)
                continue
    def getstate(self, color):
        state = color
        if color.find('#FF0000')>0:
            state = '已网上销售'
        elif color.find('#CCFFFF')>0:
            state = '未销（预）售'
        elif color.find('#000000')>0:
            state = '限制出售'
        elif color.find('#FFFF00')>0:
            state = '已在建工程抵押'
        elif color.find('#CC0099')>0:
            state = '已查封'
        elif color.find('#FFFF00')>0:
            state = '已在建工程抵押已查封'
        return state
   
    def main(self):
        self.parse_one()
        time.sleep(10)

run = wh_spider()
run.main()
