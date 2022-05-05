# -*- coding:utf-8 -*-  

import requests
import re
import pandas as pd
from lxml import etree
import time,os
import json
import random
from pandas import Series,DataFrame
import html
from threading import Thread
from queue import Queue
import traceback

urls = Queue()
okurl = set()
ysz_oklist=set()
listfile='list/xiamen_url.txt'
outfile='data/xiamen_new.txt'
okfile='data/xiamen.txt'

class jn_spider():
    def postHtml(self,url,data):
        header = {
                'User-Agent':'%E6%88%BF%E7%AE%A1%E5%B1%80/1 CFNetwork/978.0.7 Darwin/18.7.0',
                'Content-Type':'application/x-www-form-urlencoded',
                'Accept': '*/*'
                }
        try:
            res=requests.post(url=url,data=data,headers=header,verify=False)
            res.encoding='UTF-8'
            return res.text
        except Exception as e:
            print(traceback.format_exc())
        return ''
    def getHtml(self,link):
        html=""
        try: #使用try except方法进行各种异常处理
            header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.02' }
            res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
            #解码
            res.encoding='UTF-8'
            html=res.text
        except Exception as e:
            print(e)
        finally:
            return html                                                              # method_whitelist=frozenset(['GET', 'POST']))))
    def trim(self,word):
        return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')   
        
    def parse_one(self):
        if os.path.exists(outfile):
            with open(outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip().split('\t')
                    okurl.add(a[0])
                    ysz_oklist.add(a[5])
        if os.path.exists(okfile):
            with open(okfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip().split('\t')
                    okurl.add(a[0])
                    ysz_oklist.add(a[5])
        listlist=[]
        if os.path.exists(listfile):
            with open(listfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip().split('\t')
                    listlist.append(a[0])
        page=0
        while True:
            page+=1
            try:
                data11={
                    "currentpage":str(page),
                    "pagesize":"20",
                }
                lburl = 'http://fdc.zfj.xm.gov.cn/home/Getzslp'
                h1=self.postHtml(lburl,data11)
                js=json.loads(h1)["Body"]
                time.sleep(0.1)
                rjson = json.loads(js).get("bodylist")
                print('page=',page,len(rjson))
                if len(rjson)==0:
                    break
                for mm in rjson:
                    xmmc=mm.get('XMMC')
                    tid=mm.get('TRANSACTION_ID')
                    url2='http://fdc.zfj.xm.gov.cn/LP/Index?transactionid='+tid+'&projectName='+xmmc
                    dict2 = {
                        "URL":url2,
                        "城市": '厦门',
                        "项目名称": xmmc,
                        "坐落位置": mm.get('XMDZ'),
                        "开发企业": '',
                        "预售许可证编号":mm.get('YSXKZH'),
                        "发证日期":mm.get('APPROVALDATE'),
                        "开盘日期": '',
                        "预售证准许销售面积":mm.get('PZMJ'),
                        "销售状态": '',
                        "销售楼号": '',
                        "套数": mm.get('KSTS'),
                        "面积": mm.get('KSMJ'),
                        "拟售价格": '',
                        "售楼电话": '',
                        "售楼地址": '',
                        "房号": '',
                        "房屋建筑面积": '',
                        "房屋销售状态": '',
                    }
                    if url2 in okurl or mm.get('YSXKZH') in ysz_oklist or url2 in listlist:
                        continue
                    else: 
                        listlist.append(url2)
                        df = DataFrame(dict2, index=[0])
                        df.to_csv(listfile, sep='\t', mode='a', index=False, header=None)
            except Exception as e:
                print(traceback.format_exc())
                continue
        #多线程爬项目
        #按URl过滤
        repeatlist=[]
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                if a[0] in okurl or a[5] in ysz_oklist:
                    continue
                else:
                    okurl.add(a[0])
                    ysz_oklist.add(a[5])
                    urls.put(a)
        print('qsize total=',urls.qsize())           
        time.sleep(3)
        ths = []
        for i in range(1):
            t = Thread(target=self.run, args=())
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
    def run(self):
        while urls.qsize() != 0:
            alist=urls.get()
            print('qsize==== : ',urls.qsize())
            print(alist)
            url2=alist[0]
            try:
                print('url2='+url2)
                dict2 = {
                    "URL": url2,
                    "城市": '厦门',
                    "项目名称": alist[2],
                    "坐落位置": alist[3],
                    "开发企业": '',
                    "预售许可证编号": alist[5],
                    "发证日期": alist[6],
                    "开盘日期": '',
                    "预售证准许销售面积": alist[8],
                    "销售状态": '',
                    "销售楼号": '',
                    "套数": alist[11],
                    "面积": alist[12],
                    "拟售价格": '',
                    "售楼电话": '',
                    "售楼地址": '',
                    "房号": '',
                    "房屋建筑面积": '',
                    "房屋销售状态": '',
                }
                time.sleep(0.1)
                h2=self.getHtml(url2)
                table2 = etree.HTML(h2).xpath('//ul[@class="filetree"]/li/ul/li')
                print('table2=',len(table2))
                if len(table2)==0:
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    continue
                for ff in table2:
                    dict3=dict2.copy()
                    dict3["销售楼号"]=''.join(ff.xpath('./span/text()'))
                    urllist = ff.xpath('.//li')
                    if len(urllist)==0:
                        df = DataFrame(dict3, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                        continue
                    rslist=[]
                    print('urllist=',dict3["销售楼号"],len(urllist))
                    for ss in urllist:
                        params=re.sub('[\'\"\(\)]','',''.join(ss.xpath('.//a//@href')).replace('javascript:DispLp','')).split(",")
                        dy = ''.join(ss.xpath('.//span/text()'))
                        data22={
                            "BuildID": params[0],
                            "NAID": params[1],
                            "lotid": params[2]
                        }
                        url3 = 'http://fdc.zfj.xm.gov.cn/Lp/LPPartial'
                        h3=self.postHtml(url3,data22)
                        time.sleep(0.1)
                        table4 =  etree.HTML(h3).xpath('//td')
                        print('tab4=',len(table4))
                        for pp in table4:
                            HouseId = ''.join(pp.xpath('.//@id'))
                            if(HouseId!=''):
                                dict4=dict3.copy()
                                dict4["房号"] =dy+'-'+''.join(pp.xpath('.//text()')).replace("\r\n","").strip()
                                url4='http://fdc.zfj.xm.gov.cn/LP/Fwztxx?HouseId='+HouseId+'&yyxx=undefined&zs=undefined&zt=1'
                                h4=self.getHtml(url4)
                                list7= re.findall(r'<tr><td>面积</td><td>(.*?)</td>', h4, re.I)
                                list8= re.findall(r'<tr><td>拟售价格</td><td>(.*?)</td>', h4, re.I)
                                list9 = re.findall(r'<tr><td>销售状态</td><td>(.*?)</td>', h4, re.I)
                                if len(list7):
                                    dict4["房屋建筑面积"]=list7[0]
                                if len(list8):
                                    dict4["拟售价格"]=list8[0].strip()
                                if len(list9):
                                    dict4["房屋销售状态"] = list9[0]
                                else:
                                    dict4["房屋销售状态"] = '可售'
                                rslist.append(dict4)
                    if len(rslist)==0:
                        df = DataFrame(dict3, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    else:
                        for c in rslist:
                            df = DataFrame(c, index=[0])
                            df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                   
            except Exception as e:
                print(traceback.format_exc())
    def main(self):
        self.parse_one()
run = jn_spider()
run.main()