# -*- coding:utf-8 -*-
import requests
import re
import pandas as pd
from lxml import etree
import time
import json
from pandas import Series,DataFrame
from selenium import webdriver

from threading import Thread
from queue import Queue
import traceback,os

urls = Queue()
okurl = set()
listfile='list/shenzhen_url.txt'
outfile='data/shenzhen_new.txt'
okfile='data/shenzhen.txt'

class sz_spider():
    def getHtml(self,link):
        html=""
        try: #使用try except方法进行各种异常处理
            header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.02' }
            res = requests.get(link,headers=header,timeout=30,verify=False) #读取网页源码
            #解码
            res.encoding='UTF-8'
            html=res.text
        except Exception as e:
            print('err get')
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
            res.encoding='UTF-8'
            return res.text
        except Exception as e:
            print('err post')
        return ''                                                            
    def trim(self,word):
        return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','') 
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
        repeatlist=[]
        if os.path.exists(listfile):
            with open(listfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip()
                    repeatlist.append(a)
        #####
        params = {}
        of = open(listfile,'a+', encoding='utf-8') #保存结果文件
        lburl = 'http://zjj.sz.gov.cn/ris/bol/szfdc/index.aspx'
        pn=1
        end='no'
        driver=webdriver.Firefox()
        # driver.get(lburl)
        while True :
            #翻页中断
            if end=='yes':
                break
            pn+=1
            try:
                time.sleep(10)
                h1=driver.page_source
                # if pn==1:
                    # # r = self.pool.get(lburl,timeout=60, headers=headers)
                    # h1=self.getHtml(lburl)
                # else:
                    # h1=self.postHtml(lburl,params)
                    # # r = self.pool.post(lburl, params, timeout=60, headers=headers)
                
                table1 = etree.HTML(h1)
                # params={
                    # "scriptManager2": "updatepanel2|AspNetPager1",
                    # "__EVENTTARGET": "AspNetPager1",
                    # "__EVENTARGUMENT": str(pn),#str(pn),
                    # "__LASTFOCUS":"",
                    # "__VIEWSTATE":''.join(table1.xpath('//input[@name="__VIEWSTATE"]//@value')),
                    # "__VIEWSTATEGENERATOR": ''.join(table1.xpath('//input[@name="__VIEWSTATEGENERATOR"]//@value')),
                    # "__VIEWSTATEENCRYPTED":"",
                    # "__EVENTVALIDATION":''.join(table1.xpath('//input[@name="__EVENTVALIDATION"]//@value')),
                    # "tep_name":"",
                    # "organ_name":"",
                    # "site_address":"",
                    # "ddlPageCount": "20",
                # }
                table2 = table1.xpath('//table[@class="table ta-c bor-b-1 table-white"]//tr[position()>1]')
                print("page=",pn-1,len(table2))
                if len(table2)==0:
                    print('fanye table2=0')
                    break
                for kk in table2:
                    urlys=''.join(kk.xpath(".//td[2]//@href")).replace("./","")
                    #设置翻页中断
                    fzrq=''.join(kk.xpath(".//td[6]//text()")).strip()
                    if fzrq.find('2021-03')>-1:
                        end='yes'
                        print('end=yes')
                        break
                    #save new url
                    if urlys!='':
                        url1='http://zjj.sz.gov.cn/ris/bol/szfdc/'+urlys
                        if url1 in okurl:
                            continue
                        if url1 in repeatlist:
                            continue
                        repeatlist.append(url1)
                        of.write(url1+'\n')
                        of.flush()
                total=int(re.findall('共(\d+)条',h1)[0])
                if total<10*pn:
                    break
                driver.execute_script(f"__doPostBack('AspNetPager1','{pn}')")
                
            except Exception as e:
                print(traceback.format_exc())
                time.sleep(30)
                continue
        driver.close()
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
        for i in range(2):
            t = Thread(target=self.run, args=())
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
            # time.sleep(1111)
    def run(self):
        while urls.qsize() != 0:
            print("qsize less="+str(urls.qsize()))   
            url1=urls.get()
            #http://zjj.sz.gov.cn/ris/bol/szfdc/certdetail.aspx?id=53733
            print(url1)
            try: 
                dict2 = {
                    "URL":url1,
                    "城市": '深圳',
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
                h1=self.getHtml(url1)
                while h1=='':
                    time.sleep(30)
                    print('sleep30')
                    h1=self.getHtml(url1)
                    
                time.sleep(0.1)
                table3 = etree.HTML(h1)
                # print(r.text)
                # table1 = etree.HTML(r.content.decode('gb2312'))
                dict2["预售许可证编号"] = self.trim(''.join(table3.xpath('//table[@class="table ta-c table2"]//tr[2]//td[2]//text()')))
                dict2["项目名称"] = self.trim(''.join(table3.xpath('//table[@class="table ta-c table2"]//tr[2]//td[4]//text()')))
                dict2["开发企业"] = self.trim(''.join(table3.xpath('//table[@class="table ta-c table2"]//tr[3]//td[2]//text()')))
                dict2["坐落位置"] = self.trim(''.join(table3.xpath('//table[@class="table ta-c table2"]//tr[3]//td[4]//text()')))
                dict2["预售证准许销售面积"] = self.trim(''.join(table3.xpath('//table[@class="table ta-c table2"]//tr[5]//td[4]//text()')))
                dict2["发证日期"] = self.trim(''.join(table3.xpath('//table[@class="table ta-c table2"]//tr[7]//td[2]//text()')))
                # dict2["url"] = url1
                if dict2["项目名称"]=='':
                    print('pro is null')
                    continue
                print(dict2["项目名称"])
                url2 = url1.replace('certdetail','projectdetail')
                ###
                time.sleep(0.1)
                h2=self.getHtml(url2)
                while h2=='':
                    time.sleep(30)
                    print('sleep30')
                    h2=self.getHtml(url2)
                table5 = etree.HTML(h2)
                ts=0
                m = re.findall('预售总套数</div>.*?</td>.*?>(\d+)',h2,re.S)
                if len(m)>0:
                    ts+=int(m[0].strip())
                m = re.findall('现售总套数</div>.*?</td>.*?>(\d+)',h2,re.S)
                if len(m)>0 and len(m[0].strip())>0:
                    ts+=int(m[0].strip())
                if ts==0:
                    ts=''.join(re.findall('<td>\s+套数\s+</td>\s+<td.*?>(.*?)</td>',h2,re.S)).strip()
                dict2["套数"] = str(ts)
                dict2["售楼电话"] = self.trim('/'.join(re.findall('售楼电话\S</div>.*?</td>.*?>(.*?)<',h2,re.S)))
                table55 = table5.xpath('//div[@class="record fix"]//table[1]//table//tr[position()>1]')
                if len(table55)==0:
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    continue
                for ff in table55:
                    urlxq="http://zjj.sz.gov.cn/ris/bol/szfdc/"+''.join(ff.xpath('.//td[5]//a/@href'))
                    lp=''.join(ff.xpath('.//td[2]//text()')).strip()
                    dict2['销售楼号']=lp
                    print(lp+' : '+urlxq)
                    time.sleep(0.1)
                    h3=self.getHtml(urlxq)
                    while h3=='':
                        time.sleep(30)
                        print('sleep30')
                        h3=self.getHtml(urlxq)
                    table7 = etree.HTML(h3)
                    #单元连接s
                    table77 = table7.xpath('//div[@class="left presale2Abox"]//a')
                    if len(table77)==0:
                        df = DataFrame(dict2, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    
                    #预售 单元
                    for jj in table77:
                        dy = ''.join(jj.xpath('.//text()'))
                        urldy = 'http://zjj.sz.gov.cn/ris/bol/szfdc/' + ''.join(jj.xpath('.//@href'))
                        time.sleep(0.2)
                        h4=self.getHtml(urldy)
                        while h4=='':
                            time.sleep(30)
                            print('sleep30')
                            h4=self.getHtml(urldy)
                        #户室
                        self.parseDetail(dict2, h4,dy)
                    #现售
                    # self.parsexs(dict2,urldy,r3,headers)
            except Exception as e:
                print(traceback.format_exc())
            # time.sleep(11111.1)
    def parseDetail(self, dict2, h4,dy):
            rooms=re.findall("<div align='center'>房号：(.*?)</div>\s+<div align='center'><a href='(housedetail\.aspx\?id=.*?)'.*?>(.*?)<",h4,re.S)
            print(len(rooms))
            for room in rooms:
                dict2['房号']=dy+'-'+room[0].strip()
                dict2['房屋销售状态']=room[2].strip()
                url19='http://zjj.sz.gov.cn/ris/bol/szfdc/'+room[1]
                time.sleep(0.2)
                h5=self.getHtml(url19)
                while h5=='':
                    time.sleep(30)
                    print('sleep30')
                    h5=self.getHtml(url19)
                table77=etree.HTML(h5)
                dict2["拟售价格"] = self.trim(''.join(table77.xpath('//table[@class="table ta-c table2 table-white"]//tr[2]//td[4]//text()')))
                dict2["房屋建筑面积"] =self.trim(''.join(table77.xpath('//table[@class="table ta-c table2 table-white"]//tr[5]//td[2]//text()')))
                df = DataFrame(dict2, index=[0])
                df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
            
            if len(rooms)==0:
                rooms=re.findall("<div align='center'>房号：(.*?)</div>",h4,re.S)
                if len(rooms)==0:
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                for room in rooms:
                    dict2['房号']=room
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                
            
    def parsexs(self,dict2,urlxq,r3,headers):
        table7 = etree.HTML(r3.content.decode('UTF-8'))
        __VIEWSTATEGENERATOR = ''.join(table7.xpath('//input[@name="__VIEWSTATEGENERATOR"]//@value'))
        __VIEWSTATEENCRYPTED = ''.join(table7.xpath('//input[@name="__VIEWSTATEENCRYPTED"]//@value'))
        __EVENTVALIDATION = ''.join(table7.xpath('//input[@name="__EVENTVALIDATION"]//@value'))
        __VIEWSTATE = ''.join(table7.xpath('//input[@name="__VIEWSTATE"]//@value'))
        print(__VIEWSTATEGENERATOR)
        params2 = {
            "scriptManager1": "updatepanel1 | imgBt2",
            "__EVENTTARGET": "imgBt2",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": __VIEWSTATE,
            "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
            "__VIEWSTATEENCRYPTED": __VIEWSTATEENCRYPTED,
            "__EVENTVALIDATION": __EVENTVALIDATION,
        }
        r13 = self.pool.post(urlxq, params2, timeout=60, headers=headers)
        time.sleep(0.1)
        table11 = table7.xpath('//div[@class="left presale2Abox"]//a')
        if len(table11)==0:
            df = DataFrame(dict2, index=[0])
            df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
        else:
            for jj in table11:
                dy = ''.join(jj.xpath('.//text()'))
                urldy = 'http://zjj.sz.gov.cn/ris/bol/szfdc/' + ''.join(jj.xpath('.//@href'))
                r4 = self.pool.get(urldy, timeout=60, headers=headers)
                self.parseDetail(dict2, headers, r4,dy)
    def main(self):
        self.parse_one()
        time.sleep(10)


run = sz_spider()
run.main()
