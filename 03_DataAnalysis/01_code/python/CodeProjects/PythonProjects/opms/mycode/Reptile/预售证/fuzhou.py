# -*- coding:utf-8 -*-  
#按URL读取预售证，然后判断为当前预售证进行2次爬取，爬虫过滤按预售证号过滤。项目URL为可变量
#预售证列表：http://222.77.178.63:7002/Presell.asp?projectID=MTA3Mzh8MjAyMS83LzI3fDU0&projectname=%B1%A3%C0%FB%D6%D0%D4%C3%B9%AB%B9%DD%A3%A8%D2%BB%C7%F8%A3%A9
#楼栋列表：http://222.77.178.63:7002/building.asp?ProjectID=MTA3Mzh8MjAyMS83LzI3fDU0&projectName=%B1%A3%C0%FB%D6%D0%D4%C3%B9%AB%B9%DD%A3%A8%D2%BB%C7%F8%A3%A9&PreSell_ID=12509&Start_ID=12509


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

listfile='list/fuzhou_url.txt'
outfile='data/fuzhou_new.txt'
okfile='data/fuzhou.txt'

urls = Queue()
ysz_list = set()

class jn_spider():
    # def __init__(self):
        # self.pool = requests.session()
        # self.pool.mount('http://', HTTPAdapter(pool_connections=100, pool_maxsize=200,
                                                # max_retries=Retry(total=10, backoff_factor=1,
    def getHtml(self,link):
        html=""
        while True:
            try: #使用try except方法进行各种异常处理
                header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.02' }
                res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
                #解码
                res.encoding='GBK'
                html=res.text
                break
            except Exception as e:
                print(e)
                if str(e).find('Read timed out')>0:
                    time.sleep(1)
                    continue
                break
        return html
  
    def trim(self,word):
        return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')   
    def parse_one(self):
        if os.path.exists(outfile):
            with open(outfile,'r',encoding='utf-8') as txtData: 
                for line in txtData.readlines():
                    if len(line.split('\t'))<6:
                        continue
                    ysz_list.add(line.split('\t')[5])
        if os.path.exists(okfile):
            with open(okfile,'r',encoding='utf-8') as txtData: 
                for line in txtData.readlines():
                    if len(line.split('\t'))<6:
                        continue
                    ysz_list.add(line.split('\t')[5])
        ##########
        of = open(listfile,'a+', encoding='utf-8') #保存结果文件
        #翻页获取预售证列表
        #按URL读取预售证，然后判断为当前预售证进行2次爬取，爬虫过滤按预售证号过滤
        page=0
        end='no'
        while True:
            page+=1
            try:
                # 翻页中断
                if end=='yes':
                    break
                print("已抓取"+str(page))
                lburl = 'http://222.77.178.63:7002/result_new.asp?page2='+str(page)+'&xm_search=&zl_search=&gs_search=&pzs_search=&pzx_search=&SelectXZQ=&SelectBK='
                res11=self.getHtml(lburl)
                table1 = etree.HTML(res11)
                table2 = table1.xpath('//table[@bordercolor="#000000"]//tr[@height="25"]')
                urllist=[]
                if len(table2)==0:
                    break
                for jj in table2:
                    url2='http://222.77.178.63:7002/'+''.join(jj.xpath('.//td[2]//a/@href'))
                    fzrq=''.join(jj.xpath('.//td[5]/text()'))
                    # 翻页中断设置2020年
                    # if fzrq.find('2020/')>-1:
                        # end='yes'
                        # break
                    ysz=''.join(jj.xpath('.//td[1]/text()'))
                    if ysz in ysz_list:
                        continue
                    print(url2,ysz)
                    urllist.append(url2+'\t'+ysz)
                of.write('\n'.join(urllist)+'\n')
                of.flush()
                time.sleep(2)
            except Exception as e:
                print(traceback.format_exc())
                continue
            if end=='yes':
                break
        #2多线程爬项目
        #按项目名称过滤，爬一次即可
        repeatlist=[]
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.strip().split('\t')
                if len(a) ==2 and a[1] not in ysz_list:
                    ysz_list.add(a[1])
                    urls.put((a[0],a[1]))
        print(urls.qsize())           
        time.sleep(3)
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
            print('qsize==== : ',urls.qsize())
            url2,ysz=urls.get()
            print(url2,ysz)
            rslist=[]
            try:
                dict2 = {
                    "URL": url2,
                    "城市": '福州',
                    "项目名称": '',
                    "坐落位置": '',
                    "开发企业": '',
                    "预售许可证编号": ysz,
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
                time.sleep(0.2)
                res2=self.getHtml(url2)
                m=re.findall('项目地址：</td>.*?class="indextabletxt">(.*?)</td>',res2,re.S)
                if m :
                    dict2['坐落位置']=self.trim(m[0])
                m=re.findall('项目名称：</td>.*?class="indextabletxt">(.*?)</td>',res2,re.S)
                if m :
                    dict2['项目名称']=self.trim(m[0])
                m=re.findall('href="Enterprisedetail.*?>(.*?)</a>',res2,re.S)
                if m :
                    dict2['开发企业']=self.trim(m[0])
               
                url3 ='http://222.77.178.63:7002/'+''.join(re.findall("name='SUList' src='(.*?)'",res2,re.S))
                if url3=='http://222.77.178.63:7002/':
                    # df = DataFrame(dict2, index=[0])
                    # df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    continue
                print('url3='+url3)
                time.sleep(0.2)
                res3=self.getHtml(url3)
                table5 = etree.HTML(res3).xpath('//table//tr[position()>1]')
                #ysz list
                rslist=[]
                for mm in table5:
                    this_ysz=''.join(mm.xpath('.//td[1]//text()')).replace("许可证","")
                    if this_ysz!=ysz:
                        continue
                    dict2["发证日期"] =self.trim(''.join(mm.xpath('.//td[4]//text()')).replace("批准日期",""))
                    dict2["套数"] = self.trim(''.join(mm.xpath('.//td[5]//text()')).replace("总套数",""))
                    dict2["预售证准许销售面积"] =self.trim(''.join(mm.xpath('.//td[7]//text()')).replace("总面积",""))
                    url4='http://222.77.178.63:7002/'+''.join(mm.xpath('.//td[1]//a/@href'))
                    print('url4='+url4)
                    time.sleep(0.2)
                    res4=self.getHtml(url4)
                    table7 = etree.HTML(res4).xpath('//table//tr[@class="indextabletxt"]')
                    # if len(table7)==0:
                        # df = DataFrame(dict2, index=[0])
                        # df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                        
                    #lou list
                    
                    for hh in table7:
                        dict3=dict2.copy()
                        dict3["销售楼号"] = self.trim(''.join(hh.xpath('.//td[1]//text()')))
                        print(dict2["销售楼号"])
                        url5= 'http://222.77.178.63:7002/' + ''.join(hh.xpath('.//td[1]//a//@href'))
                        time.sleep(0.2)
                        res5=self.getHtml(url5)
                        table9 = etree.HTML(res5).xpath('//table[@bgcolor="cccccc"]//tr/td/table[2]/tr')
                        if len(table9)==0:
                            rslist.append(dict3)
                            continue
                        #room list
                        for tr in table9:
                            tds=tr.xpath('./td')
                            for tdnum in range(1,len(tds)):
                                rr=tds[tdnum]
                                dict2["房号"] = self.trim(''.join(rr.xpath('.//text()')))
                                dict2["房屋销售状态"] = self.getstate(''.join(rr.xpath('.//@bgcolor')))
                                dict2["房屋建筑面积"] =  self.trim(self.trim(''.join(rr.xpath('.//@title'))).replace('预测面积：',''))
                                url55 = ''.join(rr.xpath('.//a/@href'))
                                if(url55!=''):
                                    time.sleep(0.1)
                                    res55=self.getHtml('http://222.77.178.63:7002/'+url55)
                                    dict2["拟售价格"] =self.trim(re.findall(r'单价.*?bgcolor="fafafa" style="padding:3">(.*?)<', res55, re.S)[0])
                                rslist.append(dict3)
                                time.sleep(0.1)
                for c in rslist:
                    df = DataFrame(c, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                time.sleep(1)
            except Exception as e:
                    print(traceback.format_exc())
                    print("异常")
                    continue
    def getstate(self,color):
        state = color
        if(color=='#00FF00'):
            state='可售'
        elif(color=='#9999FF'):
            state = '抵押'
        elif (color == '#FFFF00'):
            state = '限制'
        elif (color == '#FFFFFF'):
            state = '未纳入网上销售'
        elif (color == '#FF0000'):
            state = '已登记'
        elif (color == '#FF3399'):
            state = '已签'
        return state

    def main(self):
        self.parse_one()
run = jn_spider()
run.main()