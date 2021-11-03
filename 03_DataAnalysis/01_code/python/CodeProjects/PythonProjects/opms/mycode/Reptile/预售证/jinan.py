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

listfile='list/jinan_url.txt'
outfile='data/jinan_new.txt'
okfile='data/jinan.txt'

class jn_spider():                    # method_whitelist=frozenset(['GET', 'POST']))))
    def getHtml(self,link):
        html=""
        try:
            header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.02' }
            res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
            #解码
            res.encoding='UTF-8'
            html=res.text
        except Exception as e:
            print(e)
        finally:
            return html             
    def parse_one(self):
        listlist=[]
        if os.path.exists(listfile):
            with open(listfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    listlist.append(a[0])
        ##
        ###########
        of = open(listfile,'a', encoding='utf-8') #保存结果文件
        page=0
        while True:
            page+=1
            try:
                lburl = 'http://124.128.246.22:8090/onsaling/index_'+str(page)+'.shtml'
                h1 = self.getHtml(lburl)
                table1 = etree.HTML(h1)
                table2 = table1.xpath('//div[@class="project_content"]//table//tr[position()>1 and position()<22]')
                print('page=',page,len(table2))
                if len(table2)==0 or h1.find('无任何数据')>0:
                    break
                for kk in table2:
                    url2 = 'http://124.128.246.22:8090'+''.join(kk.xpath('.//td[2]//@href'))
                    if url2 in listlist:
                        continue
                    listlist.append(url2)
                    of.write(url2+'\n')
                    of.flush()
            except Exception as e:
                print(traceback.format_exc())
                break        
        #2多线程爬项目
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
        ##
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.strip()
                if a not in okurl:
                    urls.put(a)
                    okurl.add(a)
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
        ths = []
        for i in range(10):
            t = Thread(target=self.run, args=())
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
    def run(self):
        while urls.qsize() != 0:
            print("qsize less="+str(urls.qsize()))   
            url2=urls.get()
            print(url2)
            try:
                dict2 = {
                        "URL":url2,
                        "城市": '济南',
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
                h3=self.getHtml(url2)
                table3 = etree.HTML(h3)
                pro_table=table3.xpath('//table[@class="message_table"]')
                dict2['项目名称']=''.join(pro_table[0].xpath('./tr[2]/td[2]')[0].xpath('string(.)')).strip()
                dict2['坐落位置']=''.join(pro_table[0].xpath('./tr[2]/td[4]')[0].xpath('string(.)')).strip()
                dict2['开发企业']=''.join(pro_table[0].xpath('./tr[3]/td[2]')[0].xpath('string(.)')).strip()
                dict2['售楼电话']=''.join(pro_table[0].xpath('./tr[5]/td[2]')[0].xpath('string(.)')).strip()
                dict2['售楼地址']=''.join(pro_table[0].xpath('./tr[5]/td[4]')[0].xpath('string(.)')).strip()
                table4 = table3.xpath('//div[@class="project_content"]//table//tr[position()>1 and position()<22]')
                pages=1
                m=re.findall('/(\d+)页',h3)
                if m :
                    pages=int(m[0])
                while True:
                    for jj in table4:
                        dict2["销售楼号"]=''.join(jj.xpath('.//td[2]//@title'))
                        dict2["预售许可证编号"] = ''.join(jj.xpath('.//td[3]//@title'))
                        dict2["套数"] = ''.join(jj.xpath('.//td[64]//text()'))
                        dict2["预售证准许销售面积"] = ''.join(jj.xpath('.//td[5]//text()'))
                        dict2["面积"] = ''.join(jj.xpath('.//td[7]//text()'))
                        df = DataFrame(dict2, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    pages=pages-1
                    if pages==0:
                        break
                    h4=self.getHtml(url2.replace('.shtml','_'+str(pages)+'.shtml'))
                    table4 =etree.HTML(h4).xpath('//div[@class="project_content"]//table//tr[position()>1 and position()<22]')
            except Exception as e:
                print(traceback.format_exc())
                continue
    def main(self):
        self.parse_one()
        time.sleep(10)
run = jn_spider()
run.main()