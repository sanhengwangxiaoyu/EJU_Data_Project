# -*- coding:utf-8 -*-  
#预售证在项目下面，同一个项目的预售证时间跨度可能很大，所以需要全爬项目，再根据已爬的的预售证进行过滤
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
ysz_oklist = set()

listfile='list/qingdao_url.txt'
outfile='data/qingdao_new.txt'
okfile='data/qingdao.txt'

class qd_spider():
    def getHtml(self,link):
        html=""
        try:
            header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.02' }
            res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
            #解码
            res.encoding='GBK'
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
                    a = i.strip().split('\t')
                    listlist.append(a[0])
        ###############
        page=0
        while True:
            break
            page+=1
            try:
                lburl = 'https://www.qdfd.com.cn/qdweb/realweb/fh/FhProjectQuery.jsp?selState=2&page='+str(page)+'&rows=20&okey=&order='
                # r = self.pool.get(lburl,timeout=60, headers=headers)
                h1=self.getHtml(lburl)
                time.sleep(0.1)
                table2 = etree.HTML(h1).xpath('//tr[@class="RowOdd" or @class="RowEven"]')
                print("page=",page,len(table2))
                if len(table2)==0:
                    break
                for kk in table2:
                    dict2 = {
                        'url': '',
                        "城市": '青岛',
                        "项目名称": ''.join(kk.xpath(".//td[2]//text()")),
                        "坐落位置": ''.join(kk.xpath(".//td[3]//text()")),
                        "开发企业": ''.join(kk.xpath(".//td[6]//text()")),
                        "预售许可证编号": '',
                        "发证日期": '',
                        "开盘日期": '',
                        "预售证准许销售面积": ''.join(kk.xpath(".//td[5]//text()")),
                        "销售状态": ''.join(kk.xpath(".//td[1]//text()")),
                        "销售楼号": '',
                        "套数": ''.join(kk.xpath(".//td[4]//text()")),
                        "面积": '',
                        "拟售价格": '',
                        "售楼电话": '',
                        "售楼地址": '',
                        "房号": '',
                        "房屋建筑面积": '',
                        "房屋销售状态": ''
                    }
                    id1=''.join(kk.xpath(".//td[2]//a//@href"))
                    id2=id1.replace('javascript:detailProjectInfo("','').replace('")','')
                    url2='https://www.qdfd.com.cn/qdweb/realweb/fh/FhProjectInfo.jsp?projectID='+id2
                    dict2["url"] = url2
                    if url2 in listlist:
                        continue
                    df = DataFrame(dict2, index=[0])
                    df.to_csv(listfile, sep='\t', mode='a', index=False, header=None)
            except Exception as e:
                print(traceback.format_exc())
                continue
        ##已爬预售证列表
        if os.path.exists(outfile):
            with open(outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    if len(a)<6:
                        print(a)
                        continue
                    ysz_oklist.add(a[5])
                    
        if os.path.exists(okfile):
            with open(okfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    ysz_oklist.add(a[5])
        repeatlist=[]
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                if repeatlist not in repeatlist:
                    urls.put(a)
                    # self.run()
                    # time.sleep(11111)
                    repeatlist.append(a[0])
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
            print("qsize less=",urls.qsize())   
            alist=urls.get()    
            url2=alist[0]
            print('url2',url2)
            try:
                dict2 = {
                    'url': url2,
                    "城市": '青岛',
                    "项目名称": alist[2],
                    "坐落位置": alist[3],
                    "开发企业": alist[4],
                    "预售许可证编号": '',
                    "发证日期": '',
                    "开盘日期": '',
                    "预售证准许销售面积": alist[8],
                    "销售状态": alist[9],
                    "销售楼号": '',
                    "套数": alist[11],
                    "面积": '',
                    "拟售价格": '',
                    "售楼电话": '',
                    "售楼地址": '',
                    "房号": '',
                    "房屋建筑面积": '',
                    "房屋销售状态": ''
                }
                h2=self.getHtml(url2)
                time.sleep(0.1)
                table3 = etree.HTML(h2)
                table4 = table3.xpath('//tr[@class="RowOdd" or @class="RowEven"]')
                for jj in table4:
                    ysz=''.join(jj.xpath(".//td[2]//text()"))
                    print(ysz)
                    if ysz in ysz_oklist:
                        print('pass ysz=',ysz)
                        continue
                    dict2["预售许可证编号"]=ysz
                    dict2["开盘日期"] = ''.join(jj.xpath(".//td[3]//text()"))
                    dict2["售楼地址"] = ''.join(jj.xpath(".//td[4]//text()"))
                    dict2["售楼电话"] = ''.join(jj.xpath(".//td[5]//text()"))
                    dict2["套数"] = ''.join(jj.xpath(".//td[6]//text()"))
                    dict2["面积"] = ''.join(jj.xpath(".//td[7]//text()"))
                    ysid=''.join(jj.xpath(".//td[2]//a//@href"))
                    preid = ysid.replace('"','').replace('javascript:getBuilingList(','').replace(')','').split(',')
                    print(preid)
                    url3="https://www.qdfd.com.cn/qdweb/realweb/fh/FhBuildingList.jsp?preid="+ preid[0]
                    h3=self.getHtml(url3)
                    time.sleep(0.1)
                    table5 = etree.HTML(h3)
                    table6 = table5.xpath('//tr[@class="RowOdd" or @class="RowEven"]')
                    rslist=[]
                    for mm in table6:
                        dict3=dict2.copy()
                        dict3["销售楼号"] = ''.join(mm.xpath(".//td[1]//text()"))
                        dict3["拟售价格"] = ''.join(mm.xpath(".//td[2]//text()"))
                        url4=''.join(mm.xpath(".//td[1]//a//@href"))
                        url44=url4.replace('"','').replace('javascript:showHouseStatus(','').replace(')','').split(',')
                        print('url44',url44)
                        url5='https://www.qdfd.com.cn/qdweb/realweb/fh/FhHouseStatus.jsp?buildingID='+url44[0]+'&startID='+url44[1]+'&projectID='+url44[2]
                        h4=self.getHtml(url5)
                        time.sleep(0.1)
                        table8 =  etree.HTML(h4).xpath('//td[@class="tableBorder3"]')
                        print('table8',len(table8))
                        if len(table8)==0:
                            rslist.append(dict3)
                            continue
                        for nn in table8:
                            dict4=dict3.copy()
                            fh=''.join(nn.xpath(".//text()")).split("  ")[0]
                            color=''.join(nn.xpath(".//@bgcolor"))
                            dict4["房屋销售状态"] =self.getstate(color)
                            dict4["房号"] = fh
                            rslist.append(dict4)
                    if len(rslist)==0:
                        df = DataFrame(dict2, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    else:
                        for c in rslist:
                            df = DataFrame(c, index=[0])
                            df.to_csv(outfile, sep='\t', mode='a',encoding = "utf-8", index=False, header=None)
            except Exception as e:
                print(traceback.format_exc())
            
    def getstate(self, color):
        state = color
        if (color == '#FFFF00'):
            state = '已签约'
        elif (color == '#FF0000'):
            state = '已登记'
        elif (color == '#00C200'):
            state = '可售'
        elif (color == '#FF00FF'):
            state = '已付定金'
        elif (color == '#CC99FF'):
            state = '抵押'
        elif (color == '#000CC'):
            state = '限制'
        elif (color == '#FFFFFF'):
            state = '未纳入网上销售'
        return state
    def main(self):
        self.parse_one()
        time.sleep(10)

run = qd_spider()
run.main()