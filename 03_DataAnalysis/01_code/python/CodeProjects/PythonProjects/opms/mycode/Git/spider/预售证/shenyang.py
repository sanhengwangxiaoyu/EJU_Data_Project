# -*- coding:utf-8 -*-  
#

import requests
import re
from lxml import etree
import time,os
import json
import random
import html
from pandas import Series,DataFrame

from threading import Thread
from queue import Queue
import traceback

urls = Queue()
okurl = set()
listfile='list/shenyang_url.txt'
outfile='data/shenyang_new.txt'
okfile='data/shenyang.txt'

class sy_spider():
    def trim(self,word):
        return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')   
    def parse_one(self):
        #if has new
        has_new=0
        #
        if os.path.exists(outfile):
            with open(outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip().split('\t')
                    okurl.add(a[0])
                    has_new=1
        if os.path.exists(okfile):
            with open(okfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.strip().split('\t')
                    okurl.add(a[0])
        repeatlist=[]
        if os.path.exists(outlistfile):
            with open(outlistfile, 'r', encoding='utf-8') as f:
                for i in f:
                    repeatlist.append(i.split('\t')[0])
        
        #如果outfile没数据，需要创建并写一个表头
        if has_new==0:
            of = open(outfile,'a', encoding='utf-8') #
            of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
            of.flush()
            
        #翻页获取项目列表range(1,145):
        of = open(listfile,'a+', encoding='utf-8') #保存结果文件
        page=0
        headers ={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.02',
                'Connection': 'close',
            }
        end='no'
        while True:
            page+=1
            # self.pool.headers.update(headers)
            lburl = 'http://124.95.133.164/work/xjlp/new_building.jsp?page='+str(page)
            # r = self.pool.get(lburl,timeout=60, headers=headers)
            # r.content.decode('gbk')
            h1=self.getHtml(lburl)
            time.sleep(0.1)
            table1 = etree.HTML(h1)
            table2 = table1.xpath('//table//table[3]//table//tr[position()>1]')
            print(len(table2))
            for x in table2:
                try:
                    url8 = ''.join(x.xpath(".//td[1]//a//@href"));
                    url2 = 'http://124.95.133.164'+url8
                    dict2 = {
                        "URL":url2 ,
                        "城市": '沈阳',
                        "项目名称": ''.join(x.xpath(".//td[1]//text()")).replace('\r\n','').replace('\t',''),
                        "坐落位置": ''.join(x.xpath(".//td[2]//text()")),
                        "开发企业": ''.join(x.xpath(".//td[3]//text()")),
                        "预售许可证编号": '',
                        "发证日期": '',
                        "开盘日期": ''.join(x.xpath(".//td[4]//text()")),
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
                    #增量，翻页到2020年
                    if dict2['开盘日期'].find('2020')>0:
                        break
                    if url2 in okurl:
                        end='yes'
                        continue
                    if url2 in repeatlist:
                        end='yes'
                        continue
                    repeatlist.append(url2)
                    
                    self.do_write([dict2])
                except Exception as e:
                    print(traceback.format_exc())
                    continue
            #结束翻页
            if end=='yes':
                break
        #2多线程爬项目
        repeatlist=[]
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.strip().split('\t')
                if a[0] not in repeatlist and a[0] not in okurl:
                    urls.put(a)
                    # self.run()
                    # time.sleep(22222)
                    repeatlist.append(a[0])
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
    def run(self):
        # ua = UserAgent()
        while urls.qsize() != 0:
            rslist=[]
            print("qsize less="+str(urls.qsize()))   
            udict=urls.get()
            url2=udict[0]
            print('url2',url2)
            try:    
                dict2 = {
                    "URL":url2 ,
                    "城市": '沈阳',
                    "项目名称": udict[2],
                    "坐落位置": udict[3],
                    "开发企业": udict[4],
                    "预售许可证编号": '',
                    "发证日期": '',
                    "开盘日期": udict[7],
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
                time.sleep(3)
                r2=self.getHtml(url2)
                table3 = etree.HTML(r2)
                table4 = table3.xpath('//table//table//table//tr[position()>1]')
                #无信息时
                if table4==None or len(table4)==0:
                    print(1)
                    dict2["销售楼号"]=''
                    dict2["套数"]=''
                    self.do_write([dict2])
                    continue
                ##有信息时
                rslist=[]
                for n in table4:
                    dict3=dict2.copy()
                    dict3["销售楼号"]=''.join(n.xpath(".//td[1]//text()")).replace("\xa0","")
                    dict3["套数"]=''.join(n.xpath(".//td[5]//text()")).replace("\xa0","")
                    print(dict3["销售楼号"],dict3["套数"])
                    url4 = ''.join(n.xpath(".//td[1]//a//@href"));
                    # print(url4)
                    houseid = re.findall(r'houseid=(.*?)&', url4, re.I)[0]
                    url3='http://124.95.133.164/work/xjlp/door_list2.jsp?houseid='+houseid
                    time.sleep(3)
                    r3=self.getHtml(url3)
                    table5 = etree.HTML(r3)
                    table6 = table5.xpath('//table//td')
                    ###
                    if len(table6)==9:
                        rslist.append(dict3)
                        continue
                    for m in table6:
                        dict4=dict3.copy()
                        url9=''.join(m.xpath(".//a/@href"))
                        if(url9==''):
                            continue
                        # print(url9)
                        bgcolor=''.join(m.xpath('./@bgcolor'))
                        # print(bgcolor)
                        xszt =''.join(re.findall('xszt=(.*)',url9))
                        if len(xszt)==0:
                            if bgcolor.upper()=='#00FF00':
                                xszt='可售'
                            elif  bgcolor.upper()=='#CCFFFF':
                                xszt='未纳入网上销售'
                            elif  bgcolor.upper()=='#CCFF00':
                                xszt='现售'
                            elif  bgcolor.upper()=='#FFFF00':
                                xszt='已售'
                            elif  bgcolor.upper()=='#0099FF':
                                xszt='已发证'
                            elif  bgcolor.upper()=='#FF0000':
                                xszt='查封'
                        dict4["房号"]=self.trim(m.xpath("string(.)"))
                        dict4["房屋销售状态"]=xszt
                        #
                        if url9.find('?')==-1:
                            rslist.append(dict4)
                            continue
                        elif url9.find('124.95.133.164')==-1:
                            url9='http://124.95.133.164/'+url9
                        time.sleep(3)
                        html=self.getHtml(url9)
                        area=''.join(re.findall('建筑面积.*?font_lan">(.*?)</td>',html,re.S)).replace('&nbsp;','')
                        dict4["房屋建筑面积"]=area
                        rslist.append(dict4)
                #保存结果
                self.do_write(rslist)
            except Exception as e:
                print(traceback.format_exc())
                continue
    def do_write(self,rsdict):
        of = open(outfile,'a+', encoding='utf-8') #保存结果文件
        for dicts in rsdict:
            of.write(dicts.get('URL','')+'\t')
            of.write(dicts.get('城市','')+'\t')
            of.write(dicts.get('项目名称','')+'\t')
            of.write(dicts.get('坐落位置','')+'\t')
            of.write(dicts.get('开发企业','')+'\t')
            of.write(dicts.get('预售许可证编号','')+'\t')
            of.write(dicts.get('发证日期','')+'\t')
            of.write(dicts.get('开盘日期','')+'\t')
            of.write(dicts.get('预售证准许销售面积','')+'\t')
            of.write(dicts.get('销售状态','')+'\t')
            of.write(dicts.get('销售楼号','')+'\t')
            of.write(dicts.get('套数','')+'\t')
            of.write(dicts.get('面积','')+'\t')
            of.write(dicts.get('拟售价格','')+'\t')
            of.write(dicts.get('售楼电话','')+'\t')
            of.write(dicts.get('售楼地址','')+'\t')
            of.write(dicts.get('房号','')+'\t')
            of.write(dicts.get('房屋建筑面积','')+'\t')
            of.write(dicts.get('房屋销售状态',''))
            of.write("\n")
            of.flush()
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
        time.sleep(1)
run = sy_spider()
run.main()