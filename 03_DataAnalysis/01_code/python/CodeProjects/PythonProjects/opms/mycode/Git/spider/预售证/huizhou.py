# -*- coding:utf-8 -*-
#按翻页url中的预售证过滤

import requests
import re
import pandas as pd
from lxml import etree
import time
import json
from pandas import Series,DataFrame
import html
from threading import Thread
from queue import Queue
import traceback,os,random

urls = Queue()
okurl = set()

urls = Queue()
ysz_oklist = set()
listfile='list/huizhou_url.txt'
outfile='data/huizhou_new.txt'
okfile='data/huizhou.txt'

class huiz_spider():
    
    def getHtml(self,link):
        html=""
        # print(link)
        try: #使用try except方法进行各种异常处理
            header = {
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'+str(random.random()),
            } 
            res = requests.get(link,headers=header,timeout=30,verify=False) #读取网页源码
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
            print(e)
            print('html err')
        finally:
            return html
    def parse_one(self):
        ##已爬URL列表
        if os.path.exists(outfile):
            with open(outfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    ysz_oklist.add(re.findall('licenceCode=(.*?)(?:&|$)',a[0])[0])
        if os.path.exists(okfile):
            with open(okfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    ysz_oklist.add(re.findall('licenceCode=(.*?)(?:&|$)',a[0])[0])
        #
        repeatlist=[]
        if os.path.exists(listfile):
            with open(listfile, 'r', encoding='utf-8') as f:
                for i in f:
                    a = i.split('\t')
                    repeatlist.append(a[0])
        #
        of = open(listfile,'a+', encoding='utf-8') #保存结果文件
        end='no'
        page=0
        total=0
        while True:
            break
            #中断翻页
            if end=='yes':
                break
            page+=1
            try:
                lburl = 'http://data.fz0752.com/jygs/yszlist.shtml?code=&num=&name=&comp=&date1=&date2=&address=&pageNo='+str(page)
                time.sleep(3)
                text=self.getHtml(lburl)
                if text.find('你所查找的信息不存在')>0:
                    print('no data')
                    break
                table1 = etree.HTML(text)
                table2 = table1.xpath('//table[@id="data"]/tbody/tr')
                print('page=',page,len(table2))
                for kk in table2:
                        url1='http://data.fz0752.com'+''.join(kk.xpath('.//a[@class="external"]/@href'))
                        fzrq=kk.xpath('string(.)')
                        #设置中断翻页到2021-07-
                        if fzrq.find('2021-03-')>-1:
                            end='yes'
                            break
                        #
                        ysz=re.findall('licenceCode=(.*?)(?:&|$)',url1)[0]
                        if url1 in repeatlist or url1 in okurl:
                            continue
                        of.write(url1+'\n')
                        of.flush()
                
            except Exception as e:
                print(traceback.format_exc())
                continue
            if total==0:
                total=int(re.findall('doSearch\((\d+)\)">尾页</a>',text)[0])
            if total<=page:
                break
        #2多线程爬项目
        repeatlist=[]
        with open(listfile,'r', encoding='utf-8') as f:
            for i in f:
                a = i.strip()
                ysz=re.findall('licenceCode=(.*?)(?:&|$)',a)[0]
                if ysz not in repeatlist and ysz not in okurl:
                    urls.put(a)
                    repeatlist.append(ysz)
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
        while urls.qsize() != 0:
            print("qsize less="+str(urls.qsize()))   
            url1=urls.get()
            #http://zjj.sz.gov.cn/ris/bol/szfdc/certdetail.aspx?id=53733
            print(url1)
            try:      
                h1=self.getHtml(url1)
                while True:
                    if h1.find('访问过于频繁，请10分钟后再访问')>0:
                        print('sleep 10m')
                        time.sleep(61*10)
                        h1=self.getHtml(url1)
                    else:
                        break
                time.sleep(3)
                table3 = etree.HTML(h1)
                url99 = table3.xpath('//tbody[@class="center-center"]//tr')
                yszurl = 'http://data.fz0752.com'+''.join(table3.xpath('//tbody[@class="center-center"]//tr[1]//td[3]//a//@href'))
                pro=''.join(table3.xpath('//div[@class="head-title"]/text()')).replace('楼栋信息 - ','').strip()
                fzrq=''.join(table3.xpath('//tbody[@class="center-center"]/tr[1]/td[1]//text()')).strip()
                ysz=''.join(table3.xpath('//tbody[@class="center-center"]/tr[1]/td[3]//text()')).strip()
                h2='<html></html>'
                print('yszurl=',yszurl,fzrq,ysz,)
                if yszurl!='http://data.fz0752.com':
                    h2=self.getHtml(yszurl)
                    while True:
                        if h2.find('访问过于频繁，请10分钟后再访问')>0:
                            print('sleep 10m')
                            time.sleep(61*10)
                            h2=self.getHtml(yszurl)
                        else:
                            break
                table4 = etree.HTML(h2)
                dict1 = {
                    "url":url1,
                    "城市": '惠州',
                    "项目名称": pro,
                    "坐落位置": ''.join(table4.xpath('//table[@class="table table-bordered table-striped"]//tr[4]//td[1]//text()')).strip(),
                    "开发企业": ''.join(table4.xpath('//table[@class="table table-bordered table-striped"]//tr[2]//td[1]//text()')).strip(),
                    "预售许可证编号": ysz,
                    "发证日期": fzrq,
                    "开盘日期": '',
                    "预售证准许销售面积":''.join(table4.xpath('//table[@class="table table-bordered table-striped"]//tr[6]//td[1]//text()')).strip(),
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
                print(dict1)
                print('loupan=',len(url99))
                if len(url99)==0:
                    df = DataFrame(dict1, index=[0])
                    df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    continue
                ###
                for ss in url99:
                    dict2=dict1.copy()
                    tds=ss.xpath('./td[not(@style="display:none;")]')
                    dict2["销售楼号"] = tds[1].xpath('string(.)').strip()#''.join(ss.xpath('.//td[2]//text()')).strip()
                    dict2["套数"] =tds[4].xpath('string(.)').strip()#'#''.join(ss.xpath('.//td[6]//text()')).strip()
                    print(dict2["销售楼号"])
                    ldurl='http://data.fz0752.com'+''.join(ss.xpath('.//td[position()>3]/a//@href'))
                    ##########
                    if ldurl=="http://data.fz0752.com":
                        df = DataFrame(dict2, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                        continue
                    #######getrooms
                    print('ldurl',ldurl)
                    time.sleep(3)
                    h3=self.getHtml(ldurl)
                    while True:
                        if h3.find('访问过于频繁，请10分钟后再访问')>0:
                            print('sleep 10m')
                            time.sleep(61*10)
                            h3=self.getHtml(ldurl)
                        else:
                            break
                    m=re.findall('class="top-info">(.*?)</td>',h3,re.S)
                    if m :
                         dict2["销售楼号"]=m[0].strip()
                    
                    table4 = etree.HTML(h3.replace('惠民之家',''))
                    rooms=table4.xpath('//table[@id="data"]/tbody/tr')
                    if len(rooms)==0:
                        df = DataFrame(dict2, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                        continue
                    print('2rooms=',len(rooms))
                    lc=''
                    for room in rooms:
                        dict3=dict2.copy()
                        tds=room.xpath('./td')
                        ln=0
                        if len(tds)>11:
                            ln=len(tds)-1
                        if ln<10:
                            continue
                        zt=self.getstate(''.join(tds[ln].xpath('.//@src'))).strip()
                        if len(zt)==0:
                            zt=''.join(tds[ln].xpath('string(.)')).strip()
                        if len(tds)==15:
                            lc=''.join(room.xpath('./td[1]//text()')).strip()
                            fh=''.join(room.xpath('./td[2]//text()')).strip()
                            mj=''.join(room.xpath('./td[3]//text()')).strip()
                            jg=''.join(room.xpath('./td[13]//text()')).strip()+'元/㎡'
                        elif len(tds)==14:
                            lc=''.join(room.xpath('./td[1]//text()')).strip()
                            fh=''.join(room.xpath('./td[2]//text()')).strip()
                            mj=''.join(room.xpath('./td[3]//text()')).strip()
                            jg=''.join(room.xpath('./td[12]//text()')).strip()+'元/㎡'
                        else :
                            fh=''.join(room.xpath('./td[1]//text()')).strip()
                            mj=''.join(room.xpath('./td[2]//text()')).strip()
                            jg=''.join(room.xpath('./td[11]//text()')).strip()+'元/㎡'
                        
                        if len(fh)>0 and fh.find('层')<1:
                            fh=lc+'层'+fh
                        dict3['房号']=fh
                        dict3['拟售价格']=jg
                        dict3['房屋建筑面积']=mj
                        dict3['房屋销售状态']=zt
                        df = DataFrame(dict3, index=[0])
                        df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
                    
                time.sleep(0.3)
                    
            except Exception as e:
                print(traceback.format_exc())
                continue
    def getstate(self, imgurl):
        zt=imgurl
        if ('yrg.gif' in imgurl):
            zt = "已认购"
        elif ('ba.gif' in imgurl):
            zt = "已备案"
        elif ('ks.gif' in imgurl):
            zt = "可售"
        elif ('bks.gif' in imgurl):
            zt = "不可售"
        elif ('cq1.gif' in imgurl):
            zt = "重签"
        elif ('ysqy.gif' in imgurl):
            zt = "已签预售合同"
        elif ('xfqy.gif' in imgurl):
            zt = "现房已签约"
        elif ('ybz.gif' in imgurl):
            zt = "已办证"
        elif ('zhxs.gif' in imgurl):
            zt = "暂缓销售"
        elif ('bzz.gif' in imgurl):
            zt = "办证中"
        elif ('house/.gif' in imgurl):
            zt = "未知"
        return zt
    def main(self):
        self.parse_one()
        time.sleep(10)


run = huiz_spider()
run.main()
