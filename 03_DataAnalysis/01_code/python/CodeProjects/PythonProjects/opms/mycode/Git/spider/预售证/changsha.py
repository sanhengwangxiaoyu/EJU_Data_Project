# -*- coding:utf-8 -*-  
#预售证在项目下面，但通过预售证的发布时间来获取需要重新爬的项目，在项目里再根据预售证号筛选新增的爬取。每次爬要重写待爬urllist列表

import time,random
from lxml import etree
import urllib.parse
import re,os
import requests
import json
import traceback
from selenium import webdriver

from threading import Thread
from queue import Queue

urls = Queue()
ysz_oklist=set()
outfile="./data/changsha_new.txt"
outlistfile="./list/changsha.txt"
okfile="./data/changsha.txt"

url='http://www.cszjxx.net/sefloor?p=$1'

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')
def get_data(contenturl):
    city='长沙'
    area=''
    rslist=[]
    cell={}
    detail=''
    newhtml=''
    progect=contenturl[1].strip()
    company=''
    addr=''
    ysz=''
    fzrq=''
    area=''

    try:
        #pro
        cell = {
                "城市": city,
                "项目名称": progect,
                "坐落位置":'',
                "开发企业": '',
                "预售许可证编号":'',
                "发证日期":'',
                "开盘日期":'',
                "预售证准许销售面积":'',
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
        ###http://222.240.149.21:8081/floorinfo/202103160174
        u1=contenturl[0]
        driver = webdriver.Firefox()
        driver.get(u1)
        driver.implicitly_wait(10)     
        time.sleep(4)
        h1=driver.page_source
        tab1=etree.HTML(h1)
        cell['开发企业']=''.join(tab1.xpath('//div[@class="hs_xqxx"]/table//tr[3]/td[2]/text()')).strip()
        cell['坐落位置']=''.join(tab1.xpath('//div[@class="hs_xqxx"]/table//tr[4]/td[2]/text()')).strip()
        cell['售楼地址']=''.join(tab1.xpath('//div[@class="hs_xqxx"]/table//tr[5]/td[2]/text()')).strip()
        cell['售楼电话']=''.join(tab1.xpath('//div[@class="hs_xqxx"]/table//tr[5]/td[4]/text()')).strip()
        cell['拟售价格']=''.join(tab1.xpath('//div[@class="hs_xqxx"]/table//tr[4]/td[4]/text()')).strip()
        print(cell)
        longdongs=tab1.xpath('//div[@class="hs_table"]/table/tbody/tr[position()>1]')
        print('longdongs=',len(longdongs))
        if len(longdongs)==0:
            rslist.append(cell)
            return rslist
        time.sleep(1)   
        ###########楼列表
        for longdong in longdongs:
            tds=longdong.xpath('./td')
            if len(tds)<9:
                continue
            cell2=cell.copy()
            cell2['预售许可证编号']=tds[0].xpath('string(.)').strip()
            cell2['销售楼号']=tds[1].xpath('string(.)').strip()
            cell2['发证日期']=tds[2].xpath('string(.)').strip()
            cell2['预售证准许销售面积']=tds[3].xpath('string(.)').strip()
            ##过滤项目内已爬过的预售证
            if cell2['预售许可证编号'] in ysz_oklist:
                continue
            
            ###http://222.240.149.21:8081/hslist?{ywzh:"200601040147",n:"1"
            tmp=tds[8].xpath('./@onclick')
            if len(tmp)==0:
                rslist.append(cell2)
                continue
            
            driver.execute_script(tmp[0])
            yszid=re.findall("hsjajx\('(.*?)'",tmp[0])[0]
            time.sleep(3)
            h2=driver.page_source
            tab2=etree.HTML(h2)
            rooms=tab2.xpath('//div[@id="hs_table2_'+yszid+'"]//tr[position()>1]')
            print('rooms=',len(rooms))
            if len(rooms)==0:
                rslist.append(cell2)
                continue
            cell2['套数']=str(len(rooms))
            for room in rooms:
                cell3=cell2.copy()
                cell3['房号']=''.join(room.xpath('./td[1]/text()')).strip()
                cell3['房屋建筑面积']=''.join(room.xpath('./td[6]/text()')).strip()
                cell3['房屋销售状态']=''.join(room.xpath('./td[9]/text()')).strip()
                rslist.append(cell3)
    except Exception as e:
        print(traceback.format_exc()) 
    finally:
        driver.close()
    # print(rslist)
    return rslist
def openList(filename):
    datalist=[]
    try:
        if  os.path.exists(filename):
            with open(filename,'r',encoding='UTF-8') as txtData: 
                for line in txtData.readlines():
                    datalist.append(line.split('\t')[0])
    except Exception as e:
        print(traceback.format_exc())
    return datalist
def do_write(outfile,url,rsdict):
    of = open(outfile,'a+', encoding='utf-8') #保存结果文件
    for dicts in rsdict:
        of.write(url+"\t")
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
def getHtml(link,refer):
    html=""
    try: #使用try except方法进行各种异常处理
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'
            }
        res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
        #解码
        print(link)
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
    finally:
        return html

def main():
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
    ##
    listlist=[]
    end='1'
    if end!='yes':
        of = open(outlistfile,'w', encoding='utf-8') #保存结果文件
    else:
        of = open(outlistfile,'a', encoding='utf-8') #保存结果文件
    pn=0
    while True and end!='yes':
        #翻页中断
        if end=='yes':
            break
        pn+=1
        html=getHtml(url.replace('$1',str(pn)),'')
        while html.find('此刻访问人数过多，请稍后进行尝试')>0:
            time.sleep(5)
            html=getHtml(url.replace('$1',str(pn)),'')
        tab=etree.HTML(html)
        #########
        hrefs=tab.xpath('//div[@class="hs_xqxx"]/table/tr[position()>1]')
        print(len(hrefs))
        if len(hrefs)==0:
            break
        for j in hrefs:
            contenturl=''.join(j.xpath('./td[1]//a/@href'))
            fzsj=''.join(j.xpath('./td[4]//text()')).strip()
            ysz=''.join(j.xpath('./td[3]//text()')).strip()
            pro=''.join(j.xpath('./td[1]//text()')).strip()
            print(ysz,fzsj)
            #翻页中断设置2021
            if fzsj.find('2021/03/')>-1:
                end='yes'
                break
            #已爬过的预售证，过滤
            if ysz in ysz_oklist:
                continue
            if contenturl.find('sefloor')>0:
                continue
            if contenturl not in listlist:
                listlist.append(contenturl)
                of.write(contenturl+'\t'+pro+'\n')
                of.flush()
        time.sleep(3)
    ####
    listlist=[]
    with open(outlistfile,'r',encoding='UTF-8') as txtData: 
        for line in txtData.readlines():
            contenturl=line.split('\t')
            if contenturl[0] in listlist:
                continue
            listlist.append(contenturl[0])
            urls.put(contenturl)
            
            # outstr=get_data(contenturl)
            # do_write(outfile,contenturl[0],outstr)
            # time.sleep(2222)
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
        ths = []
        for i in range(5):
            t = Thread(target=run, args=())
            t.start()
            ths.append(t)
        for t in ths:
            t.join()
def run():
    while urls.qsize() != 0: 
        contenturl=urls.get()
        print("qsize less="+str(urls.qsize()))
        print(contenturl)
        outstr=get_data(contenturl)
        do_write(outfile,contenturl[0],outstr)
            # time.sleep(10)

def savefile(datalist,filename):
    old_datalist=[]
    try:
        with open(filename,'r') as txtData: 
            for line in txtData.readlines():
                old_datalist.append(line.strip())
    except Exception as e:
        print(traceback.format_exc())
    of = open(filename,'a+', encoding='utf-8') #保存文件
    for data in datalist:
        if datalist not in old_datalist:
            of.write(data+"\n")
            of.flush()              
if __name__ == '__main__':
		main()