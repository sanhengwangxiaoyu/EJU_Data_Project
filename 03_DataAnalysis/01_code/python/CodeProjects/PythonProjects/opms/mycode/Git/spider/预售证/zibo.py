# -*- coding:utf-8 -*-
#按URL增量，表示楼、预
import time,random
from lxml import etree
import urllib.parse
import re,os
import requests
import json
import traceback

from threading import Thread
from queue import Queue
urls = Queue()
oklist=set()

outfile="./data/zibo_new.txt"
outlistfile="./list/zibo.txt"
okfile="./data/zibo.txt"

url='http://zbfdc.com.cn/web/webxml/selectXmList?curPage=$1&pageSize=1000&xmbh=&ssq=&xxmc=&qymc=&xmdz=&propertyRights=&propertyRightsPeople='

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')

def get_data(contenturl):
    city='淄博'
    area=''
    rslist=[]
    ceil={}
    detail=''
    newhtml=''
    lpbh=re.findall('xmbh=(.*?)&',contenturl)[0]
    try:
        #pro
        ceil = {
                "城市": city,
                "项目名称": '',
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
        ###
        u1=f'http://zbfdc.com.cn/web/webxml/selectXmList?curPage=1&pageSize=1&xmbh={lpbh}&ssq=&xxmc=&qymc=&xmdz=&propertyRights=&propertyRightsPeople='
        h1=getHtml(u1,'http://zbfdc.com.cn/html/sale.html')
        js=json.loads(h1)['list']['list'][0]
        ceil['项目名称']=js.get('xmmc','')
        ceil['坐落位置']=js.get('xmdz','')
        ceil['开发企业']=js.get('kfgs','')
        ceil['预售许可证编号']=js.get('ysxkzh','')
        ceil['发证日期']=js.get('fzsj','')
        ceil['预售证准许销售面积']=str(js.get('mj',''))
        ceil['销售状态']=getLpzt(int(js.get('lpzt',-1)),1)
        ceil['销售楼号']=js.get('lpmc','')
        ceil['套数']=str(js.get('ts',''))
        ##
        u2='http://zbfdc.com.cn/web/webxml/selectFwList?xmbh='+lpbh
        h2=getHtml(u2,'http://zbfdc.com.cn/html/sale.html')
        js=json.loads(h2)['list']['list']
        if len(js)==0:
            rslist.append(ceil)
            return rslist
        
        for room in js:
            ceil2=ceil.copy()
            ceil2['房号']=room.get('fh','')
            ceil2['房屋建筑面积']=str(room.get('jzmj',''))+'m2'
            ceil2['拟售价格']=str(room.get('ysjg',''))+'元'
            st=room['ysfwzt']
            if st=='' and room.get('isetIssinglesale','')==4:
                st='不可销售'
            try:
                st=getLpzt(int(room['ysfwzt']),2)
            except:
                pass
            ceil2['房屋销售状态']=st
            rslist.append(ceil2)
    except Exception as e:
        print(traceback.format_exc()) 
    
    # print(rslist)
    return rslist
def getLpzt(lpzt, type):
    if (lpzt == 0) :
        if (type == 1) :
            lpzt = '未开盘 '
        else:
            lpzt = '禁止销售 '
    elif (lpzt == 1):
        if (type == 1):
            lpzt = '已开盘 '
        else:
            lpzt = '未纳入销售'
    elif (lpzt == 2):
        if (type == 1):
            lpzt = '已封盘 '
        else:
            lpzt = '售中 '
    elif (lpzt == 3):
        lpzt = '可售'
    elif (lpzt == 4):
        lpzt = '预定'
    elif (lpzt == 5):
        if (type == 2):
            lpzt = '售中'
        else:
            lpzt = '已签'
    elif (lpzt == 6):
        lpzt = '已售 '
    elif (lpzt == 7):
        lpzt = '登记预告 '
    elif (lpzt == 8):
        lpzt = '抵押预告'
    elif (lpzt == 9):
        lpzt = '权证发放'
    elif (lpzt == 10):
        lpzt = '转移预告'
    elif (lpzt == 11):
        lpzt = '持证抵押'
    elif (lpzt == 12):
        lpzt = '查封'
    elif (lpzt == 13):
        lpzt = '在建工程抵押'
    elif (lpzt == 14):
        lpzt = '异议'
    elif (lpzt == 15):
        lpzt = '租赁'
    elif (lpzt == 16):
        lpzt = '物业用房'
    elif (lpzt == 17):
        lpzt = '其他限制'
    elif (lpzt == 20):
        lpzt = '已售'
  
    return lpzt
def do_write(outfile,url,rsdict):
    of = open(outfile,'a+', encoding='utf-8') #保存结果文件
    for dicts in rsdict:
        of.write(url+"\t")
        of.write(dicts.get('城市','')+'\t')
        of.write(trim(dicts.get('项目名称',''))+'\t')
        of.write(trim(dicts.get('坐落位置',''))+'\t')
        of.write(trim(dicts.get('开发企业',''))+'\t')
        of.write(trim(dicts.get('预售许可证编号',''))+'\t')
        of.write(trim(dicts.get('发证日期',''))+'\t')
        of.write(trim(dicts.get('开盘日期',''))+'\t')
        of.write(trim(dicts.get('预售证准许销售面积',''))+'\t')
        of.write(trim(dicts.get('销售状态',''))+'\t')
        of.write(trim(dicts.get('销售楼号',''))+'\t')
        of.write(trim(dicts.get('套数',''))+'\t')
        of.write(trim(dicts.get('面积',''))+'\t')
        of.write(trim(dicts.get('拟售价格',''))+'\t')
        of.write(trim(dicts.get('售楼电话',''))+'\t')
        of.write(trim(dicts.get('售楼地址',''))+'\t')
        of.write(trim(dicts.get('房号',''))+'\t')
        of.write(trim(dicts.get('房屋建筑面积',''))+'\t')
        of.write(trim(dicts.get('房屋销售状态','')))
        of.write("\n")
        of.flush()
def getHtml(link,refer):
    html=""
    try: #使用try except方法进行各种异常处理
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0 Win64 x64 rv:86.0) Gecko/20100101 Firefox/86.0',
            'Accept': '*/*',
            'Referer': refer
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
    has_new=0
    #已爬列表
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')                    
                oklist.add(a[0])
                has_new=1
                
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                oklist.add(a[0])
    
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                repeatlist.append(a[0])
    ##
    #如果outfile没数据，需要创建并写一个表头
    if has_new==0:
        of = open(outfile,'a', encoding='utf-8') #保存结果文件
        of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
        of.flush()
    #
    of = open(outlistfile,'a+', encoding='utf-8') #保存结果文件
    pn=0
    while True :
        pn+=1
        html=getHtml(url.replace('$1',str(pn)),'http://zbfdc.com.cn/html/Advance.html')
        js=json.loads(html)
        total=js['total']
        #########
        hrefs=js['list']['list']
        print('page,len=',pn,len(hrefs))
        if len(hrefs)==0:
            break   
        for j in hrefs:
            lpbh=j['lpbh']
            contenturl='http://zbfdc.com.cn/web/webxml/selectXmList?curPage=1&pageSize=1&xmbh='+lpbh+'&ssq=&xxmc=&qymc=&xmdz=&propertyRights=&propertyRightsPeople='
            if contenturl in repeatlist:
                continue
            repeatlist.append(contenturl)
            of.write(contenturl+'\n')
            of.flush() 
        if total<=pn*1000:
            break
    #
    with open(outlistfile,'r',encoding='UTF-8') as txtData: 
        for line in txtData.readlines():
            contenturl=line.strip()
            if contenturl in oklist:
                continue
            urls.put(contenturl)
            oklist.add(contenturl)
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
        ths = []
        for i in range(3):
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
        do_write(outfile,contenturl,outstr)
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