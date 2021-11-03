# coding=gbk
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

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/changchun_new.txt"
outlistfile="./list/changchun.txt"
okfile="./data/changchun.txt"

url='http://www.ccfdw.net/ecdomain/ccfcjwz/wsba/ysxk/search.jsp?xkzh=&xkz_num=&xmmc=&sfdw=&flag=1&nowPage=$1'

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')

def get_data(contenturl):
    city='长春'
    area=''
    rslist=[]
    ceil={}
    detail=''
    newhtml=''
    try:
        #pro
        h1=getHtml('http://'+contenturl,'')
        saleid=''.join(re.findall("opczfy\('(.*?)'",h1,re.S))
        lhlist=''.join(re.findall('预售范围</td>.*?>(.*?)<',h1,re.S)).strip()
        if saleid!=None:
            h2=getHtml('http://www.ccfdw.net/ecdomain/lpcs/xmxx/xmjbxxinfo.jsp?Id_xmxq='+saleid,'')
        else:
            h2=''
        
        ceil = {
                "城市": city,
                "项目名称": ''.join(re.findall('onclick="opczfy.*?>(.*?)<',h1,re.S)).strip(),
                "坐落位置": ''.join(re.findall('项目地址</td>.*?>(.*?)<',h1,re.S)).strip(),
                "开发企业": ''.join(re.findall('开发企业名称</td>.*?>(.*?)<',h1,re.S)).strip(),
                "预售许可证编号":''.join(re.findall('预售许可证编号</td>.*?>(.*?)<',h1,re.S)).strip(),
                "发证日期":''.join(re.findall('发证日期</td>.*?>(.*?)<',h1,re.S)).strip(),
                "开盘日期":''.join(re.findall('开盘日期： </td>.*?>(.*?)<',h2,re.S)).strip(),
                "预售证准许销售面积":''.join(re.findall('预售总建筑面积</td>.*?>(.*?)<',h1,re.S)).strip(),
                "销售状态": '',
                "销售楼号": '',
                "套数": '',
                "面积": '',
                "拟售价格": ''.join(re.findall('参考价：<.*?>(.*?)</td>',h2,re.S)).replace('</font>','').strip(),
                "售楼电话": re.sub('<.*?>','',''.join(re.findall("padding-left:90px;font-family:'ArialBlack';\">(.*?)</td>",h2,re.S))).strip(),
                "售楼地址": ''.join(re.findall('售楼地址： </td>.*?>(.*?)<',h2,re.S)).strip(),
                "房号": '',
                "房屋建筑面积": '',
                "房屋销售状态": '',
        }
        
        ###lou  list 
        h2=getHtml('http://www.ccfdw.net/ecdomain/lpcs/xmxx/loulist.jsp?Id_xmxq='+saleid,'')
        ldid_list=re.findall("searchByLid\('(.*?)'",h2,re.S)
        if len(ldid_list)==0:
            rslist.append(ceil)
        ##
        for ldid in ldid_list:
            ceil2=ceil.copy()
            h3=getHtml('http://www.ccfdw.net/ecdomain/lpcs/xmxx/lpbxx_new.jsp?lid='+ldid,'')
            tab3 = etree.HTML(h3)
            ysz=''.join(tab3.xpath('//div[@id="content1"]/table/tr[1]/td[2]//text()')).strip()
            print(ysz)
            if ysz!=ceil['预售许可证编号']:
                continue
            ldmc=''.join(tab3.xpath('//div[@id="content1"]/table/tr[2]/td[2]/text()')).strip()
            ts=''.join(tab3.xpath('//div[@id="content1"]/table/tr[4]/td[2]/text()')).strip()
            mj=''.join(tab3.xpath('//div[@id="content1"]/table/tr[4]/td[4]/text()')).strip()
            ceil2['销售楼号']=ldmc
            ceil2['套数']=ts.replace('总','')
            ceil2['预售证准许销售面积']=mj
            print('loupan=',ldmc,'ts=',ts)
            rooms=re.findall("searchByLid\('(.*?)'",h3,re.S)
            if len(rooms)==0:
                rslist.append(ceil2)
                continue
            ###rooms
            for room in rooms:
                ceil3=ceil2.copy()
                h4=getHtml('http://www.ccfdw.net/ecdomain/lpcs/xmxx/huxx.jsp?hid='+room+'&lid='+ldid,'')
                price=''.join(re.findall('预售价格.*?</td>.*?>(.*?)</td>',h4,re.S)).strip()
                sale=''.join(re.findall('房屋状态.*?</td>.*?>(.*?)</td>',h4,re.S)).strip()
                if '可售' not in sale :
                    sale='不可售'
                fh=''.join(re.findall('房间号.*?</td>.*?>(.*?)</td>',h4,re.S)).strip()
                jzmj=''.join(re.findall('预测建筑面积.*? </td>.*?>(.*?)</td>',h4,re.S)).strip()
                if len(price)>3:
                    ceil3['拟售价格']=price
                ceil3['房号']=fh
                ceil3['房屋建筑面积']=jzmj
                ceil3['房屋销售状态']=sale
                rslist.append(ceil3)
                # print(ceil3)
                # time.sleep(32222)
    except Exception as e:
        print(traceback.format_exc()) 
    if len(rslist)==0:
        rslist.append(ceil)
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
            'User-Agent':'%E6%88%BF%E7%AE%A1%E5%B1%80/1 CFNetwork/978.0.7 Darwin/18.7.0',
            'Content-Type':'application/x-www-form-urlencoded',
            'Accept': '*/*',
            'Referer': refer
            }
        res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
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
    finally:
        return html
def postHtml(url,data):
    #使用try except方法进行各种异常处理
    header = {
            'User-Agent':'%E6%88%BF%E7%AE%A1%E5%B1%80/1 CFNetwork/978.0.7 Darwin/18.7.0',
            'Content-Type':'application/x-www-form-urlencoded',
            'Cookie':'JSESSIONID=2420324B8617B7EACA7D596EE868CF02; JSESSIONID=5D1E7B5718E5C996ABC768E0C9A842AC',
            'Accept': '*/*'
            }
    #data=json.loads(js)
    try:
        res=requests.post(url=url,data=data,headers=header,verify=False)
        res.encoding='UTF-8'
        return res.text
    except Exception as e:
        print(traceback.format_exc())
    return ''
def main():
    oklist=openList(okfile)
    oklist1={}
    for i in oklist:
        oklist1[i]=''
    outlist=openList(outfile)
    for i in outlist:
        oklist1[i]=''
    newlist=[]
    of = open(outlistfile,'a+', encoding='utf-8') #保存结果文件
    end='no'
    pn=0
    while True :
        pn+=1
        print(pn) 
        html=getHtml(url.replace('$1',str(pn)),'')
        hrefs=re.findall('openUrl\(\'(.*?)\'\).*?>(.*?)</a>',html,re.S)
        total=int(re.findall('当前第\d+/(\d+)页',html,re.S)[0])
        print(len(hrefs),total)
        if len(hrefs)==0:
            break   
        for j,pro in hrefs:
            contenturl='www.ccfdw.net/ecdomain/ccfcjwz/wsba/ysxk/searchByInfoId.jsp?id='+j
            #增量爬虫，翻页截止年份
            if pro.find('2020')>0:                
                print('end')
                end='yes'
                break
            if contenturl in newlist:
                continue
            if contenturl in outlist:
                print('exists')
                continue
            newlist.append(contenturl)
            of.write(contenturl+'\n')
            of.flush() 
           
        if total<=pn or end=='yes':
            break
   
    with open(outlistfile,'r',encoding='UTF-8') as txtData: 
        for line in txtData.readlines():
            contenturl=line.strip()
            if contenturl in oklist1:
                continue
            urls.put(contenturl)
            # outstr=get_data(contenturl)
            # do_write(outfile,contenturl,outstr)
            # time.sleep(1111)
        print("qsize="+str(urls.qsize())) 
        time.sleep(3)       
        ths = []
        for i in range(10):
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