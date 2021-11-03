# -*- coding:utf-8 -*-
import time,random
from lxml import etree
import base64

import re,os
import requests
import json
import traceback

from threading import Thread
from queue import Queue

urls = Queue()
oklist=set()

outfile="./data/changzhou_new.txt"
outlistfile="./list/changzhou.txt"
okfile="./data/changzhou.txt"

url='http://gs.czfdc.com.cn/newxgs/Pages/Code/Xjfas.ashx?method=GetYszData&ysxkz=&kfs=&lpmc=&page=$1'
def uncodeX(word):
    word=re.sub('\\+','',word)
    print('['+word+']')
    return word.encode('l1').decode('utf8')
def getState(word):
    #在本项目内
    if word=='G1':
        return '在售'
    if word=='G6':
        return '已签合同'
    if word=='G4':
        return '非出售'
    if word=='G7':
        return '合同已登记'
    if word=='G10':
        return '已认购'
    if word=='G3':
        return '自留房'
    if word=='G2':
        return '安置房'
    if word=='G5':
        return '已签预定协议'
    #不在本项目内
    if word=='B1':
        return '正常发售'
    if word=='B2':
        return '安置房'
    if word=='B3':
        return '自留房'
    if word=='B4':
        return '非出售'
    if word=='B5':
        return '已签预定协议'
    if word=='B6':
        return '已签合同'
    if word=='B7':
        return '合同已登记'
    if word=='B10':
        return '已认购'
    if word=='B':
        return '不在任何项目内'
    #以下，2018-8-12改版  
    if word=='h2':
        return '统一'
    if word=='h3':
        return '限制'
    if word=='h4':
        return '抵押(按揭)'
    if word=='h5':
        return '产权'
    return word
def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','').replace('&nbsp;','')
def postHtml(url,data):
    #使用try except方法进行各种异常处理
    header = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
        # 'Accept':'application/json, text/javascript, */*; q=0.01',
        # 'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        # 'Accept-Encoding':'gzip, deflate',
        'Content-Type':'application/json',
        'Origin':'http://gs.czfdc.com.cn/',
        'Referer':url,
        'host':'gs.czfdc.com.cn/',
        'Cache-Control':'no-cache'
    }
    #data=json.loads(js)
    try:
        res=requests.post(url=url,data=data,headers=header,verify=False)
        
        # #调用js函数
        # print(res.call('__doPostBack','AspNetPager1',2))
        res.encoding='UTF-8'
        return res.text
        
    except Exception as e:
        print(traceback.format_exc())
    return ''
def get_data(contenturl):
    city='常州'
    area=''
    rslist=[]
    ceil={}
    detail=''
    newhtml=''
    progect=contenturl[1]
    company=contenturl[2]
    addr=contenturl[6].strip()
    ysz=contenturl[3]
    fzdt=contenturl[4]
    area=contenturl[5]
    try:
        #pro
        ceil = {
                "城市": city,
                "项目名称": progect,
                "坐落位置":addr, 
                "开发企业": company,
                "预售许可证编号":ysz,
                "发证日期":fzdt,
                "开盘日期":'',
                "预售证准许销售面积":area,
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
        print(ceil)
        ##
        ##longdong list
        h1=getHtml(contenturl[0])
        loudongs=json.loads(h1)
        if len(loudongs)==0:
            return [ceil]
        #
        for loudong in loudongs:
            ceil2=ceil.copy()
            ld=loudong['housenum']
            houseid=loudong['houseid']
            psaleid=loudong['psaleid']
            ceil2['销售楼号']=ld
            u2=f'http://gs.czfdc.com.cn/newxgs/Pages/Code/Xjfas.ashx?method=GetShowData&houseID={houseid}&saleSate=&cell=&psaleID={psaleid}'
            print(u2)
            h2=getHtml2(u2,'')
            a=base64.b64decode(h2)
            try:
                td=eval(a)
            except Exception as e :
                td=str(traceback.format_exc())
            
            rooms=re.findall('"svl":"(.*?)","curcell":"(.*?)","roomlabel":"(.*?)".*?,"BuildingArea":(.*?),"ContractPrice":(.*?),',td)
            print('rooms=',len(rooms))
            # time.sleep(11111)
            for room in rooms:
                ceil3=ceil2.copy()
                ceil3['套数']=str(len(rooms))
                ceil3['房屋销售状态']=room[0]
                ceil3['房号']=room[2]
                ceil3['房屋建筑面积']=room[3]
                ceil3['拟售价格']=room[4]
                rslist.append(ceil3)
                # print(ceil3)
        if len(rslist)==0:
            rslist.append(ceil)
    except Exception as e:
        print(traceback.format_exc()) 
    
    # print(rslist)
    # time.sleep(11111)
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
def getHtml(link):
    html=""
    # print(link)
    try: #使用try except方法进行各种异常处理
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'
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
        res.encoding='GBK'
        html=res.text
    except Exception as e:
        print(e)
    finally:
        return html
def getHtml2(link,id):
    html=""
    # print(link)
    try: #使用try except方法进行各种异常处理
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
            'Cookie': f'npsaleid={id}; wjbazt=; ASP.NET_SessionId=3b2mtoowny5c2dxpkwrvgidt;CNZZDATA915400=cnzz_eid%3D257811575-1619411532-%26ntime%3D1619411532; UM_distinctid=1790c9a3f071f0-077d05e1d885288-4b5f451b-144000-1790c9a3f08581',
            'Referer':'http://gs.czfdc.com.cn/newxgs/Pages/Lp_Show.aspx?a=0.38875284136369764',
           
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
        res.encoding='utf-8'
        html=res.text
    except Exception as e:
        print(e)
    finally:
        return html
def main():
   #已爬列表
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')                    
                oklist.add(a[0])
                
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
    ##
    of = open(outlistfile,'a+', encoding='utf-8') #保存结果文件
    pn=0
    total=0
    while True : 
        break
        pn+=1
        html=getHtml(url.replace('$1',str(pn)))
        js=json.loads(json.loads(html))['Rows']
        if total==0:
            total=int(re.findall('Total\\\\":\\\\\"(.*?)\\\\\"',html)[0])
            print(total)
        #########
        if len(js)==0:
            print('js=0 err end')
            break
        print('page=',pn,len(js))
        for j in js: 
            psaleID=j.get('psaleid','')
            ysz=j.get('PRENUM','')
            fzrq=j.get('PresaleCertificateDate','')
            sp=j.get('NAME','')
            pro=j.get('PRJNAME','')
            addr=str(j.get('BSIT',''))
            yszmj=str(j.get('YSCANSALEROOMBAREA',''))
            contenturl='http://gs.czfdc.com.cn/newxgs/Pages/Code/Xjfas.ashx?method=GetLpLzList&id='+psaleID
            
            # print(psaleID,ysz,fzrq,sp,pro,addr,yszmj)
            if contenturl in repeatlist:
                continue
            if contenturl not in oklist:
                of.write(contenturl+'\t'+pro+'\t'+sp+'\t'+ysz+'\t'+fzrq+'\t'+yszmj+'\t'+addr)
                of.flush()
        #设置翻页结束
        if total<=pn*10:
            print('end ok')
            break
        #设置翻页中断到出2020年
        if html.find('(2020)房预售证')>0:
            print('break end')
            # break
    if os.path.exists(outlistfile):
        with open(outlistfile,'r',encoding='UTF-8') as txtData: 
            for line in txtData.readlines():
                contenturl=line.split('\t')
                if contenturl[0] in oklist:
                    continue
                urls.put(contenturl)
                # outstr=get_data(contenturl)
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
        # time.sleep(1)    

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