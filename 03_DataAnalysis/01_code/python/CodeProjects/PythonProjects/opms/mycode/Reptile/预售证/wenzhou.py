# -*- coding:utf-8 -*-
#以url为增量，为预售证
import time,random
from lxml import etree

import re,os
import requests
import json
import traceback

from threading import Thread
from queue import Queue

urls = Queue()
oklist=set()

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/wenzhou_new.txt"
outlistfile="./list/wenzhou.txt"
okfile="./data/wenzhou.txt"

url='http://www.wzfg.com/realweb/stat/ProjectSellingList.jsp?currPage=$1&permitNo=&projectName=&projectAddr=&region=&num=$1'
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
        # 'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        # 'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        # 'Accept-Encoding':'gzip, deflate',
        'Content-Type':'application/json',
        'Origin':'http://221.182.157.163:8848',
        'Referer':url,
        'host':'221.182.157.163:9002',
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
    city='温州'
    area=''
    rslist=[]
    ceil={}
    detail=''
    newhtml=''
    progect=''
    company=''
    addr=''
    ysz=''
    fzdt=''
    area=''
    try:
        #pro
        h1=getHtml(contenturl)
        tab1 = etree.HTML(h1)
        tab11=tab1.xpath('//div[@id="saleInfo"]/table')[0]
        ceil = {
                "城市": city,
                "项目名称": trim(''.join(tab11.xpath('./tr[7]/td[2]//text()'))),
                "坐落位置":trim(''.join(tab11.xpath('./tr[8]/td[2]/text()'))), 
                "开发企业": trim(''.join(tab11.xpath('./tr[1]/td[2]/a/text()'))),
                "预售许可证编号":trim(''.join(tab11.xpath('./tr[2]/td[2]/text()'))),
                "发证日期":''.join(tab11.xpath('./tr[3]/td[2]/text()')).strip(),
                "开盘日期": trim(''.join(tab11.xpath('./tr[9]/td[2]/text()'))),
                "预售证准许销售面积":trim(''.join(tab11.xpath('./tr[6]/td[2]/text()'))),
                "销售状态": '',
                "销售楼号": '',
                "套数": '',
                "面积": '',
                "拟售价格": '',
                "售楼电话": trim(''.join(tab11.xpath('./tr[11]/td[2]//text()'))),
                "售楼地址": trim(''.join(tab11.xpath('./tr[10]/td[2]/text()'))),
                "房号": '',
                "房屋建筑面积": '',
                "房屋销售状态": '',
        }
        # print(ceil)
        ##lou dongs
        loudongs=tab1.xpath("//a[@name='floortitle']")
        for loudong in loudongs:
            ceil2=ceil.copy()
            ldid=''.join(loudong.xpath('./@href')).replace('javascript:ShowBld("','').replace('")','')
            title=''.join(loudong.xpath('./@title'))
            ceil2['销售楼号']=''.join(re.findall('·幢名：(\S+)',title,re.S)).strip()
            ceil2['套数']=''.join(re.findall('·户室数：(\S+)',title,re.S)).strip()
            ceil2['面积']=''.join(re.findall('总建筑面积：(\S+)',title,re.S)).strip()
            tab_room_trs=tab1.xpath(f'//table[@id="Bt{ldid}"]/tr/td/a')
            if len(tab_room_trs)==0:
                rslist.append(ceil2)
                continue
            #
            for room in tab_room_trs:
                state=''.join(room.xpath('./@class'))
                if state=='':
                    continue
                ceil3=ceil2.copy()
                ceil3['房屋销售状态']=getState(state).strip()
                fh=trim(room.xpath('string(.)')).replace('◆','')
                ceil3['房号']=fh
                title2=''.join(room.xpath('./@title')).split('\r\n')
                # print(title2)
                if len(title2)>1:
                    ceil3['房屋建筑面积']=title2[0].strip()
                    ceil3['拟售价格']=title2[1].replace('一房一价：','').strip()
                elif len(title2)==1:
                    ceil3['房屋建筑面积']=title2[0].strip()
                rslist.append(ceil3)
            print(ceil2['销售楼号'])
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
    #添加首行
    if has_new==0:
        of = open(outfile,'a', encoding='utf-8') #保存结果文件
        of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
        of.flush()
    ##
    of = open(outlistfile,'a', encoding='utf-8') #保存结果文件，按url增量
    pn=0
    while True :
        pn+=1
        html=getHtml(url.replace('$1',str(pn)))
        tab1 = etree.HTML(html)
        hrefs=re.findall('(FirstHandProjectInfo\.jsp\?projectID=\d+)',html,re.S)
        #########
        print('page=',pn,len(hrefs))
        if len(hrefs)==0:
            break
        for j in hrefs:
            contenturl='http://www.wzfg.com/realweb/stat/'+j
            if contenturl in repeatlist:
                continue
            repeatlist.append(contenturl)
            of.write(f'{contenturl}\n')
        #搂年份中断
        if html.find('字(2020)第')>0:
            print('2020 break')
            break
    if os.path.exists(outlistfile):
        with open(outlistfile,'r',encoding='UTF-8') as txtData: 
            for line in txtData.readlines():
                contenturl=line.strip()
                if contenturl in oklist:
                    continue
                oklist.add(contenturl)
                urls.put(contenturl)
                
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
        ths = []
        for i in range(1):
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