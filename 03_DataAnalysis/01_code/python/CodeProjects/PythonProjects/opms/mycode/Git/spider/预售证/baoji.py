# -*- coding:utf-8 -*-
#按URL增量爬虫，为预售证url

import queue,time,random
from lxml import etree

import re,os
import requests
import json
import xlrd
import traceback
from queue import Queue

outfile="./data/baoji_new.txt"
outlistfile="./list/baoji.txt"
okfile="./data/baoji.txt"

url="http://61.185.68.162/bit-xxzs/xmlpzs/webissue.asp?page="

urls = Queue()
okurl = set()

def trim(word):
    return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')   
def get_data(contenturl):
    html=getHtml(contenturl)
    lxmls = etree.HTML(html)
    trs =[]
    city='宝鸡'
    area=''
    rslist=[]
    cell={}
    detail=''
    newhtml=''
    progect=''
    company=''
    addr=''
    ysz=''
    fzdt=''
    area=''
    m=re.findall(r'项目名称：</div></td>.*?colspan="5">(.*?)</td>',html,re.S)
    if m :
        progect=trim(m[0])
    m=re.findall(r'开 发 商：</div></td>.*?colspan="5">(.*?)</td>',html,re.S)
    if m :
        company=trim(m[0])
    m=re.findall(r'项目位置：</div></td>.*?colspan="5">(.*?)</td>',html,re.S)
    if m :
        addr=trim(m[0])
    m=re.findall(r'预售证号：</div></td>.*?colspan="5">(.*?)</td>',html,re.S)
    if m :
        ysz=trim(m[0])
    m=re.findall(r'批准时间：</div></td>.*?colspan="5">(.*?)</td>',html,re.S)
    if m :
        fzdt=m[0].strip()
    m=re.findall(r'楼栋面积：</div></td>.*?colspan="5">(.*?)</td>',html,re.S)
    if m :
        area=m[0].strip()
    cell['城市']=city
    cell['项目名称']=progect
    cell['开发企业']=company
    cell['坐落位置']=addr
    cell['预售许可证编号']=ysz
    cell['发证日期']=fzdt
    cell['预售证准许销售面积']=area
    #
    try:
        ######项目
        html=trim(html)
        trs = re.findall("builddetail\.asp\?buildid=(.*?)'>([^<]*?)</a></td><td>([^<]*?)</td><td>([^<]*?)</td><td>.*?</td><td>([^<]*?)</td>",html,re.S)
        print('len楼栋 ='+str(len(trs)))
        if len(trs)>0:
            for tr in trs:
                lh=tr[1]
                ts=tr[2]
                mj=tr[3]
                buildID=tr[0]
                # url2=f'http://61.185.68.162/ytt/business/buildingRooms_xml.asp?functiontype=6&sid=0.36282906349926647&client_showMode=&client_stanID=1610&client_realtypeID=-1&client_mainTable=unrelatedresource&client_mainno=0&client_buildID={buildID}&floorStart=-100&floorEnd=-100&roomNoStart=-100&roomNoEnd=-100&cancelBldroomShow=1&client_showiscansale=0&client_showRoomCond=&pmBldRoomID=undefined'
                cell2=cell.copy() 
                cell2['销售楼号']=lh
                cell2['面积']=mj
                cell2['套数']=ts
                # h3=getHtml(url2)
                rslist.append(cell2)
        else:
            rslist.append(cell)
    except Exception as e:
        print(traceback.format_exc())
    # print(rslist)
    return rslist
def do_write(outfile,url,rsdict):
    of = open(outfile,'a+', encoding='utf-8') #保存结果文件
    for dicts in rsdict:
        of.write(url+"\t")
        of.write(dicts.get('城市','')+'\t')
        of.write(trim(dicts.get('项目名称',''))+'\t')
        of.write(trim(dicts.get('坐落位置',''))+'\t')
        of.write(trim(dicts.get('开发企业',''))+'\t')
        of.write(trim(dicts.get('预售许可证编号',''))+'\t')
        of.write(dicts.get('发证日期','').strip()+'\t')
        of.write(dicts.get('开盘日期','').strip()+'\t')
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
def getHtml(link):
    html=""
    try: #使用try except方法进行各种异常处理
        header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
        res = requests.get(link,headers=header,timeout=10,verify=False) #读取网页源码
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
    #
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:                  
                okurl.add(i.split('\t')[0])
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                okurl.add(i.split('\t')[0])
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile, 'r', encoding='utf-8') as f:
            for i in f:
                repeatlist.append(i.split('\t')[0])
    #爬列表，vc有时效性，每轮重写，不需要翻页
    of = open(outlistfile,'a', encoding='utf-8') #保存结果文件
    pn=0
    total=0
    while True :
        pn+=1
        html=getHtml(url+str(pn))
        if total==0:
            total=int(re.findall('共有(.*?)页',html)[0])
        ######### 
        hrefs =re.findall('href="(bsdetail\.asp\?id=.*?)" target="_blank">(.*?)</a>',html)
        print('page=',pn,len(hrefs))
        for href in hrefs:
            contenturl="http://61.185.68.162/bit-xxzs/xmlpzs/"+href[0]
            if contenturl in okurl or contenturl in repeatlist:
                continue
            of.write(contenturl+'\t'+href[1]+'\n')
            of.flush()
        #中断设置
        # if html.find('</td><td>2021-01')>0:
            # break
        if total<=pn:
            break
    #爬详细
    repeatlist=[]    
    with open(outlistfile, 'r', encoding='utf-8') as f:
         for i in f:
            s=i.split('\t')
            if s[0] in okurl:
                print('pass1')
                continue
            if s[0] in repeatlist:
                print('pass2')
                continue
            repeatlist.append(s[0])
            urls.put(s[0])
    #
    print('qsize=',urls.qsize())
    while urls.qsize()!=0:
        # contenturl='http://zzgj.haikou.gov.cn/xxgk/tdgl/tdzpggg/201911/t20191105_1461423.html' #
        print('qsize less=',urls.qsize())
        contenturl=urls.get()
        outstr=get_data(contenturl)
        # print(outstr)
        do_write(outfile,contenturl,outstr)
        # time.sleep(10)    
           
if __name__ == '__main__':
		main()