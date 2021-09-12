# -*- coding:utf-8 -*-  
# 按URL增量，为预售证
import time,random
from lxml import etree
import urllib.parse
import re,os
import requests
import json
import traceback

from dateutil.parser import parse
from threading import Thread
from queue import Queue
urls = Queue()
okurl=set()

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/xianyang_new.txt"
outlistfile="./list/xianyang.txt"
okfile="./data/xianyang.txt"

#
url='http://zjj.xys12345.cn/api/facetSearch?pageIndex=$1&count=10&categoryId=1119&word='
def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')

def get_data(dicts):
    city='咸阳'
    area=''
    rslist=[]
    cell={}
    detail=''
    newhtml=''
    try:
        #dicts=[contenturl,pro,sp,addr,tel]
        #pro
        h1=getHtml(dicts[0],'')
        h1=re.sub('(\s+)','',h1)
        h1=h1.replace('\\t','').replace('\\n','').replace('</br>',',')
        tab1=json.loads(h1)['column']['data']
        js=json.loads(tab1)
        # try:
            # js=str(json.loads(tab1))
        # except:
            # js=str(tab1)
        print(js)
        cell = {
                "城市": city,
                "项目名称": dicts[1].strip(),
                "坐落位置": js.get('fwzl'),#js.get('fwzl',''),
                "开发企业": js.get('xmmc',''),#''.join(re.findall('"xmmc":"(.*?)"',h1,re.S)).strip(),#,
                "预售许可证编号":'威房预售字第'+js.get('djh','')+'号',
                "发证日期":dicts[2].strip(),
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
        print(cell)
        #"lhy":"1","jgy":"钢筋混凝土结构","sgy":"地上34层、地下2层","fwyty":"住宅","jzmjy":"住宅：17684.92","zztsy":"186套"
        # data=re.findall('"lh\w+":"([^\"]*?)","jg\w+":"[^\"]*?","sg\w+":"[^\"]*?","fwyt\w+":"[^\"]*?","jzmj\w+":"([^\"]*?)","zzts\w+":"([^\"]*?)"',str(tab1),re.S)
        # # print(data)
        # for d in data:
            # if len(d[0])==0:
                # break
            # cell2=cell.copy()
            # cell2['销售楼号']=d[0]
            # cell2['面积']=d[1]
            # cell2['套数']=d[2]
            # rslist.append(cell2)
        lhnum=['y','e','s','ss','w']
        if len(rslist)==0:
            for num in lhnum:
                lh=js.get(f'lh{num}','')
                if len(lh)==0:
                    continue
                cell2=cell.copy()
                cell2['销售楼号']=lh
                cell2['面积']=js.get(f'jzmj{num}','')
                cell2['套数']=js.get(f'zzts{num}','')
                rslist.append(cell2)
            #1
            #2
            #3
            #4
            #5
            
    except Exception as e:
        print(traceback.format_exc()) 
    if len(rslist)==0:
        rslist.append(cell)
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
        of.write(dicts.get('发证日期','').replace('\r','').replace('\n','').strip()+'\t')
        of.write(dicts.get('开盘日期','').replace('\r','').replace('\n','').strip()+'\t')
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
            }
        res = requests.get(link,headers=header,timeout=20,verify=False) #读取网页源码
        #解码
        # print(link)
        res.encoding='UTF-8'
        html=res.text
    except Exception as e:
        print(e)
    finally:
        return html

def main():
    #获取已爬list
    has_new=0
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                okurl.add(a[0])
                has_new=1
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                okurl.add(a[0])
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile, 'r', encoding='utf-8') as f:
            for i in f:
                repeatlist.append(i.split('\t')[0])
    #如果outfile没数据，需要创建并写一个表头
    if has_new==0:
        of = open(outfile,'a', encoding='utf-8') #保存结果文件
        of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
        of.flush()
    
    #翻页爬取预售证列表
    of = open(outlistfile,'a', encoding='utf-8') #保存结果文件
    pn=0
    total=0
    while True :
        break
        pn+=1
        html=getHtml(url.replace('$1',str(pn)),'').replace('&nbsp;','')
        js=json.loads(html)['page']['list']
        if total==0:
            total=int(re.findall('totalCount":(\d+)',html)[0])
        print('count=',total,'thispage=',pn,'thisnum=',len(js))
        if len(js)==0:
            print('fs=0 end')
            break
        
        for j in js:
            contenturl=f'http://zjj.xys12345.cn/api/sqNewInfo?id={j["id"]}'
            pro=j.get('title','').strip()
            fzrq=j.get('publishDate')
            fzrq=time.localtime(int(str(fzrq)[0:10]))
            fzrq=time.strftime("%Y-%m-%d",fzrq)
            rs=[contenturl,pro,fzrq]
            if contenturl in okurl:
                continue
            if contenturl in repeatlist:
                continue
            repeatlist.append(contenturl)
            of.write('\t'.join(rs)+'\n')
            of.flush()
        #翻页结束
        if total<=pn*10:
            break
    
    #爬虫房信息
    with open(outlistfile,'r',encoding='UTF-8') as txtData: 
        for line in txtData.readlines():
            contenturl=line.replace('\n','').split('\t')
            if contenturl[0] in okurl:
                continue
            urls.put(contenturl)
            okurl.add(contenturl[0])
            # print(contenturl)
            # # outstr=get_data(contenturl)
            # # print(outstr)
            # # time.sleep(1111)
        print("qsize="+str(urls.qsize())) 
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
        if len(outstr)>0:
            do_write(outfile,contenturl[0],outstr)
        
        # time.sleep(111110)    
  
if __name__ == '__main__':
		main()