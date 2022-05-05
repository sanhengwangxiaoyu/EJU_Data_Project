# -*- coding:utf-8 -*-
#许可证在项目中才能爬，需要在项目中对比预售证列表再增量爬

import time,random
from lxml import etree

import re,os
import requests
import json
import traceback

from threading import Thread
from queue import Queue

urls = Queue()
ysz_oklist=set()

outfile="./data/zhuhai_new.txt"
outlistfile="./list/zhuhai.txt"
okfile="./data/zhuhai.txt"

url='http://113.106.103.37/presalelist?keywords=presale&tabkey=all&searchcode=&start=$1&count=10'

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
    city='珠海'
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
    try:
        #pro
        h1=getHtml(contenturl)
        ysz_idlist=re.findall('<option value="(.*?)" >(.*?)（发证日期：<strong>(.*?)）</option>',h1,re.S)
        print('ysz num='+str(len(ysz_idlist)))
        ifexists=0
        for yszid,ysz,fzrq in ysz_idlist:
            if ysz in ysz_oklist:
                print('pass ysz=',ysz)
                continue
            h2=getHtml(f'{contenturl}&presalepermitid={yszid}')
            tab2 = etree.HTML(h2)
            tab21=tab2.xpath('//div[@class="building-info-table"]/table')[0]
            cell = {
                "城市": city,
                "项目名称": trim(''.join(re.findall('style="font-size:22px;">(.*?)</strong>',h2,re.S))),
                "坐落位置": trim(''.join(tab21.xpath('./tr[1]/td[2]/text()'))),
                "开发企业": trim(''.join(tab21.xpath('./tr[2]/td[2]/text()')[0])),
                "预售许可证编号": ysz,
                "发证日期": fzrq,
                "开盘日期": '',
                "预售证准许销售面积":trim(''.join(tab21.xpath('./tr[3]/td[2]/text()')[0])),
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
            ##lpid
            lpid_list=tab2.xpath('//select[@id="buildnum"]/option')
            print('lp num='+str(len(lpid_list)))
            for lp in lpid_list:
                lpname=lp.xpath('string(.)')
                lpid=''.join(lp.xpath('./@value'))
                h3=getHtml(f'http://113.106.103.37/getFloorList?housebuildingid={lpid}')
                js=json.loads(h3)
                for dy in js:
                    if dy=='无单元':
                        u4=f'http://113.106.103.37/presalehouseview?housebuildingid={lpid}&unitnumber={dy}'
                    else:
                        u4=f'http://113.106.103.37/presalehouseview?housebuildingid={lpid}&unitnumber={dy}'
                    h4=getHtml(u4)
                    tab4 = etree.HTML(h4)
                    ts=''.join(re.findall('strong>(.*?)</strong>.*?<p>房源总数</p>',h4,re.S))
                    rooms=tab4.xpath('//a[@class="operateBtn"]')
                    if len(rooms)==0:
                        cell5=cell.copy()
                        cell5['销售楼号']=lpname
                        cell5['房号']=dy
                        cell5['套数']=ts
                        rslist.append(cell5)
                    print(f'lpname={lpname},dy={dy},ts={ts}')
                    for room in rooms:
                        fh=dy+'-'+trim(''.join(room.xpath('./div[1]/text()')))
                        state=trim(''.join(room.xpath('./p[1]/text()')))
                        price=trim(''.join(room.xpath('./p[2]/text()')))
                        if state!='不可售':
                            u5=''.join(room.xpath('./@data_id'))
                            h5=getHtml(f'http://113.106.103.37/salehouseview?houseid={u5}')
                            mj=trim(''.join(re.findall('建筑面积：</td>.*?<td width="35%">(.*?)<font',h5,re.S)))+'㎡'
                        else:
                            mj=''
                        cell5=cell.copy()
                        cell5['销售楼号']=lpname
                        cell5['套数']=ts
                        cell5['房屋建筑面积']=mj
                        cell5['房号']=fh
                        cell5['拟售价格']=price
                        cell5['房屋销售状态']=state
                        rslist.append(cell5)
                        ifexists=1
            if ifexists==0:
                rslist.append(cell)
    except Exception as e:
        print(traceback.format_exc()) 
    
    # print(rslist)
    # time.sleep(11111)
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
        of.write(dicts.get('发证日期','')+'\t')
        of.write(dicts.get('开盘日期','')+'\t')
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
        html=res.text
    except Exception as e:
        print(e)
    finally:
        return html
def main():
    #
    has_new=0
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')                    
                ysz_oklist.add(a[5])
                has_new=1
                
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                ysz_oklist.add(a[5])
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile, 'r', encoding='utf-8') as f:
            for i in f:
                repeatlist.append(i.strip())
    #如果outfile没数据，需要创建并写一个表头
    if has_new==0:
        of = open(outfile,'a', encoding='utf-8') #保存结果文件
        of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）')
        of.flush()
    ##翻页
    of = open(outlistfile,'a', encoding='utf-8') #保存结果文件，增量添加
    total=0
    pn=0
    while True :
        pn+=1
        html=getHtml(url.replace('$1',str(pn)))
        tab1 = etree.HTML(html)
        hrefs=tab1.xpath('//div[@class="house-info left"]/a/@href')
        #########
        if len(hrefs)==0:
            break
        if total==0:
            total=int(re.findall("writePagenumButton\(\d+,'(\d+)'",html)[0])        
        print('total,page,len=',total,pn,len(hrefs))
        for j in hrefs:
            contenturl='http://113.106.103.37'+j
            if contenturl in repeatlist:
                continue
            repeatlist.append(contenturl)
            of.write(contenturl+'\n')
            of.flush() 
        if pn>=total:
            break
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile,'r',encoding='UTF-8') as txtData: 
            for line in txtData.readlines():
                contenturl=line.strip()
                if contenturl in repeatlist:
                    continue
                repeatlist.append(contenturl)
                urls.put(contenturl)
                
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
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
        outstr=get_data(contenturl)
        do_write(outfile,contenturl,outstr)
        # time.sleep(10)    
if __name__ == '__main__':
		main()