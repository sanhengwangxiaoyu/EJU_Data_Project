# -*- coding:utf-8 -*-  
# 按URL增量，为预售证。但爬取详情页面时，地址需要判断预售证ID一致
import time,random
from lxml import etree

import re,os
import requests
import json
import traceback
import urllib.parse

from threading import Thread
from queue import Queue

urls = Queue()
okurl = set()
dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/shijiazhuang_new.txt"
outlistfile="./list/shijiazhuang.txt"
okfile="./data/shijiazhuang.txt"

url='http://zjj.sjz.gov.cn:8081/plus/scxx_ysxk.php?pageno=$1&'

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','').replace('&nbsp;','')

def get_data(contenturl):
    city='石家庄'
    area=''
    rslist=[]
    ceil={}
    #contenturl+'\t'+pro+'\t'+addr+'\t'+sp+'\t'+ysz+'\t'+kprq+'\n'
    print(contenturl)
    try:
        #pro
        proid=re.findall('\?id=(.*?)&',contenturl[0])[0]
        yszid=re.findall('nodeid=(.*?)$',contenturl[0])[0]
        h1=getHtml(f'http://zjj.sjz.gov.cn:8081/plus/scxx_presale_show.php?projid={proid}&id={yszid}')
        tel=''.join(re.findall('预售楼电话.*?241px">(.*?)<',h1,re.S)).strip()
        sale_addr=''.join(re.findall('销售地址.*?241px">(.*?)<',h1,re.S)).strip()
        ysmj=''.join(re.findall('ID="YSBFZJZMJ" runat="server">(.*?)<',h1,re.S)).strip()
        ceil = {
            "城市": city,
            "项目名称": contenturl[1],
            "坐落位置":  contenturl[2],
            "开发企业": contenturl[3],
            "预售许可证编号": contenturl[4],
            "发证日期": '',
            "开盘日期": contenturl[5].strip(),
            "预售证准许销售面积":ysmj,
            "销售状态": '',
            "销售楼号": '',
            "套数": '',
            "面积": '',
            "拟售价格": '',
            "售楼电话": tel,
            "售楼地址": sale_addr,
            "房号": '',
            "房屋建筑面积": '',
            "房屋销售状态": '',
        }
        ##lp & danyuan
        u2=getHtml('http://zjj.sjz.gov.cn:8081/plus/cxda_ys_json_menu.php?id='+proid)
        js=json.loads(u2)
        if len(js)==0:
            return [ceil]
        
        for j in js:
            #ysz check
            id=j['id']
            #非此预售证过滤
            if id!=yszid:
                continue
            #ld
            if j.get('children')==None:
                continue
            children=j['children']
            if len(children)==0:
                continue
            for ld in children:
                #http://zjj.sjz.gov.cn:8081/plus/scxx_floor_show.php?projid=50055&id=0040150017007
                u3=getHtml('http://zjj.sjz.gov.cn:8081/plus/'+ld['attributes']['url'])
                ldmc=ld['text']
                mj=''.join(re.findall('>建筑面积</td>.*?>(.*?)<',u3,re.S)).strip()
                ts=''.join(re.findall('>总套数</td>.*?>(.*?)<',u3,re.S)).strip()
                price=''.join(re.findall('>均价</td>.*?>(.*?)<',u3,re.S)).strip()
                #dy
                children2=ld['children']
                if len(children2)==0:
                    ceil5=ceil.copy()
                    ceil5['销售楼号']=ldmc
                    ceil5['套数']=ts
                    ceil5['面积']=mj
                    ceil5['拟售价格']=price
                    rslist.append(ceil5)
                    continue
                for dy in children2:
                    # print(dy)
                    dymc=dy['text']
                    url4='http://zjj.sjz.gov.cn:8081/plus/'+dy['attributes']['url'].replace('scxx_subroom_show','scxx_subroom_showX')+'&numvar=0.09123960499198147'
                    u4=getHtml(url4)
                    # print(url4)
                    u4=urllib.parse.unquote(u4).replace('+','')
                    #
                    rooms=re.findall('<tdalign="center"><imgsrc="/images/(.*?).gif"alt="房屋状态"/><a></a><aonclick=\'tourl\(\)\'href="(.*?)"class="w">(.*?)</a></td>',u4,re.S)
                    if len(rooms)==0:
                        continue
                    for room in rooms:
                        ceil5=ceil.copy()
                        state=getState(room[0])
                        url5='http://zjj.sjz.gov.cn:8081/plus/'+room[1]
                        # u5=getHtml(url5)
                        time.sleep(0.1)
                        # print(u5)
                        jzmj=''
                        ceil5['销售楼号']=ldmc
                        ceil5['面积']=mj
                        ceil5['套数']=ts
                        ceil5['拟售价格']=price
                        ceil5['房号']=dymc+'-'+room[2]
                        ceil5['房屋建筑面积']=url5#''.join(re.findall('id="AREA_ROTAL">(.*?)</span>',u5,re.S)).strip()
                        ceil5['房屋销售状态']=state
                        rslist.append(ceil5)
                        # print(ceil5)
                        # time.sleep(1112)
         
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
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
            'Cookie':'Hm_lvt_11bf8a9319c593325ac1a60cce653bca=1626250846; Hm_lpvt_11bf8a9319c593325ac1a60cce653bca=1626250846',
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
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
def getState(word):
    if word=='Green':
        return '可售'
    elif word=='yel':
        return '已售'
    else:
        return '限制'
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
        of = open(outfile,'a', encoding='utf-8') #
        of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
        of.flush()
        
    pn=0
    of = open(outlistfile,'a+', encoding='utf-8') #保存列表文件
    total=0
    while True :
        break
        pn+=1
        time.sleep(5)
        html=getHtml(url.replace('$1',str(pn)))
        html=html.replace('<table width="100%" border="0" cellspacing="1" cellpadding="0" style="margin-bottom:15px;">','</table><table width="100%" border="0" cellspacing="1" cellpadding="0" style="margin-bottom:15px;">')
        if total==0:
            total=int(re.findall('符合您的查询条件的<span>(\d+)</span>条记录</h2>',html,re.S)[0])
        tab1 = etree.HTML(html)
        hrefs=tab1.xpath('//div[@class="scxx_jieguo"]/table')
        #########
        print('page=',pn,len(hrefs))
        if len(hrefs)==0:
            break   
        for j in hrefs:
            sp=''.join(j.xpath('./tr[1]/td[1]/text()')).strip()
            ysz=''.join(j.xpath('./tr[1]/td[2]/text()')).strip().replace('&nbsp;','')
            addr=''.join(j.xpath('./tr[2]/td[1]/text()')).strip()
            pro=''.join(j.xpath('./tr[3]/td[1]/text()')).strip()
            kprq=''.join(j.xpath('./tr[3]/td[2]/text()')).strip()
            u=''.join(j.xpath('./tr[1]/td[2]/a/@href'))
            if len(u)<3:
                continue
            contenturl='http://zjj.sjz.gov.cn:8081'+u
            if contenturl in repeatlist or contenturl in okurl:
                continue
            repeatlist.append(contenturl)
            print(pro,addr,sp,ysz,kprq)
            of.write(contenturl+'\t'+pro+'\t'+addr+'\t'+sp+'\t'+ysz+'\t'+kprq+'\n')
            of.flush()
        if total<=7*pn:
            break
        
    if os.path.exists(outlistfile):
        with open(outlistfile,'r',encoding='UTF-8') as txtData: 
            for line in txtData.readlines():
                contenturl=line.split('\t')
                if contenturl[0] in okurl:
                    continue
                urls.put(contenturl)
                okurl.add(contenturl[0])
                # outstr=get_data(contenturl)
                # time.sleep(52222)
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
        if 'http://zjj.sjz.gov.cn:8081'==contenturl[0]:
            print(contenturl)
            continue
        outstr=get_data(contenturl)
        do_write(outfile,contenturl[0],outstr)
        time.sleep(1)    
          
if __name__ == '__main__':
		main()