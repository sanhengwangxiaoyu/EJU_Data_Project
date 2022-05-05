# -*- coding:utf-8 -*-
#URL的&vc=前面部分做增量 vc有效期短，每次爬都需要重写url列表
import queue,time,random,os
from lxml import etree

import re
import requests
import json
import xlrd
import traceback
from threading import Thread
from queue import Queue

urls = Queue()
okurl = set()

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/dongguan_new.txt"
outlistfile="./list/dongguan.txt"
okfile="./data/dongguan.txt"
url="http://dgfc.dg.gov.cn/dgwebsite_v2/Vendition/ProjectInfo.aspx?New=1"

arealist=['1','2','4','8','16','32','64','128','256','512','1024','2048','4096','8192','16384','32768','4294967296','65536','131072','262144','524288','1048576','2097152','4194304','8388608','16777216','33554432','67108864','134217728','268435456','536870912','1073741824','2147483648','8589934592','17179869184']
def trim(word):
    return re.sub('(\s*)','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')   
def get_data(contenturl):
    city='东莞'
    rslist=[]
    #
    try:
        #http://dgfc.dg.gov.cn/dgwebsite_v2/Vendition/BeianDetail.aspx?id=4292&vc=c4d8ef4b
        html=getHtml(contenturl)
        #http://dgfc.dg.gov.cn/dgwebsite_v2/Vendition/BeianView.aspx?id=6181&vc=05b12ad8
        u2='http://dgfc.dg.gov.cn/dgwebsite_v2/Vendition/'+''.join(re.findall('(BeianView\.aspx\?id=\w+&vc=\w+)',html))
        print(u2)
        h2=getHtml(u2)
        lxmls = etree.HTML(html)
        cell = {
                "城市": city,
                "项目名称":''.join(re.findall(r'项目名称：</b>(.*?)<',html,re.S)).strip(),
                "坐落位置":''.join(re.findall(r'楼盘坐落：.*?class="tal">(.*?)</td>',html,re.S)).strip(), 
                "开发企业": ''.join(re.findall(r'开发单位：</b>(.*?)<',html,re.S)),
                "预售许可证编号":''.join(re.findall(r'<span id="YsZheng">(.*?)</span>',h2,re.S)),
                "发证日期":'',
                "开盘日期":''.join(re.findall(r'开盘时间：.*?class="tal">(.*?)</td>',html,re.S)).strip(),
                "预售证准许销售面积":''.join(re.findall(r'id="Totalarea">(.*?)平方米',h2,re.S)).strip().replace('</span>',''),
                "销售状态": '',
                "销售楼号": '',
                "套数": '',
                "面积": '',
                "拟售价格": '',
                "售楼电话": ''.join(re.findall(r'售楼部电话：.*?class="tal">(.*?)</td>',html,re.S)).strip(),
                "售楼地址": ''.join(re.findall(r'售楼部地址：.*?class="tal">(.*?)</td>',html,re.S)).strip(), 
                "房号": '',
                "房屋建筑面积": '',
                "房屋销售状态": '',
        }
        print(cell)
        ######## 楼栋
        trs = lxmls.xpath('//table[@id="houseTable_1"]//tr[position()>1]')
        print('len楼栋 ='+str(len(trs)))
        
        for tr in trs:
            cell2=cell.copy()
            cell2['销售楼号']=''.join(tr.xpath('./td[2]//text()'))
            cell2['套数']=''.join(tr.xpath('./td[4]//text()'))
            cell2['面积']=''.join(tr.xpath('./td[6]//text()'))
            ##rooms
            url_rooms='http://dgfc.dg.gov.cn/dgwebsite_v2/Vendition/'+''.join(tr.xpath('./td[1]/a/@href'))
            rooms_html=getHtml(url_rooms)
            rooms_lxmls = etree.HTML(rooms_html)
            rooms_tds=rooms_lxmls.xpath("//table[@class='roomTable']/tr/td/table/tr/td")
            print(len(rooms_tds))
            for room in rooms_tds:
                #是否空位置
                roomid=''.join(room.xpath('.//text()')).strip()
                if len(roomid)==0:
                    continue
                #
                cell3=cell2.copy()
                cell3['房号']=roomid
                tmp=room.xpath('./@title')
                if len(tmp)==1:
                    tmp2=trim(tmp[0].replace('\n','。'))
                    m=re.findall(r'房屋状态：(.+?)(?:。|$)',tmp2)
                    if m :
                        cell3['房屋销售状态']=m[0]
                    m=re.findall(r'建筑面积：(.*?㎡)',tmp2)
                    if m :
                        cell3['房屋建筑面积']=m[0]
                    m=re.findall(r'单价：(.*?㎡)',tmp2)
                    if m :
                        cell3['拟售价格']=m[0]
                rslist.append(cell3)
            
        # print(rslist)
        # time.sleep(1222)
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
def postHtml(url,data):
    #使用try except方法进行各种异常处理
    header = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding':'gzip, deflate',
        'Content-Type':'application/x-www-form-urlencoded',
    }
    try:    
        res=requests.post(url=url,data=data,headers=header,verify=False)
        #解码
        return res.text
        
    except Exception as e:
        print(e)
    return ''
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
        		    res.encoding='GB2312'
        res.encoding='UTF-8'
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
                okurl.add(i.split('&vc=')[0])
                
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                okurl.add(i.split('&vc=')[0])
                
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile, 'r', encoding='utf-8') as f:
            for i in f:
                repeatlist.append(i.strip())
    #爬列表，vc有时效性，每轮重写，不需要翻页
    of = open(outlistfile,'w', encoding='utf-8') #保存结果文件
    html=getHtml(url)
    __VIEWSTATE=''.join(re.findall('name="__VIEWSTATE" id="__VIEWSTATE" value="(.*?)"',html,re.S))
    __EVENTVALIDATION=''.join(re.findall('name="__EVENTVALIDATION" id="__EVENTVALIDATION" value="(.*?)"',html,re.S))
    resultCount=''.join(re.findall('id="resultCount" value="(.*?)"',html,re.S))
    for town in arealist:
        data={
            '__VIEWSTATE':__VIEWSTATE,
            '__EVENTVALIDATION':__EVENTVALIDATION,
            'townName':town,
            'usage':'',
            'projectName':'',
            'projectSite':'',
            'developer':'',
            'area1':'',        
            'area2':'',        
            'resultCount':resultCount, 
            'pageIndex':'0', 
        }
        html=postHtml(url,data)
        lxmls = etree.HTML(html)
        hrefs =lxmls.xpath('//table[@class="resultTable1"]/tr/td[1]/a/@href') #lxmls.xpath('//span[@class="qxx1"]/a[2]/@href')
        print(len(hrefs))
        for href in hrefs:
            contenturl="http://dgfc.dg.gov.cn/dgwebsite_v2/Vendition/"+href
            if contenturl.split('&vc=')[0] in okurl:
                continue
            if contenturl in repeatlist:
                continue
            repeatlist.append(contenturl)
            of.write(contenturl+'\n')
            of.flush()
        __VIEWSTATE=''.join(re.findall('name="__VIEWSTATE" id="__VIEWSTATE" value="(.*?)"',html,re.S))
        __EVENTVALIDATION=''.join(re.findall('name="__EVENTVALIDATION" id="__EVENTVALIDATION" value="(.*?)"',html,re.S))
        resultCount=''.join(re.findall('id="resultCount" value="(.*?)"',html,re.S))
    #爬详细
    repeatlist=[]    
    with open(outlistfile, 'r', encoding='utf-8') as f:
         for i in f:
            i=i
            if i.split('&vc=')[0] in okurl:
                print('pass1')
                continue
            if i in repeatlist:
                print('pass2')
                continue
            repeatlist.append(i)
            urls.put(i.strip())
    #
    print('qsize=',urls.qsize())
    while urls.qsize()!=0:
        print('less----',urls.qsize())
        # contenturl='http://zzgj.haikou.gov.cn/xxgk/tdgl/tdzpggg/201911/t20191105_1461423.html' #
        contenturl=urls.get()
        outstr=get_data(contenturl)
        # print(outstr)
        do_write(outfile,contenturl,outstr)
        # time.sleep(10)    
        
           
if __name__ == '__main__':
		main()