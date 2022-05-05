# -*- coding:utf-8 -*-
#按预售证年搜索获取，按项目保存URL，项目爬虫中通过预售证+楼栋过滤
import queue,time,random
from lxml import etree
from threading import Thread
import re,os
import requests
import json
import traceback
from queue import Queue

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
urls = Queue()
okurl = set()
ysz_ld_oklist = set()

outlistfile='list/jiujiang_url.txt'
outfile='data/jiujiang_new.txt'
okfile='data/jiujiang.txt'

url = 'http://zjj-xx.jiujiang.gov.cn/RSalesQuery.php?psid=$1&bdesc='

def getHtml(link):
    html=''
    print(link)
    while True:
        try: #使用try except方法进行各种异常处理
            header = {
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0 Win64 x64 rv:86.0) Gecko/20100101 Firefox/86.0',
                'Accept': '*/*',
                }
            res = requests.get(link,headers=header,timeout=60,verify=False) #读取网页源码
            #解码
            res.encoding='GBK'
            html=res.text
            break
        except Exception as e:
            print(e)
            if str(e).find('Read timed out')>0:
                time.sleep(2)
    return html
    
def trim(word):
    return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')   
def run():
    while urls.qsize() != 0:
        print("qsize less="+str(urls.qsize()))   
        url1=urls.get()
        rslist=[]
        cell = {
                'url': url1,
                "城市": '',
                "项目名称": '',
                "坐落位置":  '',
                "开发企业": '',
                "预售许可证编号": '',
                "发证日期": '',
                "开盘日期": '',
                "预售证准许销售面积": '',
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
        text = getHtml(url1)
        time.sleep(0.1)
        table3 = etree.HTML(text)
        protab=table3.xpath('//table[@class="spf_table"]/tbody')
        cell['项目名称']=''.join(protab[0].xpath('./tr[1]/td[2]/text()')).strip()
        cell['开发企业']=''.join(protab[0].xpath('./tr[2]/td[2]/a/text()')).strip()
        cell['坐落位置']=''.join(protab[0].xpath('./tr[3]/td[2]/text()')).strip()
        cell['预售证准许销售面积']=''.join(protab[0].xpath('./tr[4]/td[4]/text()')).strip()
        cell['售楼电话']=''.join(re.findall('售楼部电话</td>.*?>(.*?)</td>',text,re.S)).strip()
        cell['售楼地址']=''.join(re.findall('售楼部地址</td>.*?>(.*?)</td>',text,re.S)).strip()
                
        table4 = table3.xpath('//table[@class="table table-hover"]//tr[position()>1]')
        print('table4=',len(table4))
        for ff in table4:
            try:
                ysz=''.join(ff.xpath(".//td[2]//text()"))
                lh=''.join(ff.xpath(".//td[1]//text()"))
                if ysz+lh in ysz_ld_oklist:
                    print('pass ysz ld',ysz,lh)
                    continue
                cell2=cell.copy()
                cell2["销售楼号"]=''.join(ff.xpath(".//td[1]//text()"))
                cell2["预售许可证编号"] = ''.join(ff.xpath(".//td[2]//text()"))
                cell2["面积"] = ''.join(ff.xpath(".//td[3]//text()"))
                cell2["发证日期"] = ''.join(ff.xpath(".//td[4]//text()"))
                cell2["拟售价格"] = ''.join(ff.xpath(".//td[5]//text()"))
                cell2["套数"] = ''.join(ff.xpath(".//td[6]//text()"))
                cell2["销售状态"] = ''.join(ff.xpath(".//td[8]//text()"))
                
                url3 = 'http://zjj-xx.jiujiang.gov.cn/' + ''.join(ff.xpath(".//td[1]//a//@href"))
                res11 =getHtml(url3)
                time.sleep(1)
                table5 = etree.HTML(res11)
                # table5 = etree.HTML(r3.content.decode('gbk'))
                table6 = table5.xpath('//table[@class="table table-hover table-borded"]//table[@class="table"]//tr//td')
                #更新套数，非住宅+住宅
                ts=0
                tmp=re.findall('住宅套数dM</td>.*?>(\d*)</td>',res11,re.S)
                if tmp and len(tmp[0])>0:
                    ts=int(tmp[0])
                tmp=re.findall('非住宅套数</td>.*?>(\d*)</td>',res11,re.S)
                if tmp and len(tmp[0])>0:
                    ts+=int(tmp[0])
                if ts>0:
                    cell['套数']=ts
                #户数
                print('table6=',len(table6),'ts=',ts)
                if len(table6)==0:
                    rslist.append(cell2)
                for tt in table6:
                    cell3=cell2.copy()
                    xq=tt.xpath('.//text()')
                    if(len(xq)>2):
                        color = ''.join(tt.xpath('.//div//@style'))
                        cell3["房屋销售状态"] = getstate(color)
                        cell3["房号"] =''.join(tt.xpath('.//text()')[1]).replace("\r\n","").strip()
                        cell3["房屋建筑面积"] =''.join(tt.xpath('.//text()')[2]).replace("建筑面积：","").strip()
                        if 'background' in getstate(color) :
                             cell3["房屋销售状态"] =''.join(tt.xpath('.//div//@title'))
                        rslist.append(cell3)
            except Exception as e:
                print(traceback.format_exc())
                print("异常")
                continue
        if len(rslist)==0:
            do_write(outfile,url1,[cell])
        else:
            do_write(outfile,url1,rslist)
            
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
def getstate(color):
    state = color
    if(color=='background:#0033FF'):
        state='已公示'
    elif(color=='background:#FFFFFF'):
        state = '不可售'
    elif (color == 'background:#00FF00'):
        state = '可售'
    elif (color == 'background:#0099FF'):
        state = '可现售'
    elif (color == 'background:#C4A93C'):
        state = '已销售上报'
    elif (color == 'background:#F3F394'):
        state = '已定'
    elif (color == 'background:#99CC00'):
        state = '已签,公租已签'
    elif (color == 'background:#CC99FF'):
        state = '公租备案(完全),备案,公租备案'
    elif (color == 'background:#FFCC99'):
        state = '在建工程抵押,工程最高额'
    elif (color == 'background:#AAAAAA'):
        state = '共管房'
    return state
def main():
    has_new-0
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a=i.split('\t')
                ysz_ld_oklist.add(a[5]+a[10])
                has_new=1
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a=i.split('\t')
                ysz_ld_oklist.add(a[5]+a[10])
    repeatlist=[]
    if os.path.exists(outlistfile):
        with open(outlistfile, 'r', encoding='utf-8') as f:
            for i in f:
                repeatlist.append(i.strip())
    #如果outfile没数据，需要创建并写一个表头
    if has_new==0:
        of = open(outfile,'a', encoding='utf-8') #保存结果文件
        of.write('URL\t城市\t项目名称\t坐落位置\t开发企业\t预售许可证编号\t发证日期\t开盘日期\t预售证准许销售面积\t销售状态\t销售楼号\t套数\t面积\t拟售价格\t售楼电话\t售楼地址\t房号\t房屋建筑面积\t房屋销售状态（颜色区分）\n')
        of.flush()
    #按预售证的年号搜索
    of = open(outlistfile,'a', encoding='utf-8') #保存结果文件，按url增量
    for year in range(2021,2000,-1):
        break
        text = getHtml(url.replace('$1',str(year)))
        time.sleep(0.1)
        # tabs = etree.HTML(text).xpath('//table[@class="table table-striped table-bordered table-hover table-condensed"]//tr[position()>1]')
        hrefs=re.findall('<tr>.*?loupan_loudong_detail\.php\?lpid=(.*?)&sid=.*?</tr>',text,re.S)
        print('year=',year,len(hrefs))
        if len(hrefs)==0:
            break
        for href in hrefs:
            href='http://zjj-xx.jiujiang.gov.cn/loupan_detail.php?lpid='+href
            if href in repeatlist :
                continue
            repeatlist.append(href)
            of.write(href+'\n')
            of.flush()
        break #中断，目前增量只需要爬2021年的
        
    #2多线程爬项目
    with open(outlistfile,'r', encoding='utf-8') as f:
        for i in f:
            urls.put(i.strip())
            # run()
            # time.sleep(22222)
    print("qsize="+str(urls.qsize()))           
    time.sleep(1)
    ths = []
    for i in range(1):
        t = Thread(target=run, args=())
        t.start()
        ths.append(t)
    for t in ths:
        t.join()
        # time.sleep(1111)
if __name__ == '__main__':
	main()