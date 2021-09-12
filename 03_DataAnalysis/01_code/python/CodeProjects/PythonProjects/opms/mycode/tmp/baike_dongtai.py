# -*- coding:utf-8 -*-
#爬取动态列表，有的可能没有

import requests
import re,time
from lxml import etree
from queue import Queue
import traceback,os,random

urls = Queue()

ok_dongtai = set()
inlist='bkurl.txt'
outfile='bk_dongtai_new.txt'
okfile='bk_dongtai.txt'

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')
def do_write(outfile,url,rsdict):
    of = open(outfile,'a+', encoding='utf-8') #保存结果文件
    for dicts in rsdict:
        of.write(url+"\t")
        of.write(dicts.get('城市','')+'\t')
        of.write(trim(dicts.get('tag',''))+'\t')
        of.write(trim(dicts.get('title',''))+'\t')
        of.write(trim(dicts.get('date',''))+'\t')
        of.write(trim(dicts.get('content',''))+'\n')
        of.flush()
def getHtml(link):
    html=""
    # print(link)
    try: #使用try except方法进行各种异常处理
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0'+str(random.random()),
        } 
        res = requests.get(link,headers=header,timeout=30,verify=False) #读取网页源码
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
        print('html err')
    finally:
        return html
def main():
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                cell={}
                cell['tag']=a[2]
                cell['title']=a[3]
                cell['date']=a[4]
                cell['content']=a[5].strip()
                ok_dongtai.add(str(cell))
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')                    
                cell={}
                cell['tag']=a[2]
                cell['title']=a[3]
                cell['date']=a[4]
                cell['content']=a[5].strip()
                ok_dongtai.add(str(cell))
    if os.path.exists(inlist):
        with open(inlist, 'r', encoding='utf-8') as f:
            for i in f:
                run(i.strip())
                
def run(url):
    print('url=',url)
    pn=0
    rslist=[]
    end='no'
    while True and end=='no':
        time.sleep(4)#有限爬，访问间隔时间可能需要调整
        pn+=1
        num=0
        text=getHtml(f'{url}dongtai/pg{pn}')
        tab=etree.HTML(text).xpath('//div[@class="dongtai-one for-dtpic"]')
        print('tabs=',len(tab))
        if len(tab)==0:
            print('no data')
            break
        for tr in tab:
            cell={}
            num+=1
            print('page=',pn,'num=',num)
            cell['tag']=trim(''.join(tr.xpath('./a/span[1]/text()')))
            cell['title']=trim(''.join(tr.xpath('./a/span[1]/text()')))
            cell['date']=trim(''.join(tr.xpath('./a/span[3]/text()')))
            cell['content']=trim(''.join(tr.xpath('.//div[@class="a-word"]/.//text()')))
            if str(cell) in ok_dongtai:
                end='yes'
                break
            rslist.append(cell)
    do_write(outfile,url,rslist)
            
if __name__ == '__main__':
		main()