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
ysz_oklist=set()

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/nanning_new.txt"
outlistfile="./list/nanning.txt"
okfile="./data/nanning.txt"

url='http://zjj.nanning.gov.cn/commonCenter.do?pageNum=$1&YSZH=&XMZL=&GSMC=&XMMC=&serviceid=zfj_document&methodname=queryNumberListNowZJJ'

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')
def postHtml(url,data):
    #ʹ��try except�������и����쳣����
    header = {
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
        'Content-Type':'application/json',
        'Origin':'http://221.182.157.163:8848',
        'Referer':url,
        'host':'221.182.157.163:9002',
        'Cache-Control':'no-cache'
    }
    #data=json.loads(js)
    try:
        res=requests.post(url=url,data=data,headers=header,verify=False)
        # #����js����
        # print(res.call('__doPostBack','AspNetPager1',2))
        res.encoding='UTF-8'
        return res.text
        
    except Exception as e:
        print(traceback.format_exc())
    return ''
def get_data(contenturl):
    city='����'
    area=''
    rslist=[]
    ceil={}
    detail=''
    newhtml=''
    progect=contenturl[1]
    company=contenturl[2]
    addr=contenturl[4]
    ysz=contenturl[3]
    fzrq=contenturl[5]
    area=contenturl[6].strip()
    yszh=urllib.parse.quote((urllib.parse.quote(ysz)))
    try:
        #pro
        ceil = {
                "����": city,
                "��Ŀ����": progect,
                "����λ��":addr,
                "������ҵ": company,
                "Ԥ�����֤���":ysz,
                "��֤����":fzrq,
                "��������":'',
                "Ԥ��֤׼���������":area,
                "����״̬": '',
                "����¥��": '',
                "����": '',
                "���": '',
                "���ۼ۸�": '',
                "��¥�绰": '',
                "��¥��ַ": '',
                "����": '',
                "���ݽ������": '',
                "��������״̬": '',
        }
        ###
        u1='http://zjj.nanning.gov.cn/commonCenter.do?YSZH='+yszh+'&serviceid=zfj_document&methodname=queryDHZJJ'
        h1=getHtml(u1,'http://zjj.nanning.gov.cn/web/zxxh1.html')
        js1=json.loads(h1)['result']
        print('longdongs=',len(js1))
        if len(js1)==0:
            rslist.append(ceil)
            return rslist
        for j in js1:
            ceil2=ceil.copy()
            dh=j.get('DH','')
            ceil2['����¥��']=dh
            ceil2['���']=j.get('MJ','')
            ceil2['����']=j.get('TS','')
            ###
            dh=urllib.parse.quote(urllib.parse.quote(dh))
            u2=f'http://zjj.nanning.gov.cn/commonCenter.do?YSZH={yszh}&DH={dh}&serviceid=zfj_document&methodname=queryHouseImageZJJ'
            h2=getHtml(u2,'http://zjj.nanning.gov.cn/web/lpb.html')
            js2=json.loads(h2)['result']
            print('rooms=',len(js2))
            if len(js2)==0:
                rslist.append(ceil2)
                continue
            for room in js2:
                ceil3=ceil2.copy()
                ceil3['����']=room.get('FH','')
                ceil3['���ݽ������']=room.get('JZMJ','')
                ba=room.get('SFYBA','')
                if ba=='-1':
                    ba='�ѱ���'
                elif ba=='0':
                    ba='δ����'
                ceil3['��������״̬']=ba
                rslist.append(ceil3)
    except Exception as e:
        print(traceback.format_exc()) 
    
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
    of = open(outfile,'a+', encoding='utf-8') #�������ļ�
    for dicts in rsdict:
        of.write(url+"\t")
        of.write(dicts.get('����','')+'\t')
        of.write(dicts.get('��Ŀ����','')+'\t')
        of.write(dicts.get('����λ��','')+'\t')
        of.write(dicts.get('������ҵ','')+'\t')
        of.write(dicts.get('Ԥ�����֤���','')+'\t')
        of.write(dicts.get('��֤����','')+'\t')
        of.write(dicts.get('��������','')+'\t')
        of.write(dicts.get('Ԥ��֤׼���������','')+'\t')
        of.write(dicts.get('����״̬','')+'\t')
        of.write(dicts.get('����¥��','')+'\t')
        of.write(dicts.get('����','')+'\t')
        of.write(dicts.get('���','')+'\t')
        of.write(dicts.get('���ۼ۸�','')+'\t')
        of.write(dicts.get('��¥�绰','')+'\t')
        of.write(dicts.get('��¥��ַ','')+'\t')
        of.write(dicts.get('����','')+'\t')
        of.write(dicts.get('���ݽ������','')+'\t')
        of.write(dicts.get('��������״̬',''))
        of.write("\n")
        of.flush()
def getHtml(link,refer):
    html=""
    try: #ʹ��try except�������и����쳣����
        header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
            'Cookie':'Hm_lvt_b24ff3dd29c6727b010f2da3b8a746e8=1617843746,1618973782; ASP.NET_SessionId=qevt0ca2an0h1ktv0ihtf0uk; Hm_lpvt_b24ff3dd29c6727b010f2da3b8a746e8=1618983378',
            'Host':'zjj.nanning.gov.cn',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
            'Referer': refer
            }
        res = requests.get(link,headers=header,timeout=20,verify=False) #��ȡ��ҳԴ��
        #����
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
    ##����URL�б�
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                ysz_oklist.add(a[5])
    if os.path.exists(okfile):
        with open(okfile, 'r', encoding='utf-8') as f:
            for i in f:
                a = i.split('\t')
                ysz_oklist.add(a[5])
    #
    repeatlist=openList(outlistfile)
    pn=0
    end='no'
    of = open(outlistfile,'a+', encoding='utf-8') #�������ļ�
    while True:
        #�жϷ�ҳ
        if end=='yes':
            break
        pn+=1
        html=getHtml(url.replace('$1',str(pn)),'http://zjj.nanning.gov.cn/web/xsxx.html')
        js=json.loads(html)
        total=js['totalPage']
        #########
        hrefs=js['result']
        # print(hrefs)
        if len(hrefs)==0:
            break   
        for j in hrefs:
            ysz=j['YSZH']
            # u=urllib.parse.quote((urllib.parse.quote(ysz)))
            contenturl='http://zjj.nanning.gov.cn/commonCenter.do?postData='+j['YSZH']+'&serviceid=zfj_document&methodname=queryDetaileZJJ'
            sp=j['GSMC']
            fzsj=j['PZYSSJ']
            pro=j['XMMC']
            addr=j['XMZL']
            ysmj=j['YSMJ']
            # contenturl=j#http://zjj.nanning.gov.cn/commonCenter.do?postData=%25E5%258D%2597%25E6%2588%25BF%25E5%2595%2586%25E5%25A4%2587%25E5%25AD%2597%25E7%25AC%25AC20210428330%25E5%258F%25B7&serviceid=zfj_document&methodname=queryDetaileZJJ  
            # time.sleep(1222)
            #���÷�ҳ�жϵ�2021-03
            if fzsj.find('2021-03')>-1:
                end='yes'
                break
            if ysz in ysz_oklist:
                continue
            if contenturl in repeatlist:
                continue
            repeatlist.append(contenturl)
            of.write(contenturl+'\t'+pro+'\t'+sp+'\t'+ysz+'\t'+addr+'\t'+fzsj+'\t'+ysmj+'\n')
            of.flush()
    ######
    repeatlist=[]
    with open(outlistfile,'r',encoding='UTF-8') as txtData: 
        for line in txtData.readlines():
            contenturl=line.split('\t')
            if contenturl[3] in ysz_oklist or contenturl[0] in repeatlist:
                continue
            repeatlist.append(contenturl[0])
            urls.put(contenturl)
            # outstr=get_data(contenturl)
            # do_write(outfile,contenturl[0],outstr)
            # time.sleep(2222)
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
            # time.sleep(10)    

def savefile(datalist,filename):
    old_datalist=[]
    try:
        with open(filename,'r') as txtData: 
            for line in txtData.readlines():
                old_datalist.append(line.strip())
    except Exception as e:
        print(traceback.format_exc())
    of = open(filename,'a+', encoding='utf-8') #�����ļ�
    for data in datalist:
        if datalist not in old_datalist:
            of.write(data+"\n")
            of.flush()              
if __name__ == '__main__':
		main()