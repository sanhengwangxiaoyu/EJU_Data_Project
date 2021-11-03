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

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/haerbin_new.txt"
outlistfile="./list/haerbin.txt"
okfile="./data/haerbin.txt"

url='http://www.hrbonline365.com/gxdzjzb/front/project/queryProjectList?pageNo=$1&pageSize=10&presaleNo=&propertyName=&developmentName=&districtId=&projectType=&t=1622022599384'

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')

def get_data(contenturl):
    city='������'
    area=''
    rslist=[]
    ceil={}
    detail=''
    newhtml=''
    #contenturl+'\t'+pro+'\t'+addr+'\t'+sp+'\t'+ysz+'\t'+fz_dt+'\t'+ysmj+'\n')
    try:
        #pro
        proid=re.findall('projectId=(.*?)&',contenturl[0])[0]
        ceil = {
                "����": city,
                "��Ŀ����": contenturl[1],
                "����λ��": contenturl[2],
                "������ҵ": contenturl[3],
                "Ԥ�����֤���":contenturl[4],
                "��֤����":contenturl[5],
                "��������":'',
                "Ԥ��֤׼���������":contenturl[6].strip(),
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
        h1=getHtml('http://www.hrbonline365.com/gxdzjzb/front/project/queryProjectBuildingList?pageNo=1&pageSize=10&projectId='+proid+'&t=1622080068698','')
        m=re.findall('pages":(\d+)',h1,re.S)
        if len(m)==1:
            pages=int(m[0])+1
            print('pages=',pages)
            if pages==1:
                rslist.append(ceil)
                return rslist
            ###
            
            while True:
                print(h1)
                js=json.loads(h1)['result']['records']
                
                for j in js:
                    ceil2=ceil.copy()
                    ceil2['����¥��']=j['buildingName']
                    ceil2['����']=j['allowHouseholdsAmount']
                    ceil2['���']=j['allowAreaAmount']
                    rslist.append(ceil2)
                #
                pages=pages-1
                if pages<=1:
                    break
                h1=getHtml('http://www.hrbonline365.com/gxdzjzb/front/project/queryProjectBuildingList?pageNo='+str(pages)+'&pageSize=10&projectId='+proid+'&t=1622080068698','')
        else:
            rslist.append(ceil)
            
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
        of.write(trim(dicts.get('��Ŀ����',''))+'\t')
        of.write(trim(dicts.get('����λ��',''))+'\t')
        of.write(trim(dicts.get('������ҵ',''))+'\t')
        of.write(trim(dicts.get('Ԥ�����֤���',''))+'\t')
        of.write(trim(dicts.get('��֤����',''))+'\t')
        of.write(trim(dicts.get('��������',''))+'\t')
        of.write(trim(dicts.get('Ԥ��֤׼���������',''))+'\t')
        of.write(trim(dicts.get('����״̬',''))+'\t')
        of.write(trim(dicts.get('����¥��',''))+'\t')
        of.write(trim(dicts.get('����',''))+'\t')
        of.write(trim(dicts.get('���',''))+'\t')
        of.write(trim(dicts.get('���ۼ۸�',''))+'\t')
        of.write(trim(dicts.get('��¥�绰',''))+'\t')
        of.write(trim(dicts.get('��¥��ַ',''))+'\t')
        of.write(trim(dicts.get('����',''))+'\t')
        of.write(trim(dicts.get('���ݽ������',''))+'\t')
        of.write(trim(dicts.get('��������״̬','')))
        of.write("\n")
        of.flush()
def getHtml(link,refer):
    html=""
    try: #ʹ��try except�������и����쳣����
        header = {
            'User-Agent':'%E6%88%BF%E7%AE%A1%E5%B1%80/1 CFNetwork/978.0.7 Darwin/18.7.0',
            'Content-Type':'application/x-www-form-urlencoded',
            'Accept': '*/*',
            'Referer': refer
            }
        res = requests.get(link,headers=header,timeout=20,verify=False) #��ȡ��ҳԴ��
        #����
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
def postHtml(url,data):
    #ʹ��try except�������и����쳣����
    header = {
            'User-Agent':'%E6%88%BF%E7%AE%A1%E5%B1%80/1 CFNetwork/978.0.7 Darwin/18.7.0',
            'Content-Type':'application/x-www-form-urlencoded',
            'Cookie':'JSESSIONID=2420324B8617B7EACA7D596EE868CF02; JSESSIONID=5D1E7B5718E5C996ABC768E0C9A842AC',
            'Accept': '*/*'
            }
    #data=json.loads(js)
    try:
        res=requests.post(url=url,data=data,headers=header,verify=False)
        res.encoding='UTF-8'
        return res.text
    except Exception as e:
        print(traceback.format_exc())
    return ''
def main():
    oklist=openList(okfile)
    outlist=openList(outfile)
    newlist=[]
    of = open(outlistfile,'a', encoding='utf-8') #�������ļ�
    
    #ysz http://www.hrbonline365.com/gxdzjzb/front/project/queryProjectById?projectId=1010004003&t=1622023728436
    #ld http://www.hrbonline365.com/gxdzjzb/front/project/queryProjectBuildingList?pageNo=1&pageSize=10&projectId=48960&t=1622080068698
    #room http://www.hrbonline365.com/gxdzjzb/front/project/queryPinControlChart?buildingId=1011003912&keyCode=402745e38ef8479488372445f6260b6d&projectId=1010004003&t=1622023734401
    #
    districts=[15800106,15800105,15800104,15800103,15800102,15800101,15800100]
    for d in districts:
        pn=0
        while True :
            pn+=1
            print('page=',pn) 
            thisurl='http://www.hrbonline365.com/gxdzjzb/front/project/queryProjectList?pageNo='+str(pn)+'&pageSize=10&presaleNo=&propertyName=&developmentName=&districtId='+str(d)+'&projectType=&t=1622022599384'
            html=getHtml(thisurl,'')
            pages=int(re.findall('pages":(\d+)',html,re.S)[0])
            js=json.loads(html)['result']['records']
            print(len(js),pages)
            if len(js)==0:
                break
            for j in js:
                proid=j['projectId']
                contenturl='http://www.hrbonline365.com/#/Home/commodityDetails?projectId='+proid+'&number='
                pro=j['projectName']
                fz_dt=j['isuedocTime']
                ysmj=j['area']
                sp=j['developmentName']
                addr=j['specificLocation']
                ysz=j['presaleNo']
                if fz_dt==None:
                    fz_dt=''
                if ysmj==None:
                    ysmj=''
                if contenturl in newlist:
                    continue
                if contenturl not in outlist:
                    newlist.append(contenturl)
                    of.write(contenturl+'\t'+pro+'\t'+addr+'\t'+sp+'\t'+ysz+'\t'+fz_dt+'\t'+ysmj+'\n')
                    of.flush() 
            if pages<=pn:
                break
    
    
    with open(outlistfile,'r',encoding='UTF-8') as txtData: 
        for line in txtData.readlines():
            contenturl=line.split('\t')
            if contenturl[0] in oklist:
                continue
            urls.put(contenturl)
            # outstr=get_data(contenturl)
            # do_write(outfile,contenturl,outstr)
            # time.sleep(1111)
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