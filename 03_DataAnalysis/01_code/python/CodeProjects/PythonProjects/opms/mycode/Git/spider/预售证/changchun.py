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
outfile="./data/changchun_new.txt"
outlistfile="./list/changchun.txt"
okfile="./data/changchun.txt"

url='http://www.ccfdw.net/ecdomain/ccfcjwz/wsba/ysxk/search.jsp?xkzh=&xkz_num=&xmmc=&sfdw=&flag=1&nowPage=$1'

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','')

def get_data(contenturl):
    city='����'
    area=''
    rslist=[]
    ceil={}
    detail=''
    newhtml=''
    try:
        #pro
        h1=getHtml('http://'+contenturl,'')
        saleid=''.join(re.findall("opczfy\('(.*?)'",h1,re.S))
        lhlist=''.join(re.findall('Ԥ�۷�Χ</td>.*?>(.*?)<',h1,re.S)).strip()
        if saleid!=None:
            h2=getHtml('http://www.ccfdw.net/ecdomain/lpcs/xmxx/xmjbxxinfo.jsp?Id_xmxq='+saleid,'')
        else:
            h2=''
        
        ceil = {
                "����": city,
                "��Ŀ����": ''.join(re.findall('onclick="opczfy.*?>(.*?)<',h1,re.S)).strip(),
                "����λ��": ''.join(re.findall('��Ŀ��ַ</td>.*?>(.*?)<',h1,re.S)).strip(),
                "������ҵ": ''.join(re.findall('������ҵ����</td>.*?>(.*?)<',h1,re.S)).strip(),
                "Ԥ�����֤���":''.join(re.findall('Ԥ�����֤���</td>.*?>(.*?)<',h1,re.S)).strip(),
                "��֤����":''.join(re.findall('��֤����</td>.*?>(.*?)<',h1,re.S)).strip(),
                "��������":''.join(re.findall('�������ڣ� </td>.*?>(.*?)<',h2,re.S)).strip(),
                "Ԥ��֤׼���������":''.join(re.findall('Ԥ���ܽ������</td>.*?>(.*?)<',h1,re.S)).strip(),
                "����״̬": '',
                "����¥��": '',
                "����": '',
                "���": '',
                "���ۼ۸�": ''.join(re.findall('�ο��ۣ�<.*?>(.*?)</td>',h2,re.S)).replace('</font>','').strip(),
                "��¥�绰": re.sub('<.*?>','',''.join(re.findall("padding-left:90px;font-family:'ArialBlack';\">(.*?)</td>",h2,re.S))).strip(),
                "��¥��ַ": ''.join(re.findall('��¥��ַ�� </td>.*?>(.*?)<',h2,re.S)).strip(),
                "����": '',
                "���ݽ������": '',
                "��������״̬": '',
        }
        
        ###lou  list 
        h2=getHtml('http://www.ccfdw.net/ecdomain/lpcs/xmxx/loulist.jsp?Id_xmxq='+saleid,'')
        ldid_list=re.findall("searchByLid\('(.*?)'",h2,re.S)
        if len(ldid_list)==0:
            rslist.append(ceil)
        ##
        for ldid in ldid_list:
            ceil2=ceil.copy()
            h3=getHtml('http://www.ccfdw.net/ecdomain/lpcs/xmxx/lpbxx_new.jsp?lid='+ldid,'')
            tab3 = etree.HTML(h3)
            ysz=''.join(tab3.xpath('//div[@id="content1"]/table/tr[1]/td[2]//text()')).strip()
            print(ysz)
            if ysz!=ceil['Ԥ�����֤���']:
                continue
            ldmc=''.join(tab3.xpath('//div[@id="content1"]/table/tr[2]/td[2]/text()')).strip()
            ts=''.join(tab3.xpath('//div[@id="content1"]/table/tr[4]/td[2]/text()')).strip()
            mj=''.join(tab3.xpath('//div[@id="content1"]/table/tr[4]/td[4]/text()')).strip()
            ceil2['����¥��']=ldmc
            ceil2['����']=ts.replace('��','')
            ceil2['Ԥ��֤׼���������']=mj
            print('loupan=',ldmc,'ts=',ts)
            rooms=re.findall("searchByLid\('(.*?)'",h3,re.S)
            if len(rooms)==0:
                rslist.append(ceil2)
                continue
            ###rooms
            for room in rooms:
                ceil3=ceil2.copy()
                h4=getHtml('http://www.ccfdw.net/ecdomain/lpcs/xmxx/huxx.jsp?hid='+room+'&lid='+ldid,'')
                price=''.join(re.findall('Ԥ�ۼ۸�.*?</td>.*?>(.*?)</td>',h4,re.S)).strip()
                sale=''.join(re.findall('����״̬.*?</td>.*?>(.*?)</td>',h4,re.S)).strip()
                if '����' not in sale :
                    sale='������'
                fh=''.join(re.findall('�����.*?</td>.*?>(.*?)</td>',h4,re.S)).strip()
                jzmj=''.join(re.findall('Ԥ�⽨�����.*? </td>.*?>(.*?)</td>',h4,re.S)).strip()
                if len(price)>3:
                    ceil3['���ۼ۸�']=price
                ceil3['����']=fh
                ceil3['���ݽ������']=jzmj
                ceil3['��������״̬']=sale
                rslist.append(ceil3)
                # print(ceil3)
                # time.sleep(32222)
    except Exception as e:
        print(traceback.format_exc()) 
    if len(rslist)==0:
        rslist.append(ceil)
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
    oklist1={}
    for i in oklist:
        oklist1[i]=''
    outlist=openList(outfile)
    for i in outlist:
        oklist1[i]=''
    newlist=[]
    of = open(outlistfile,'a+', encoding='utf-8') #�������ļ�
    end='no'
    pn=0
    while True :
        pn+=1
        print(pn) 
        html=getHtml(url.replace('$1',str(pn)),'')
        hrefs=re.findall('openUrl\(\'(.*?)\'\).*?>(.*?)</a>',html,re.S)
        total=int(re.findall('��ǰ��\d+/(\d+)ҳ',html,re.S)[0])
        print(len(hrefs),total)
        if len(hrefs)==0:
            break   
        for j,pro in hrefs:
            contenturl='www.ccfdw.net/ecdomain/ccfcjwz/wsba/ysxk/searchByInfoId.jsp?id='+j
            #�������棬��ҳ��ֹ���
            if pro.find('2020')>0:                
                print('end')
                end='yes'
                break
            if contenturl in newlist:
                continue
            if contenturl in outlist:
                print('exists')
                continue
            newlist.append(contenturl)
            of.write(contenturl+'\n')
            of.flush() 
           
        if total<=pn or end=='yes':
            break
   
    with open(outlistfile,'r',encoding='UTF-8') as txtData: 
        for line in txtData.readlines():
            contenturl=line.strip()
            if contenturl in oklist1:
                continue
            urls.put(contenturl)
            # outstr=get_data(contenturl)
            # do_write(outfile,contenturl,outstr)
            # time.sleep(1111)
        print("qsize="+str(urls.qsize())) 
        time.sleep(3)       
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
    of = open(filename,'a+', encoding='utf-8') #�����ļ�
    for data in datalist:
        if datalist not in old_datalist:
            of.write(data+"\n")
            of.flush()              
if __name__ == '__main__':
		main()