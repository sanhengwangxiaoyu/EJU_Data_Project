# coding=gbk
import queue,time,random,os
from lxml import etree
from selenium import webdriver
import re,urllib.parse
import requests
import json
import xlrd
import traceback

dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/guiyang_ysz.txt"
outlistfile="./list/guiyang.txt"
url="https://www.gyfc.net.cn/2_proInfo/search.aspx?type=1&box_pro_qu=-1&box_pro_xz=undefined&box_pro_mc=&box_pro_dj="

arealist=[]
def trim(word):
    return re.sub('[\s]*','',word).replace('\\n','').replace('\\xa0','').replace('\\t','').replace('&nbsp;','')
def get_data(contenturl):
    s=contenturl.split('\t')
    uid=s[0].split('?')[1].split('&')
    xkzh=uid[0].replace('yszh=','')
    uid[0]=urllib.parse.quote(uid[0].replace('yszh=','').encode('gb2312'))
    html=getHtml(f'https://www.gyfc.net.cn/pro_query/index.aspx?yszh={uid[0]}&{uid[1]}')
    lxmls = etree.HTML(html)
    trs =[]
    city='贵阳'
    area=''
    rslist=[]
    cell={}
    cell2={}
    cell3={}
    detail=''
    newhtml=''
    progect=s[1]
    addr=s[2]
    
    table=lxmls.xpath('//table[@id="Table2"]/tr')
    #开发商
    company=trim(''.join(re.findall('开发商：.*?</td>.*?>(.*?)</td>',html,re.S)))
    #sale
    saleaddr=trim(''.join(re.findall('售楼处地址：.*?</td>.*>(.*?)</td>',html,re.S)))
    xsdh=''.join(re.findall('销售电话：.*?</td>.*?>(.*?)</td>',html,re.S)).strip()
    print(xsdh)
    xsdh=re.sub('([\s、，,\.])+','/',xsdh)
    newurl=''.join(re.findall('/pro_query/index/proAffiche\.aspx\?gonggaoId=(.*?)yszh=',html,re.S))
    newurl=f'https://www.gyfc.net.cn/pro_query/index/proAffiche.aspx?gonggaoId={newurl}&yszh={uid[0]}'
    #ysmj
    ysmj=''
    fzrq=''
    if len(newurl)>0:
        tmp=getHtml(newurl)
        ysmj=trim(''.join(re.findall('预售面积(?:：|\:)(.*?)<',tmp,re.S)))
        if len(ysmj)==0:
            ysmj=trim(''.join(re.findall('建筑面积(?:：|\:)(.*?)<',tmp,re.S)))
        fzrq=trim(''.join(re.findall('有效期(?:：|\:)(.*?)(?:至|<)',tmp,re.S)))
        if len(fzrq)==0:
            fzrq=trim(''.join(re.findall('预售许可延期情况.*>([\d\- ]*)</td><td.*?>[\d\- ]*</td>\s*</tr>\s*</table>',tmp,re.S)))
    #addr
    cell['城市']=city
    cell['项目名称']=progect
    cell['坐落位置']=addr
    cell['开发企业']=company
    cell['预售许可证编号']=xkzh
    cell['发证日期']=fzrq
    cell['预售证准许销售面积']=ysmj
    cell['售楼地址']=saleaddr
    cell['售楼电话']=xsdh
    print(cell)
    return [cell]
def getHtml(link):
    html=""
    while True:
        try: #使用try except方法进行各种异常处理
            header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'}
            res = requests.get(link,headers=header,timeout=10,verify=False) #读取网页源码
            #解码
            
            print(link)
            if res.encoding=='utf-8' or res.encoding=='UTF-8' or res.text.find('charset="utf-8"')>0:
                    res.encoding='utf-8'
            else:
                    m = re.compile('<meta .*(http-equiv="?Content-Type"?.*)?charset="?([a-zA-Z0-9_-]+)"?', re.I).search(res.text)
                    if m and m.lastindex == 2:
                        charset = m.group(2).upper()
                        res.encoding=charset
                    else:
                        res.encoding='GBK'
            res.encoding='GBK'
            html=res.text
            break
        except Exception as e:
            print(traceback.format_exc())
            if str(e).find('Read timed out')>0:
                time.sleep(1)
                continue
            break
    return html

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
def main():
    oklist=[]
    num=0
    of=open(outfile,'r',encoding='utf-8')
    lines=of.readlines()
    for ss in lines:
        sss=ss.split('\t')[0]
        oklist.append(sss)
        num+=1
    of=open(outlistfile,'r',encoding='utf-8')
    lines=of.readlines()
    for contenturl in lines:
        num-=1
        #增量 
        sss=contenturl.split('\t')[0]
        if sss in oklist:
            print('pass',num)
            continue
        oklist.append(sss)
        #
        outstr=get_data(contenturl)
        print('less',num)
        # print(outstr)
        do_write(outfile,contenturl.split('\t')[0],outstr)
        time.sleep(1)   
        
if __name__ == '__main__':
		main()