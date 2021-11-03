#coding=gbk
import time,random
from lxml import etree
import base64

import execjs

from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

import re,os
import requests
import json
import traceback

import base64
from Crypto.Cipher import PKCS1_v1_5 as Cipher_pksc1_v1_5
from Crypto.PublicKey import RSA

from threading import Thread
from queue import Queue

urls = Queue()
ysz_oklist=set()
dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="./data/chengdu_new.txt"
outlistfile="./list/chengdu.txt"
okfile="./data/chengdu.txt"

url='https://zw.cdzj.chengdu.gov.cn/zwdt/SCXX/Default.aspx?action=ucSCXXShowNew2'

def trim(word):
    return re.sub('\s+','',word).replace('\\n','').replace('\\xa0','').replace('\\t','').replace('&nbsp;','')

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

def main():
    #已爬列表
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
    if True :
        driver = webdriver.Firefox()
        # of = open(outlistfile,'a+', encoding='utf-8') #保存结果文件
        try: #使用try except方法进行各种异常处理
            driver.get(url)
            time.sleep(3)
            text=driver.page_source
            total=int(re.findall('共(\d+)页',text)[0])+1
            for page in range(1,total):
                tab=etree.HTML(text)
                tab11=tab.xpath('//table[@id="ID_ucSCXXShowNew2_gridView"]/tbody/tr[position()>1]')
                print('page=',page,len(tab11))
                for tr in tab11:
                    contenturl=tr.xpath('./td[12]/a/@href')[0].replace('\n','')
                    if tr.xpath('./td[1]//text()')[0] in ysz_oklist:
                        continue
                    outstr={
                        '城市':'成都',
                        '项目名称':tr.xpath('./td[2]//text()')[0],
                        '坐落位置':tr.xpath('./td[4]//text()')[0],
                        '开发企业':tr.xpath('./td[6]//text()')[0],
                        '预售许可证编号':tr.xpath('./td[1]//text()')[0],
                        '预售证准许销售面积':tr.xpath('./td[7]//text()')[0],
                        '开盘日期':tr.xpath('./td[8]')[0].xpath('string(.)').strip()
                    }
                    do_write(outfile,contenturl,[outstr])
                while True:
                    try:
                        driver.execute_script("javascript:__doPostBack('ID_ucSCXXShowNew2$UcPager1$btnNewNext','')")
                        # driver.execute_script("javascript:__doPostBack('ID_ucSCXXShowNew2$UcPager1$btnNewLast','')")
                        driver.implicitly_wait(10)
                        time.sleep(3)
                        break
                    except Exception as e :
                        print('refresh input')
                        input()
                text=driver.page_source
        except Exception as e:
            print(traceback.format_exc()) 
        finally:
            driver.close()
      
if __name__ == '__main__':
		main()