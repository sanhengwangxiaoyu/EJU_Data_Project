# -*- coding:utf-8 -*-  
__author__ = '#######'
__date__ = '2021/9/24 13:32'

import requests,os,re,time
from lxml import etree
from fake_useragent import UserAgent
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

##开始时间
print('start time -->   '+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
##指定输出
dt=time.strftime('%Y%m%d',time.localtime(time.time()))
outfile="guiyang_"+ dt + ".txt"
##指定本地的useragent，并创建useragent对象用作浏览
location = os.getcwd() + '\\fake_useragent.json'
ua = UserAgent(path=location)
##创建session对象
pool = requests.session()
##设置连接池大小
pool.mount('http://',HTTPAdapter(
  pool_connections=100, #连接池数量
  pool_maxsize=200,     #连接池允许的最大长连接数量
  max_retries=          #设置重试次数
    Retry(total=10,     #手动指定重试次数
      backoff_factor=1, #重试间隔时间:urllib3的backoff_factor算法
      method_whitelist=frozenset(['GET','POST']) # 设置 post()方法进行重访问
)))
# HTTP “请求头信息”
headers = {
    'User-Agent': ua.random,
    'Connection': 'close',
}
pool.headers.update(headers)
# 访问链接
lburl = 'http://www.fcxx0898.com/syfcSiteWeb/Pages/Project/PresaleList.aspx?id=16062'
r = pool.get(lburl,timeout=60, headers=headers)
# # 转译编码
table1 = etree.HTML(r.content.decode('utf-8'))
# # 获取指定内容
# table2 = table1.xpath('//div//div//div//div[3]//table//tbody//tr')
# table2[0].xpath(".//td[1]//@class")
dict2 = {
    "URL":lburl ,
    "城市": '沈阳',
    "项目名称": ''.join(table2[1].xpath(".//td[1]//text()")).replace('\r\n','').replace('\t',''),
    "坐落位置": ''.join(table2[1].xpath(".//td[2]//text()")),
    "开发企业": ''.join(table2[1].xpath(".//td[3]//text()")),
    "预售许可证编号": '',
    "发证日期": '',
    "开盘日期": ''.join(table2[1].xpath(".//td[4]//text()")),
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





























table2[1]
url8 = ''.join(table2[1].xpath(".//td[1]//a//@href"));
url2 = 'http://124.95.133.164'+url8

dict2 = {
    "URL":url2 ,
    "城市": '沈阳',
    "项目名称": ''.join(table2[1].xpath(".//td[1]//text()")).replace('\r\n','').replace('\t',''),
    "坐落位置": ''.join(table2[1].xpath(".//td[2]//text()")),
    "开发企业": ''.join(table2[1].xpath(".//td[3]//text()")),
    "预售许可证编号": '',
    "发证日期": '',
    "开盘日期": ''.join(table2[1].xpath(".//td[4]//text()")),
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
headers = {
    'User-Agent': ua.random,
    'Connection': 'close',
}

pool.headers.update(headers)
r2 = pool.get(url2, timeout=60, headers=headers)
table3 = etree.HTML(r2.content.decode('gbk'))
table4 = table3.xpath('//table//table//table//tr[position()>1]')
table4[5]

dict2["销售楼号"]=''.join(table4[5].xpath(".//td[1]//text()")).replace("\xa0","")
dict2["套数"]=''.join(table4[5].xpath(".//td[5]//text()")).replace("\xa0","")
url4 = ''.join(table4[5].xpath(".//td[1]//a//@href"));

houseid = re.findall(r'houseid=(.*?)&', url4, re.I)[0]
url3='http://124.95.133.164/work/xjlp/door_list2.jsp?houseid='+houseid
headers = {
'User-Agent': ua.random,
'Connection': 'close',
}
pool.headers.update(headers)
r3 = pool.get(url3, timeout=60,headers=headers)
table5 = etree.HTML(r3.content.decode('gbk'))
table6 = table5.xpath('//table//td')
table5.xpath('//table//td')
dict4={}
table6[1]

url9=''.join(table6[1].xpath(".//a/@href"))
xszt = url9.split('&')[1].replace('xszt=','')
r4 = pool.get('http://124.95.133.164'+url9, timeout=60, headers=headers)
table7 = etree.HTML(r4.content.decode('gbk'))
table8 = table7.xpath('//table//table/table[2]//tr[8]//td[2]/text()')
dict3 = {
   "房号": ''.join(table6[1].xpath(".//text()")).replace('\r\n','').replace('\t','').strip(),
   "房屋建筑面积": ''.join(table8).replace("\xa0",""),
   "房屋销售状态": xszt,
}
dict4 = dict2.copy()
dict4.update(dict3)
df = DataFrame(dict4, index=[0])
df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)

if len(dict4)==0:
    df = DataFrame(dict2, index=[0])
    df.to_csv(outfile, sep='\t', mode='a', index=False, header=None)
