#In[]
from email import header
from lxml import etree
import requests,re


header = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36",
}
date = '2022-01-04'
top_level_url = 'http://bjjs.zjw.beijing.gov.cn/eportal/ui?pageId=307670'




#In[]
for i in range(1,10):
    end_date = "9999-99-99"
    data = {
        "rblFWType": "q",
        "currentPage": i,
        "pageSize": 15,
    }
    resq = requests.post(top_level_url,headers=header,data=data,verify=False)
    resq.encoding='UTF-8'
    web_text = re.sub(r'\n|\r|\t','',resq.text)
    #解析
    html  = etree.HTML(web_text)
    #拿到每一个服务商的div
    tbodys = html.xpath("//form//table[3]/tr[2]/td/table/tr")
    for tr in tbodys:
        if len(tr.xpath("./td[1]/a/text()")) == 0  :
            continue
        url = "http://bjjs.zjw.beijing.gov.cn" + tr.xpath("./td[2]/a/@href")[0].strip()
        newest_name = tr.xpath("./td[1]/a/text()")[0].strip()
        issue_date = tr.xpath("./td[3]/text()")[0].strip()
        issue_code = tr.xpath("./td[2]/a/text()")[0].strip()
        if issue_date<=date :
            end_date = issue_date
            break
        print(str(url) + '\t' + str(newest_name) + '\t' + str(issue_date) + '\t' + str(issue_code))
    if end_date<=date :
        break
        

#In[]
for i in range(1,10):
    end_date = "9999-99-99"
    data = {
        "rblFWType": "x",
        "currentPage": i,
        "pageSize": 15,
    }
    resq = requests.post(top_level_url,headers=header,data=data,verify=False)
    resq.encoding='UTF-8'
    web_text = re.sub(r'\n|\r|\t','',resq.text)
    #解析
    html  = etree.HTML(web_text)
    #拿到每一个服务商的div
    tbodys = html.xpath("//form//table[3]/tr[2]/td/table/tr")
    for tr in tbodys:
        if len(tr.xpath("./td[1]/a/text()")) == 0  :
            continue
        url = "http://bjjs.zjw.beijing.gov.cn" + tr.xpath("./td[2]/a/@href")[0].strip()
        newest_name = tr.xpath("./td[1]/a/text()")[0].strip()
        issue_date = tr.xpath("./td[3]/text()")[0].strip()
        issue_code = tr.xpath("./td[2]/a/text()")[0].strip()
        if issue_date<=date :
            end_date = issue_date
            break
        print(str(url) + '\t' + str(newest_name) + '\t' + str(issue_date) + '\t' + str(issue_code))
    if end_date<=date :
        break




#In[]
for i in range(1,10):
    data = {
        "rblFWType": "q",
        "currentPage": i,
        "pageSize": 15,
    }
    print(i)
    resq = requests.post(url,headers=header,data=data,verify=False)
    resq.encoding='UTF-8'
    web_text = re.sub(r'\n|\r|\t','',resq.text)
    #解析
    html  = etree.HTML(web_text)
    #拿到每一个服务商的div
    tbodys = html.xpath("//form//table[3]/tr[2]/td/table/tr")

    for tr in tbodys:
        if len(tr.xpath("./td[1]/a/text()")) == 0  :
            continue
        url = "http://bjjs.zjw.beijing.gov.cn" + tr.xpath("./td[2]/a/@href")[0].strip()
        newest_name = tr.xpath("./td[1]/a/text()")[0].strip()
        issue_date = tr.xpath("./td[3]/text()")[0].strip()
        issue_code = tr.xpath("./td[2]/a/text()")[0].strip()
        if issue_date<=date :
            exit()
        print(str(url) + '\t' + str(newest_name) + '\t' + str(issue_date) + '\t' + str(issue_code))




