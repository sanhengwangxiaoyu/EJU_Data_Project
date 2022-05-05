#In[]
from cmath import isnan
from typing import List
import requests,json,pandas as pd,time,numpy as np,pymysql,os,configparser,datetime,re,random
from sqlalchemy import create_engine
from lxml import etree
from requests.adapters import HTTPAdapter
from queue import Queue
from difflib import SequenceMatcher#导入库


##读取配置文件##
pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块
##设置变量初始值##
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
# database = 'odsdb'
date = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')+" T00:00:00"
# date = "2021-12-24T00:00:00"
# day = "2021-12-08"
df = pd.DataFrame(columns=['url','region','gd_city','floor_name','address','business','issue_code','issue_date', \
                 'issue_area','building_code','room_sum'])
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
}
##mysql连接配置##
# -*- coding: utf-8 -*-
class MysqlClient:
    def __init__(self, db_host,database,user,password):
        """
        create connection to hive server
        """
        self.conn = pymysql.connect(host=db_host, user=user,password=password,database=database,charset="utf8")
    def query(self, sql):
        """
        query
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        columnDes = cur.description #获取连接对象的描述信息
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]
        data = pd.DataFrame([list(i) for i in res],columns=columnNames)
        cur.close()
        return data
    # 更新SQL
    def updata_one(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()
    def close(self):
        self.conn.close()   

##mysql写入执行##
def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)
#创建数据库连接
con = MysqlClient(db_host,database,user,password)

def is_nullto_value(value_str):
    if value_str is None :
        return '暂无'
    elif len(value_str) == 0:
        return '暂无'
    else:
        return value_str

def repsPost(url,headers,data,res_type) :
    response = requests.post(url, headers=headers, data=data)
    response.encoding='UTF-8'
    response.close()
    if res_type == 'json' :
        return response.json()
    else :
        return response.text

def respGet(url,headers,data,res_type) :
    response = requests.get(url,headers=headers,data=data,timeout=15,verify=False) #读取网页源码
    response.encoding='UTF-8'#解码
    response.close()
    if res_type == 'json' :
        return response.json()
    else :
        return response.text 

def sess_repPost(sess,url,headers,data,res_type) :
    # proxiess = [{ "https":"http://121.205.220.201:8080"},{ "https":"http://101.74.2.219:8888"},{ "https":"http://122.230.151.134:8080"},{ "https":"http://223.242.229.22:8080"},{ "https":"http://115.208.233.9:8080"}]
    # proxies = random.choice(proxiess)
    # print(proxies)
    # response = sess.post(url,headers=headers,data=data,timeout=15,verify=False,proxies=proxies) #读取网页源码
    response = sess.post(url,headers=headers,data=data,timeout=15,verify=False) #读取网页源码
    response.encoding='UTF-8'#解码
    response.close()
    if res_type == 'json' :
        return response.json()
    else :
        return response.text

def sess_repGet(sess,url,headers,data,res_type) :
    response = sess.get(url,headers=headers,data=data,timeout=15,verify=False) #读取网页源码
    response.encoding='UTF-8'#解码
    response.close()
    if res_type == 'json' :
        return response.json()
    else :
        return response.text
#计算两字符串之间的相似成都
def similarity(a, b):
    return SequenceMatcher(None, b, a).ratio()#引用ratio方法，返回序列相似性的度量
#对一个字符串内容，进行去重的方法
def str_drop_duplicates(str):
    listl_newest_name = list(str)
    lists_newest_name = list(set(str))
    lists_newest_name.sort(key=listl_newest_name.index)
    str_result = "".join(lists_newest_name)
    return str_result



issue_ccode_maxdate = con.query("select gd_city,max(issue_date) issue_date from odsdb.ods_city_issue_code where dr = 0 and issue_date != '9999-09-09' group by gd_city")  #指定已经爬取的预售证信息
issue_ccode = con.query("select gd_city,issue_date,issue_code from odsdb.ods_city_issue_code where dr = 0 and issue_date != '9999-09-09' and region != '通州区' group by issue_code,issue_date,gd_city")  #指定已经爬取的预售证信息
issue_ccode = pd.merge(issue_ccode,issue_ccode_maxdate,how='inner',on=['issue_date','gd_city']) #筛选出最大时间的所有预售证
issue_ccode = issue_ccode.groupby(by=['issue_date','gd_city']).agg({'issue_code':list}).reset_index()

# #获取免费代理
# proxiess = []
# proxies_data = {}
# proxies_tab1 = etree.HTML(respGet('http://www.taiyanghttp.com/free/',headers,proxies_data,'text'))
# proxies_divs=proxies_tab1.xpath('/html/body/section/div[2]/div/div[2]/div')
# for j in proxies_divs:
#     ip =''.join(j.xpath('./div[1]/text()')).strip()
#     port = ''.join(j.xpath('./div[2]/text()')).strip()
#     proxiess.append({'http':'http://'+ip+':'+str(port),'https':'http://'+ip+':'+str(port)})

# response_2 = requests.get('http://www.66ip.cn/index.html',headers=headers,timeout=15,verify=False) #读取网页源码
# response_2.encoding='GBK'#解码
# response_2.close()
# proxies_tab2 = etree.HTML(response_2.text)
# proxies_tbodys=proxies_tab1.xpath('/html/body/div[4]/div[1]/div[2]/div[1]/table/tbody/tr')
# for j in proxies_tbodys:
#     ip =''.join(j.xpath('./tr[2]/td[1]/text()')).strip()
#     port = ''.join(j.xpath('./tr[2]/td[2]/text()')).strip()
#     proxiess.append({'http':'http://'+ip+':'+str(port),'https':'http://'+ip+':'+str(port)})

# proxiess


#In[]
#####赣州
GZstart_date_time = issue_ccode['issue_date'][(issue_ccode['gd_city']=='赣州市')].values[0]  # 获取爬虫开始时间
GZissue_ccode_list = issue_ccode['issue_code'][(issue_ccode['gd_city']=='赣州市')].values[0]  # 获取预售证号列表

url = 'http://111.75.255.76/PropertyIDInfo/Presell/GetShareList'
GZ_sess = requests.Session()
params = {
    "county": "",
    "project": "",
    "company": "",
    "license": "",
    "pageindex": "1",
    "pagerecord": "20"
}
GZ_sess.mount('http://', HTTPAdapter(max_retries=3))  #添加http协议的重试次数
GZ_sess.mount('https://', HTTPAdapter(max_retries=3)) #不想思考,两个协议都添加
resq = sess_repPost(GZ_sess,url,headers,params,'json')

floor_info = pd.json_normalize(resq['Data']['PresellShareList'])
floor_info['BLSJ'] = floor_info['BLSJ'].apply(lambda x: time.strftime(  "%Y-%m-%d",time.localtime( int(re.findall(r"\d+",x)[0])/1000 )  )) #转换时间
lost_newest = pd.DataFrame(columns=['floor_name','issue_date','bussiness'])


# floor_info = floor_info[(floor_info['BLSJ'] == '2022-02-11')&(floor_info['ZXKZH'] == '信售许字（2021）60')]

for index, row in floor_info.iterrows():
    issue_code = is_nullto_value(row['ZXKZH'])
    newest_name = row['YSXMMC']
    building_area = 0
    room_num = 0
    issue_building_code = ''
    if row['YSXMMC'] is None:
        continue
    elif issue_code is None or issue_code == '0':
        issue_code = '无'
    print(row['QXMC'] +'     '+ row['YSXMMC'] +'     '+ row['QYMC'] +'     '+ is_nullto_value(row['ZXKZH']) +'     '+ row['BLSJ'] )
    # if '测试' not in row['YSXMMC'] and row['BLSJ'] >= '2017-01-03' and row['BLSJ'] <= '2022-10-01' and issue_code not in ('xxxxxxxxxxxx'): ## 筛选当前时间
    if '测试' not in row['YSXMMC'] and row['BLSJ'] >= GZstart_date_time and issue_code not in GZissue_ccode_list:
        child_url = 'http://111.75.255.76/jsonHandle/GetKFSItemTree_ForManager?bid=&fid=&nc='
        child_params = {
            "h_id": "DE0BC504D090FF69",
            "QYJG": "all",
            "type": "0",
            "Accuracy": "false",
            # "condition": "赣州祥嵘置业有限公司"
            "condition": row['QYMC']
        }
        
        child_resq = sess_repPost(GZ_sess,child_url,headers,child_params,'json')
        # time.sleep(3)
        # json_data_company = ('[{'+child_resq['Data'].split(r'[{',2)[2].replace("'",'"').replace('id','"id"').replace('text','"text"').replace('im0','"im0"').replace('im1','"im1"').replace('im2','"im2"').replace('item','"item"'))[:-3]
        # df_data_company = pd.json_normalize(json.loads(json_data_company))
        df_data_company = pd.DataFrame(
                              ((pd.DataFrame(
                                    (pd.json_normalize(json.loads(
                                            child_resq['Data'].replace("'",'"').replace('id','"id"').replace('text','"text"').replace('im0','"im0"').replace('im1','"im1"').replace('im2','"im2"').replace('item','"item"')
                                        )) ##将json中的Data数据转为dataframe 
                                    ).explode('item', ignore_index=True)['item'].values.tolist())  ###将dataframe中的item字段的数据单独转成Dataframe
                                ).explode('item', ignore_index=True))['item'].values.tolist()      ###单独讲item列表数据拆分多行,在转换为列表
                          )    ###再转一层  : 将dataframe中的item字段的数据单独转成Dataframe
        """
            首先判断项目名称能否直接找到， 如果找到进行转换
            第二次判断是否包含数字：如果包含数字就先按照数子进行切分进行比较后，再按照数字再进行比较，取两者的最大值进行转换
            以上条件都不符合直接按照原来的进行比较，之后取比较的最大值并转换
        """
        if len(df_data_company['item'].values[0])==0:
            lost_newest_new=pd.DataFrame({'floor_name':newest_name ,'issue_date': row['BLSJ'],'bussiness':row['QYMC']},index=[0])
            lost_newest=lost_newest.append(lost_newest_new,ignore_index=True)
            continue
        if row['YSXMMC'] in  df_data_company['text'].values.tolist(): ###判断查找到的所有楼栋信息，是否在项目名称当中
            df_data_company = df_data_company[df_data_company['text'] == row['YSXMMC']]
            df_data_company['issue_code'] = issue_code
            df_data_company = df_data_company[df_data_company['item'].apply(lambda x: len(x)) != 0]
            df_data_name = pd.DataFrame(( df_data_company.explode('item', ignore_index=True))['item'].values.tolist())
        elif len(df_data_company[df_data_company['text'].apply(lambda x: x in row['YSXMMC'])])>=1:
            df_data_company = df_data_company[df_data_company['text'].apply(lambda x: x in row['YSXMMC'])]
            df_data_company['floor_name_rate'] = df_data_company['text'].apply(lambda x: similarity(re.split('\d+',x)[0],re.split('\d+',row['YSXMMC'])[0]))
            df_data_company['floor_name_rate2'] = df_data_company['text'].apply(lambda x: similarity(re.sub('\D','',x),re.sub('\D','',row['YSXMMC'])))
            df_data_company['issue_code'] = issue_code
            df_data_namerate = df_data_company.groupby(['issue_code'])['floor_name_rate'].max().reset_index()
            df_data_namerate2 = df_data_company.groupby(['issue_code'])['floor_name_rate2'].max().reset_index()
            df_data_name = pd.DataFrame(((
                                df_data_company[df_data_company['floor_name_rate']==df_data_namerate['floor_name_rate'].values[0]]  ##获取df_data_company中最大匹配度的楼房
                            ).explode('item', ignore_index=True))['item'].values.tolist())
            if len(df_data_company[df_data_company['floor_name_rate']==df_data_namerate['floor_name_rate'].values[0]])>1 :
                df_data_name = pd.DataFrame(((
                                df_data_company[(df_data_company['floor_name_rate']==df_data_namerate['floor_name_rate'].values[0])&(df_data_company['floor_name_rate2']==df_data_namerate2['floor_name_rate2'].values[0])]  ##获取df_data_company中最大匹配度的楼房
                            ).explode('item', ignore_index=True))['item'].values.tolist())
        elif bool(re.search(r'\d', row['YSXMMC'] )):  ##现根据切分后的判断，取最大值，如果只取出一个最大值，继续下边步骤。  如果取出多个值再按照数字进行判断
            df_data_company['floor_name_rate'] = df_data_company['text'].apply(lambda x: similarity(re.split('\d+',x)[0],re.split('\d+',row['YSXMMC'])[0]))
            df_data_company['floor_name_rate2'] = df_data_company['text'].apply(lambda x: similarity(re.sub('\D','',x),re.sub('\D','',row['YSXMMC'])))
            df_data_company['issue_code'] = issue_code
            df_data_namerate = df_data_company.groupby(['issue_code'])['floor_name_rate'].max().reset_index()
            df_data_namerate2 = df_data_company.groupby(['issue_code'])['floor_name_rate2'].max().reset_index()
            df_data_name = pd.DataFrame(((
                                df_data_company[df_data_company['floor_name_rate']==df_data_namerate['floor_name_rate'].values[0]]  ##获取df_data_company中最大匹配度的楼房
                            ).explode('item', ignore_index=True))['item'].values.tolist())
            if len(df_data_company[df_data_company['floor_name_rate']==df_data_namerate['floor_name_rate'].values[0]])>1 :
                df_data_name = pd.DataFrame(((
                                df_data_company[(df_data_company['floor_name_rate']==df_data_namerate['floor_name_rate'].values[0])&(df_data_company['floor_name_rate2']==df_data_namerate2['floor_name_rate2'].values[0])]  ##获取df_data_company中最大匹配度的楼房
                            ).explode('item', ignore_index=True))['item'].values.tolist())
                
            # df_data_name = pd.DataFrame(df_data_company['item'][df_data_company['floor_name_rate']==df_data_namerate['floor_name_rate'].values[0]].values[0])
        else: 
            df_data_company['floor_name_rate'] = df_data_company['text'].apply(lambda x: similarity(x,row['YSXMMC']))
            df_data_company['issue_code'] = issue_code
            df_data_company = df_data_company[df_data_company['item'].apply(lambda x: len(x)) != 0]
            df_data_namerate = df_data_company.groupby(['issue_code'])['floor_name_rate'].max().reset_index()
            df_data_name = pd.DataFrame(((
                                df_data_company[df_data_company['floor_name_rate']==df_data_namerate['floor_name_rate'].values[0]]  ##获取df_data_company中最大匹配度的楼房
                            ).explode('item', ignore_index=True))['item'].values.tolist())
        if len(df_data_name) == 0:##记录被跳过的楼盘
            lost_newest_new=pd.DataFrame({'floor_name':newest_name ,'issue_date': row['BLSJ'],'bussiness':row['QYMC']},index=[0])
            lost_newest=lost_newest.append(lost_newest_new,ignore_index=True)
            continue
        df_data_name['issue_code'] = issue_code
        df_data_name['text'] = df_data_name['text'].apply(lambda x: re.sub("[\u4e00-\u9fa5]", "", re.sub(r'.*地块','',x)).replace('#',''))
        if bool(re.search(r'\d', row['YSXMMC'] )) or bool(re.search(r'\d#', row['YSXMMC'] )) or bool(re.search(r'\d楼', row['YSXMMC'] )) or bool(re.search(r'\d号', row['YSXMMC'] )) or bool(re.search(r'\d栋', row['YSXMMC'] )) or bool(re.search(r'\d撞', row['YSXMMC'] )):  
            df_data_name['est'] = df_data_name['text'].apply(lambda x: 0 if x in re.sub("[\u4e00-\u9fa5]", "", row['YSXMMC']) else 1)
            df_data_name = df_data_name[df_data_name['est'] == 0 ]
        if len(df_data_name) == 0:##记录被跳过的楼盘
            lost_newest_new=pd.DataFrame({'floor_name':newest_name ,'issue_date': row['BLSJ'],'bussiness':row['QYMC']},index=[0])
            lost_newest=lost_newest.append(lost_newest_new,ignore_index=True)
            continue
        issue_building_code = (df_data_name.groupby(['issue_code'])['text'].apply(list).to_frame())['text'].values[0]
        for index, child_row in df_data_name.iterrows():
            if len( pd.DataFrame(child_row['item'])) >=1 :
                newest_name = newest_name.replace(child_row['text'],'')
                df_data_id = pd.DataFrame(child_row['item'])
                data_id = df_data_id['id'].values[0]
                grand_son_url = 'http://111.75.255.76/JsonHandle/GetHouseResourcesData_House_DW?bid=&fid=&nc=&dbh='+data_id+'&hid=DE0BC504D090FF69'
                grand_son_params = {
                    "bid": "",
                    "fid": "",
                    "nc": "",
                    "dbh": data_id,
                    "hid": "DE0BC504D090FF69"
                }
                try:
                    grand_son_resq = sess_repPost(GZ_sess,grand_son_url,headers,grand_son_params,'json')
                except Exception as e:
                    print(e)
                    lost_newest_new=pd.DataFrame({'floor_name':newest_name+">>>>"+  df_data_id['text'].values[0],'issue_date': row['BLSJ'],'bussiness':row['QYMC']},index=[0])
                    lost_newest=lost_newest.append(lost_newest_new,ignore_index=True)
                    continue

                if len(grand_son_resq['Data']) == 0:
                    continue
                data_room = pd.DataFrame(grand_son_resq['Data'])
                data_room['issue_code'] = issue_code
                room_num = room_num + len(data_room)
                building_area = building_area + (data_room.groupby(['issue_code'])['BuildArea'].sum().reset_index())['BuildArea'].values[0]
                # time.sleep(2)
    else:
        print("从赣州网址的第"+'1'+"页获取 :" + (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d') +" 登记的数据失败")
        break
        # continue
    
    newest_name = str_drop_duplicates(re.sub(r'[A-Z]|-|栋|幢|、|高层|洋楼|K|地块|#|楼|号|\d|.期.*|地下.*|，|（）|（|车库.*','',newest_name))
    busniss = row['QYMC']
    issue_code = issue_code
    issue_location = row['QXMC']
    issue_region = row['QXMC']
    issue_area = building_area
    issue_room_num = room_num
    issue_start_date = row['BLSJ']
    issue_building_code = row['YSXMMC']+'----'+",".join(issue_building_code)
    city_name = "赣州市"
    # print('项目名称:  '+newest_name+'      '+'开发商:  '+busniss+'      '+'预售证号:  '+str(issue_code)+'      '+'楼盘地:  '+issue_location+'      '+'楼盘区县:  '+str(issue_region)+'      '+'预售面积:  '+str(issue_area)+'      '+'预售套数: ' +str(issue_room_num)+'      '+'发证时间:  '+str(issue_start_date)+'      '+'建筑编号:  '+str(issue_building_code)+'      '+'城市名称:  '+city_name)
    new=pd.DataFrame({'url':url,'region':issue_region,'gd_city':city_name,'floor_name':newest_name,'address':issue_location,'business':busniss,'issue_code':issue_code,'issue_date':issue_start_date,'issue_area':issue_area,'building_code':issue_building_code,'room_sum':issue_room_num},index=[0])
    df=df.append(new,ignore_index=True)
    print(df[['issue_code','issue_date','floor_name']].tail(5))
    print(lost_newest[['issue_date','floor_name','bussiness']].tail(5))
    # time.sleep(10) 
    re.sub(r'.期.*','','东方名城一期第一批地下室')





#In[]
# ###石家庄

def sjz_getHtml(url):
    proxiess = [
             { "https":"http://117.63.128.218:4375"},{ "https":"http://220.179.219.173:4331"},{ "https":"http://114.239.148.78:4331"},{ "https":"http://60.18.160.118:4340"},{ "https":"http://117.34.192.37:4320"},{ "https":"http://124.94.191.25:4352"},{ "https":"http://113.235.72.160:4364"},{ "https":"http://121.230.46.93:4326"},{ "https":"http://182.147.82.35:4356"},{ "https":"http://115.212.162.156:4368"},{ "https":"http://27.150.160.244:4345"},{ "https":"http://115.209.23.2:4331"},{ "https":"http://115.239.16.191:4378"},{ "https":"http://120.40.181.166:4354"},{ "https":"http://183.165.193.15:4315"},{ "https":"http://113.241.137.98:4310"},{ "https":"http://119.109.85.237:4378"},{ "https":"http://58.46.249.200:4313"},{ "https":"http://114.106.171.34:4358"},{ "https":"http://42.52.173.5:4324"},{ "https":"http://221.10.104.76:4331"},{ "https":"http://111.126.93.71:4345"},{ "https":"http://116.115.208.126:4331"},{ "https":"http://114.239.3.134:4345"},{ "https":"http://112.194.90.123:4345"},{ "https":"http://140.237.128.112:4316"},{ "https":"http://114.250.173.180:4331"},{ "https":"http://121.226.155.3:4364"},{ "https":"http://49.85.74.3:4336"},{ "https":"http://222.77.214.122:4370"},{ "https":"http://223.242.229.21:4312"},{ "https":"http://117.24.81.48:4373"},{ "https":"http://27.153.143.105:4345"},{ "https":"http://115.208.43.250:4380"},{ "https":"http://121.57.85.123:4345"},{ "https":"http://114.99.222.117:4316"},{ "https":"http://114.239.123.40:4331"},{ "https":"http://27.150.85.215:4345"},{ "https":"http://113.237.246.188:4331"},{ "https":"http://115.209.125.138:4356"},{ "https":"http://121.231.214.199:4375"},{ "https":"http://124.94.189.78:4324"},{ "https":"http://114.106.137.26:4358"},{ "https":"http://175.165.231.105:4310"},{ "https":"http://121.234.172.115:4345"},{ "https":"http://218.86.19.198:4342"},{ "https":"http://125.111.147.46:4345"},{ "https":"http://220.179.210.64:4327"},{ "https":"http://175.166.90.74:4378"},{ "https":"http://60.20.97.167:4378"},{ "https":"http://140.237.30.86:4345"},{ "https":"http://36.25.226.76:4378"},{ "https":"http://117.70.41.81:4358"},{ "https":"http://123.180.208.173:4331"},{ "https":"http://121.205.223.93:4345"},{ "https":"http://223.242.12.226:4335"},{ "https":"http://113.239.157.229:4378"},{ "https":"http://125.44.66.133:4345"},{ "https":"http://36.6.69.219:4312"},{ "https":"http://27.159.187.18:4331"},{ "https":"http://222.213.136.204:4367"},{ "https":"http://113.141.222.74:4346"},{ "https":"http://113.243.32.109:4343"},{ "https":"http://27.155.220.94:4345"},{ "https":"http://117.63.135.240:4375"},{ "https":"http://49.89.151.161:4345"},{ "https":"http://119.112.80.165:4320"},{ "https":"http://120.38.241.184:4312"},{ "https":"http://59.58.47.5:4363"},{ "https":"http://117.63.133.158:4376"},{ "https":"http://117.95.176.57:4364"},{ "https":"http://49.70.123.247:4331"},{ "https":"http://27.150.40.240:4345"},{ "https":"http://116.22.49.165:4331"},{ "https":"http://119.7.146.195:4378"},{ "https":"http://110.82.250.18:4313"},{ "https":"http://119.5.179.160:4358"},{ "https":"http://124.94.189.149:4360"},{ "https":"http://175.149.61.254:4375"},{ "https":"http://60.31.89.45:4331"},{ "https":"http://117.26.130.46:4313"},{ "https":"http://36.6.68.122:4345"},{ "https":"http://113.218.242.210:4331"},{ "https":"http://123.245.248.158:4331"},{ "https":"http://175.167.20.24:4313"},{ "https":"http://36.6.147.247:4345"},{ "https":"http://49.89.141.119:4345"},{ "https":"http://42.55.180.94:4385"},{ "https":"http://117.95.198.147:4364"},{ "https":"http://175.147.117.31:4313"},{ "https":"http://175.155.141.157:4356"},{ "https":"http://180.105.204.56:4360"},{ "https":"http://49.86.9.51:4312"},{ "https":"http://114.99.12.45:4378"},{ "https":"http://117.86.191.166:4331"},{ "https":"http://218.60.253.251:4324"},{ "https":"http://182.147.82.243:4356"},{ "https":"http://61.132.170.105:4327"},{ "https":"http://42.86.81.177:4324"},{ "https":"http://125.78.227.159:4370"},{ "https":"http://118.124.59.20:4326"},{ "https":"http://121.207.93.74:4346"},{ "https":"http://27.190.0.230:4341"},{ "https":"http://139.203.22.187:4313"},{ "https":"http://117.63.136.216:4376"},{ "https":"http://175.170.44.32:4331"},{ "https":"http://112.192.179.160:4358"},{ "https":"http://116.115.210.210:4331"},{ "https":"http://60.185.205.252:4326"},{ "https":"http://114.250.169.240:4331"},{ "https":"http://124.94.252.234:4331"},{ "https":"http://121.230.167.107:4336"},{ "https":"http://27.150.41.33:4312"},{ "https":"http://117.29.228.191:4354"},{ "https":"http://121.233.226.23:4312"},{ "https":"http://120.42.132.114:4346"},{ "https":"http://121.205.223.140:4345"},{ "https":"http://175.146.98.35:4331"},{ "https":"http://175.165.229.48:4330"},{ "https":"http://114.99.23.212:4331"},{ "https":"http://27.153.140.49:4345"},{ "https":"http://117.28.40.226:4352"},{ "https":"http://115.209.110.102:4326"},{ "https":"http://221.226.193.68:4354"},{ "https":"http://125.111.151.65:4345"},{ "https":"http://117.29.229.229:4345"},{ "https":"http://175.146.213.244:4356"},{ "https":"http://114.106.156.58:4358"},{ "https":"http://113.237.0.40:4356"},{ "https":"http://60.20.199.39:4331"},{ "https":"http://111.163.84.240:4331"},{ "https":"http://121.231.215.88:4375"},{ "https":"http://125.125.25.47:4380"},{ "https":"http://113.226.111.175:4320"},{ "https":"http://36.62.216.242:4312"},{ "https":"http://1.197.34.161:4331"},{ "https":"http://117.95.195.207:4331"},{ "https":"http://49.89.151.127:4345"},{ "https":"http://118.118.203.47:4320"},{ "https":"http://58.46.249.6:4331"},{ "https":"http://59.60.129.68:4332"},{ "https":"http://114.231.186.227:4331"},{ "https":"http://117.95.201.0:4345"},{ "https":"http://27.153.140.251:4345"},{ "https":"http://120.42.132.56:4345"},{ "https":"http://117.26.131.105:4331"},{ "https":"http://183.0.214.200:4331"},{ "https":"http://1.199.40.101:4356"},{ "https":"http://117.60.37.119:4321"},{ "https":"http://118.123.41.195:4331"},{ "https":"http://220.179.210.228:4325"},{ "https":"http://183.156.16.166:4386"},{ "https":"http://110.187.20.36:4345"},{ "https":"http://220.179.211.215:4378"},{ "https":"http://115.209.50.83:4356"},{ "https":"http://182.204.157.139:4310"},{ "https":"http://140.237.158.9:4345"},{ "https":"http://42.56.2.78:4360"},{ "https":"http://171.12.165.206:4356"},{ "https":"http://27.150.85.165:4345"},{ "https":"http://114.99.223.220:4345"},{ "https":"http://42.178.145.229:4356"},{ "https":"http://121.206.253.120:4316"},{ "https":"http://115.59.152.7:4310"},{ "https":"http://110.90.137.87:4313"},{ "https":"http://123.172.180.71:4345"},{ "https":"http://125.78.226.82:4306"},{ "https":"http://222.90.42.140:4345"},{ "https":"http://113.141.222.49:4331"},{ "https":"http://116.115.210.219:4331"},{ "https":"http://61.190.160.118:4331"},{ "https":"http://121.206.9.208:4313"},{ "https":"http://110.81.248.150:4352"},{ "https":"http://182.34.101.124:4334"},{ "https":"http://121.230.166.40:4357"},{ "https":"http://119.5.189.221:4358"},{ "https":"http://223.215.177.237:4354"},{ "https":"http://59.58.19.49:4363"},{ "https":"http://59.59.213.16:4345"},{ "https":"http://114.239.125.41:4331"},{ "https":"http://114.230.125.37:4312"},{ "https":"http://119.112.84.39:4315"},{ "https":"http://125.111.147.145:4345"},{ "https":"http://113.237.228.28:4324"},{ "https":"http://27.150.193.197:4345"},{ "https":"http://114.250.167.141:4331"},{ "https":"http://119.5.184.140:4358"},{ "https":"http://49.83.171.198:4332"},{ "https":"http://121.207.92.73:4345"},{ "https":"http://119.120.76.238:4346"},{ "https":"http://118.118.202.153:4310"},{ "https":"http://171.13.118.84:4331"},{ "https":"http://113.237.3.219:4356"},{ "https":"http://36.6.69.234:4312"},{ "https":"http://183.165.195.47:4317"},{ "https":"http://114.233.178.199:4385"},{ "https":"http://42.56.41.254:4340"},{ "https":"http://182.34.21.244:4334"},{ "https":"http://140.237.13.146:4345"},{ "https":"http://183.143.98.235:4345"}
     ]
    proxies = random.choice(proxiess)
    html=""
    # print(link)
    try: #使用try except方法进行各种异常处理
        res = requests.get(url.replace('$1',str(pn)),headers=headers,timeout=20,verify=False,proxies=proxies) #读取网页源码
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
# url='http://zjj.sjz.gov.cn:8081/plus/scxx_ysxk.php?pageno=$1&ysskzh=2021&'
# pn=13
# total=0
# while True :
#     pn+=1
#     time.sleep(5)
#     html=sjz_getHtml(url.replace('$1',str(pn)))
#     html=html.replace('<table width="100%" border="0" cellspacing="1" cellpadding="0" style="margin-bottom:15px;">','</table><table width="100%" border="0" cellspacing="1" cellpadding="0" style="margin-bottom:15px;">')
#     if total==0:
#         total=int(re.findall('符合您的查询条件的<span>(\d+)</span>条记录</h2>',html,re.S)[0])
#     tab1 = etree.HTML(html)
#     hrefs=tab1.xpath('//div[@class="scxx_jieguo"]/table')
#     #########
#     print('page=',pn,len(hrefs))
#     if len(hrefs)==0:
#         break   
#     for j in hrefs:
#         sp=''.join(j.xpath('./tr[1]/td[1]/text()')).strip()
#         ysz=''.join(j.xpath('./tr[1]/td[2]/text()')).strip().replace('&nbsp;','')
#         addr=''.join(j.xpath('./tr[2]/td[1]/text()')).strip()
#         pro=''.join(j.xpath('./tr[3]/td[1]/text()')).strip()
#         kprq=''.join(j.xpath('./tr[3]/td[2]/text()')).strip()
#         u=''.join(j.xpath('./tr[1]/td[2]/a/@href'))
#         if len(u)<3:
#             continue
#         contenturl='http://zjj.sjz.gov.cn:8081'+u
#         print(pro,addr,sp,ysz,kprq)
sjz_issue_code = con.query("select case when max(issue_code)<'2022001' then '2022000' else max(issue_code) end issue_code from odsdb.ods_city_issue_code where gd_city = '石家庄市' and issue_date > '2021-12-31' ")
url='http://zjj.sjz.gov.cn:8081/plus/scxx_ysxk.php?pageno=1&ysskzh=$1&'
pn=0
ysskzh=int(sjz_issue_code.iloc[0].at['issue_code'])
print(ysskzh)
ysskzh_end = ysskzh+10
total=0
while ysskzh < ysskzh_end :
# while ysskzh < 2021246 :
    ysskzh+=1
    # time.sleep(5)
    html = sjz_getHtml(url.replace('$1',str(ysskzh))) #读取网页源码
    html=html.replace('<table width="100%" border="0" cellspacing="1" cellpadding="0" style="margin-bottom:15px;">','</table><table width="100%" border="0" cellspacing="1" cellpadding="0" style="margin-bottom:15px;">')
    if total==0:
        total=int(re.findall('符合您的查询条件的<span>(\d+)</span>条记录</h2>',html,re.S)[0])
    tab1 = etree.HTML(html)
    hrefs=tab1.xpath('//div[@class="scxx_jieguo"]/table')
    if len(hrefs)==0:
        continue   
    for j in hrefs:
        business=''.join(j.xpath('./tr[1]/td[1]/text()')).strip()
        issue_code=''.join(j.xpath('./tr[1]/td[2]/text()')).strip().replace('&nbsp;','')
        address=''.join(j.xpath('./tr[2]/td[1]/text()')).strip()
        newest_name=''.join(j.xpath('./tr[3]/td[1]/text()')).strip()
        issue_date=''.join(j.xpath('./tr[3]/td[2]/text()')).strip()
        issue_date = issue_date[0:4]+'-'+issue_date[4:6]+'-'+issue_date[6:8]
        building_code = ''.join(j.xpath('./tr[4]/td[1]/text()')).strip()
        u=''.join(j.xpath('./tr[1]/td[2]/a/@href'))
        issue_region = is_nullto_value(address.replace('河北省石家庄市','').replace('石家庄市','').replace('河北省','')[0:3])
        room_sum = 0
        issue_area = 0
        if len(u)<3:
            continue
        contenturl='http://zjj.sjz.gov.cn:8081'+u
        print(issue_date,issue_code,address,business,newest_name,building_code,issue_region)
        city_name='石家庄市'
        #contenturl+'\t'+pro+'\t'+addr+'\t'+sp+'\t'+ysz+'\t'+kprq+'\n'
        print(contenturl)
        try:
            #pro
            proid=re.findall('\?id=(.*?)&',contenturl)[0]
            yszid=re.findall('nodeid=(.*?)$',contenturl)[0]
            u2 = sjz_getHtml('http://zjj.sjz.gov.cn:8081/plus/cxda_ys_json_menu.php?id='+proid)
            js=json.loads(u2)

            for j in js:
                #ysz check
                id=j['id']
                #非此预售证过滤
                if id!=yszid:
                    continue
                #ld
                if j.get('children')==None:
                    continue
                children=j['children']
                if len(children)==0:
                    continue
                for ld in children:
                    u3 = sjz_getHtml('http://zjj.sjz.gov.cn:8081/plus/'+ld['attributes']['url'])
                    ldmc=ld['text']
                    housing_type=''.join(re.findall('>楼房用途</td>.*?>(.*?)<',u3,re.S)).strip()
                    if housing_type.find('商业')!=-1 or housing_type.find('地下')!=-1 or housing_type.find('车库')!=-1:
                        continue
                    taoshu=''.join(re.findall('>总套数</td>.*?>(.*?)<',u3,re.S)).strip()
                    room_sum = int(taoshu) + room_sum
                    mianji=''.join(re.findall('>建筑面积</td>.*?>(.*?)<',u3,re.S)).strip()
                    issue_area=int(float(mianji)) + issue_area
                    jzlh = ''.join(re.findall('>楼号</td>.*?>(.*?)<',u3,re.S)).strip()
            print('项目名称:  '+newest_name+'      '+'开发商:  '+business+'      '+'预售证号:  '+str(issue_code)+'      '+'楼盘地:  '+address+'      '+'楼盘区县:  '+str(issue_region)+'      '+'预售面积:  '+str(issue_area)+'      '+'预售套数: ' +str(room_sum)+'      '+'发证时间:  '+str(issue_date)+'      '+'建筑编号:  '+str(building_code)+'      '+'城市名称:  '+city_name)
            new=pd.DataFrame({'url':contenturl,'region':issue_region,'gd_city':city_name,'floor_name':newest_name,'address':address,'business':business,'issue_code':issue_code,'issue_date':issue_date,'issue_area':issue_area,'building_code':building_code,'room_sum':room_sum},index=[0])
            df=df.append(new,ignore_index=True) 
            df = df.drop_duplicates(ignore_index=True)
            # time.sleep(2)
        except Exception as e:
            print(e)
df = df.groupby(by=['region','gd_city','floor_name','address','business','issue_code','issue_date']).agg({'room_sum': sum, 'issue_area': sum,'url':list,'building_code':list}).reset_index()
df['building_code']=df['building_code'].apply(lambda x:str(x).replace('[','').replace(']','').replace('、','\',\''))
df['url']=df['url'].apply(lambda x:str(x).replace('[','').replace(']',''))
df['region'].at[df['region'] == '井陉矿'] = '井陉矿区'
df['region'].at[~df['region'].isin(['长安区','桥西区','新华区','井陉矿区','裕华区','藁城区','鹿泉区','栾城区','井陉县','正定县','行唐县','灵寿县','高邑县','深泽县','赞皇县','无极县','平山县','元氏县','赵县','辛集市','晋州市','新乐市'])] = np.nan




#In[]
###保定---涿州  还不确定网站数据的更新时间
for i in range(1,26):
    dat = {
        "call": "action",
        "code": "0113",
        "pageIndex": i,
        "pageSize": "20"
    }
    url = 'http://www.zzdcxh.com/ListHandler.ashx?call=action&code=0113&pageIndex='+str(i)+'&pageSize=20'  #第一页网址网址
    resq = requests.post(url,headers=headers,data=dat,timeout=20,verify=False) #读取网页源码
    json_data = json.loads((resq.text).replace('\'','"'))
    tree = etree.HTML(json_data['detail'].replace("%", "\\").encode('utf-8').decode('unicode_escape'))#转未html
    trs = tree.xpath("/html/body/table/tbody/tr")
    for tr in trs:
        if len(tr.xpath('./td[2]/a/text()'))!=0:
            newest_name = tr.xpath('./td[2]/a/text()')[0]
            issue_code = tr.xpath('./td[4]/text()')[0]
            issue_location = tr.xpath('./td[3]/a/text()')[0]
            issue_region = '涿州市'
            day_time = tr.xpath('./td[4]/text()')[0]
            permitLook_code = re.sub('\D','',tr.xpath('./td[2]/a/@onclick')[0])
            child_dat = {
            "call": "permitLook",
            "code": permitLook_code,
            "pageSize": "0"
            }
            chil_url = 'http://www.zzdcxh.com/ListHandler.ashx?call=permitLook&code='+str(permitLook_code)+'&pageSize=0'  #第一页网址网址
            chil_resq = requests.post(chil_url,headers=headers,data=child_dat,timeout=20,verify=False) #读取网页源码
            chil_json_data = json.loads((chil_resq.text).replace('\'','"'))
            chil_tree = etree.HTML(chil_json_data['detail'].replace("%", "\\").encode('utf-8').decode('unicode_escape'))#转未html
            chil_tbody = chil_tree.xpath("/html/body/table/tbody")[0]
            # for chil_tbody in chil_tbodys:
            busniss = chil_tbody.xpath("./tr[2]/td[2]/text()")[0] 
            issue_area = chil_tbody.xpath("./tr[6]/td[2]/text()")[0] 
            issue_room_num = chil_tbody.xpath("./tr[7]/td[2]/text()")[0] 
            issue_start_date = chil_tbody.xpath("./tr[9]/td[2]/text()")[0].split('/')[0]+'-'+str(chil_tbody.xpath("./tr[9]/td[2]/text()")[0].split('/')[1].rjust(2,'0'))+'-'+str(chil_tbody.xpath("./tr[9]/td[2]/text()")[0].split('/')[2].rjust(2,'0'))
            if len(chil_tbody.xpath("./tr[10]/td[2]/text()")) != 0:
                issue_building_code = chil_tbody.xpath("./tr[10]/td[2]/text()")[0] 
            else : 
                issue_building_code = newest_name
            city_name = "保定市"
            print('项目名称:  '+newest_name+'      '+'开发商:  '+busniss+'      '+'预售证号:  '+str(issue_code)+'      '+'楼盘地:  '+issue_location+'      '+'楼盘区县:  '+str(issue_region)+'      '+'预售面积:  '+str(issue_area)+'      '+'预售套数: ' +str(issue_room_num)+'      '+'发证时间:  '+str(issue_start_date)+'      '+'建筑编号:  '+str(issue_building_code)+'      '+'城市名称:  '+city_name)
            new=pd.DataFrame({'url':chil_url,'region':issue_region,'gd_city':city_name,'floor_name':newest_name,'address':issue_location,'business':busniss,'issue_code':issue_code,'issue_date':issue_start_date,'issue_area':issue_area,'building_code':issue_building_code,'room_sum':issue_room_num},index=[0])
            df=df.append(new,ignore_index=True) 
        time.sleep(5)
        # print(href)
        chil_resq.close()
resq.close()














#++++++++++++++++++++++++++++++++++++++++++++++++++=======下为日批运行=========+++++++++++++++++++++++++++++++++++++++++++++++++++++


#In[]
###三亚

url = "http://www.fcxx0898.com/syfcSiteApi//Presale/ListPresale"
for i in range(1,2):
    # dat = {
    #     "KeyWord": "",  
    #     "PageIndex": i, 
    #     "PageSize": 15
    # }
    # resq = requests.post(url,headers=headers,data=dat)
    # json_data = json.loads(resq.text)
    # pageList = json_data['Data']['pageList']
    # for item in range(0,len(pageList)):
    #         ID = json_data['Data']['pageList'][item]['ID']
    #         chil_url = 'http://www.fcxx0898.com/syfcSiteApi//Presale/GetPerSale'
    #         chil_dat = {"ID": ID}
    #         chil_resq = requests.post(chil_url,headers=headers,data=chil_dat)
    #         chil_json_data = json.loads(chil_resq.text)
    #         chil_json_data = chil_json_data['Data']
    #         # if chil_json_data['RegisterDate'][0:10] == '2021-11-29' :
    #         if chil_json_data['RegisterDate'][0:10] == day :  ## 获取当天登记的数据
    #         # if chil_json_data['StartDate'] >= date :  ## 获取指定时间之前的数据
    #             newest_name = json_data['Data']['pageList'][item]['Name']
    #             busniss = json_data['Data']['pageList'][item]['Enterprise']
    #             issue_code = chil_json_data['PresaleCert']
    #             issue_location = chil_json_data['Location']
    #             issue_region = is_null_to_value(chil_json_data['Location']).replace('海南省三亚市','').replace('三亚市','').replace('海南省','')[0:3]
    #             issue_area = str(chil_json_data['TotalArea'])
    #             issue_room_num = str(chil_json_data['RoomCount'])
    #             issue_start_date = chil_json_data['StartDate'][0:10]
    #             issue_building_code = chil_json_data['Content']
    #             city_name = "三亚市"
    #             print(chil_json_data)
    #             new=pd.DataFrame({'url':"http://www.fcxx0898.com/syfcSiteWeb/Pages/Project/PresaleInfo.aspx?id="+str(ID),'region':issue_region,'gd_city':city_name,'floor_name':newest_name,'address':issue_location,'business':busniss,'issue_code':issue_code,'issue_date':issue_start_date,'issue_area':issue_area,'building_code':issue_building_code,'room_sum':issue_room_num},index=[0])
    #             df=df.append(new,ignore_index=True) 
    #         else:
    #             print("从网址的第"+str(i)+"页获取 :" + '2021-11-12' +" 登记的数据失败")
    #             break 
    try:
        dat = {
            "KeyWord": "", 
            "PageIndex": i, 
            "PageSize": 15
        }

        resq = requests.post(url,headers=headers,data=dat)
        json_data = json.loads(resq.text)
        pageList = json_data['Data']['pageList']

        for item in range(0,len(pageList)):
            ID = json_data['Data']['pageList'][item]['ID']
            chil_url = 'http://www.fcxx0898.com/syfcSiteApi//Presale/GetPerSale'
            chil_dat = {"ID": ID}
            chil_resq = requests.post(chil_url,headers=headers,data=chil_dat)
            chil_json_data = json.loads(chil_resq.text)
            chil_json_data = chil_json_data['Data']
            # if chil_json_data['RegisterDate'][0:10] == '2021-11-29' :
            # if chil_json_data['RegisterDate'][0:10] == (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d') :  ## 获取当天登记的数据
            if chil_json_data['RegisterDate'][0:10] >= date[0:10] :  ## 获取指定时间之前的数据
                newest_name = json_data['Data']['pageList'][item]['Name']
                busniss = json_data['Data']['pageList'][item]['Enterprise']
                issue_code = chil_json_data['PresaleCert']
                issue_location = chil_json_data['Location']
                issue_region = is_nullto_value(chil_json_data['Location']).replace('海南省三亚市','').replace('三亚市','').replace('海南省','')[0:3]
                issue_area = str(chil_json_data['TotalArea'])
                issue_room_num = str(chil_json_data['RoomCount'])
                issue_start_date = chil_json_data['StartDate'][0:10]
                issue_building_code = chil_json_data['Content']
                city_name = "三亚市"
                print(chil_json_data)
                new=pd.DataFrame({'url':"http://www.fcxx0898.com/syfcSiteWeb/Pages/Project/PresaleInfo.aspx?id="+str(ID),'region':issue_region,'gd_city':city_name,'floor_name':newest_name,'address':issue_location,'business':busniss,'issue_code':issue_code,'issue_date':issue_start_date,'issue_area':issue_area,'building_code':issue_building_code,'room_sum':issue_room_num},index=[0])
                df=df.append(new,ignore_index=True) 
            else:
                print("从网址的第"+str(i)+"页获取 :" + (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d') +" 登记的数据失败")
                break 
            chil_resq.close()
    except requests.exceptions.RequestException as e:
        print(e)
        print("报异常:跳过")
        pass
resq.close()


df.at[df['region'].isin(['三亚崖','海榆西','红塘湾']),'region'] = '崖州区'
df.at[df['region'].isin(['三亚海','南田温','国营南','崖州湾','海棠北','海棠湾']),'region'] = '海棠区'
df.at[df['region'].isin(['三亚中','三亚凤','南新横','吉阳镇','新风街','榆亚路','河东区','田独镇','荔枝沟','迎宾路','龙塘路','东岸湿','凤凰路','河东路']),'region'] = '吉阳区'
df.at[df['region'].isin(['三亚湾','园林路','海南三','海坡度','育秀路','金鸡岭']),'region'] = '天涯区'
df.at[df['region'].isin(['三亚湾','园林路','海南三','海坡度','育秀路','金鸡岭']),'region'] = '天涯区'



#In[]

def is_null_to_value(value_str):
    if value_str is None :
        return np.nan
    elif len(value_str) == 0:
        return np.nan
    else:
        return value_str
## 三亚
for i in range(1,2):

        dat = {
            "KeyWord": "", 
            "PageIndex": i, 
            "PageSize": 15
        }

        resq = requests.post(url,headers=headers,data=dat)
        json_data = json.loads(resq.text)
#        tree = etree.HTML(resq) #转未html
        pageList = json_data['Data']['pageList']
        print(pageList)
        for item in range(0,len(pageList)):
                ID = json_data['Data']['pageList'][item]['ID']
                chil_url = 'http://www.fcxx0898.com/syfcSiteApi//Presale/GetPerSale'
                chil_dat = {"ID": ID}
                chil_resq = requests.post(chil_url,headers=headers,data=chil_dat)
                chil_json_data = json.loads(chil_resq.text)
                chil_json_data = chil_json_data['Data']
                # if chil_json_data['RegisterDate'][0:10] == '2021-11-29' :
                if chil_json_data['RegisterDate'][0:10] == (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d') :  ## 获取当天登记的数据
                # if chil_json_data['StartDate'] >= date :  ## 获取指定时间之前的数据
                    newest_name = json_data['Data']['pageList'][item]['Name']
                    busniss = json_data['Data']['pageList'][item]['Enterprise']
                    issue_code = chil_json_data['PresaleCert']
                    issue_location = chil_json_data['Location']
                    issue_region = is_null_to_value(chil_json_data['Location']).replace('海南省三亚市','').replace('三亚市','').replace('海南省','')[0:3]
                    issue_area = str(chil_json_data['TotalArea'])
                    issue_room_num = str(chil_json_data['RoomCount'])
                    issue_start_date = chil_json_data['StartDate'][0:10]
                    issue_building_code = chil_json_data['Content']
                    city_name = "三亚市"
                    print('SY - 当前数据' + chil_json_data)
                    new=pd.DataFrame({'url':"http://www.fcxx0898.com/syfcSiteWeb/Pages/Project/PresaleInfo.aspx?id="+str(ID),'region':issue_region,'gd_city':city_name,'floor_name':newest_name,'address':issue_location,'business':busniss,'issue_code':issue_code,'issue_date':issue_start_date,'issue_area':issue_area,'building_code':issue_building_code,'room_sum':issue_room_num},index=[0])
                    df=df.append(new,ignore_index=True) 
                else:
                    print(">                               从网址的第"+str(i)+"页获取 :" + (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d') +" 登记的数据失败")
                    break 
                chil_resq.close()
        print(e)
        print("报异常:跳过")
        pass
        resq.close()

df.at[df['region'].isin(['三亚崖','海榆西','红塘湾']),'region'] = '崖州区'
df.at[df['region'].isin(['三亚海','南田温','国营南','崖州湾','海棠北','海棠湾']),'region'] = '海棠区'
df.at[df['region'].isin(['三亚中','三亚凤','南新横','吉阳镇','新风街','榆亚路','河东区','田独镇','荔枝沟','迎宾路','龙塘路','东岸湿','凤凰路','河东路']),'region'] = '吉阳区'
df.at[df['region'].isin(['三亚湾','园林路','海南三','海坡度','育秀路','金鸡岭']),'region'] = '天涯区'
df.at[df['region'].isin(['三亚湾','园林路','海南三','海坡度','育秀路','金鸡岭']),'region'] = '天涯区'



#In[]
###保定---审计
url = 'http://xzspj.baoding.gov.cn/zfxxgk/009003/009003002/secondPage.html'  #第一页网址网址

# url = 'http://xzspj.baoding.gov.cn/zfxxgk/009003/009003002/2.html'  #第一页网址网址
url_master = 'http://xzspj.baoding.gov.cn' #首页
data={}
# resq = requests.get(url,headers=headers,timeout=20,verify=False) #读取网页源码
# resq.encoding='UTF-8'#解码
BD_sess = requests.Session()
BD_sess.mount('http://', HTTPAdapter(max_retries=3))  #添加http协议的重试次数
BD_sess.mount('https://', HTTPAdapter(max_retries=3)) #不想思考,两个协议都添加
resq = sess_repGet(BD_sess,url,headers,data,'text')
tree = etree.HTML(resq) #转未html
lis = tree.xpath("/html/body/div[2]/div[2]/div/div[2]/div/ul/li")#xpath获取当前页所有预售证信息
# print(len(lis))
#循环遍历获取子链接，然后从子链接获取想要的数据
for li in lis:
    href = li.xpath("./a/@href")[0]  #子链接
    day_time = li.xpath("./span/text()")[0] #数据录入时间
    issue_building_code = li.xpath("./a/text()")[0].replace('预售公告','').replace('预售公示','').replace('预售信息公示','').replace('预售信息公告','')  #建筑编号
#从子链接中获取数据
    # if day_time >= date[0:10] and li.xpath("./a/text()")[0].find("预售"):
    if day_time >= '2021-12-20' and li.xpath("./a/text()")[0].find("预售"):
        chil_url = url_master+href
        # chil_resq = requests.get(chil_url,headers=headers,timeout=20,verify=False) #读取网页源码
        # chil_resq.encoding='UTF-8'#解码
        page_content = sess_repGet(BD_sess,chil_url,headers,data,'text')
        # page_content = chil_resq.text
        #解析数据
        obj = re.compile(r'<!DOCTYPE html>.*?<table.*?'
                         r'开发企业名称.*?<td.*?<span.*?>(?P<busniss>.*?)<.*?>(?P<busniss1>.*?)<.*?'
                         r'土地位置.*?<td.*?<span.*?>(?P<issue_location>.*?)<.*?>(?P<issue_location1>.*?)<.*?'
                         r'预售项目名称.*?<td.*?<span.*?>(?P<newest_name>.*?)<.*?>(?P<newest_name1>.*?)<.*?'
                         r'预售证号.*?<td.*?<span.*?>(?P<issue_code>.*?)<.*?>(?P<issue_code_num1>.*?)<.*?>(?P<issue_code_num2>.*?)<.*?>(?P<issue_code_num3>.*?)<.*?>(?P<issue_code_num4>.*?)<.*?>(?P<issue_code_num5>.*?)<.*?>(?P<issue_code_num6>.*?)<.*?>(?P<issue_code_num7>.*?)<.*?'
                         r'预售面积.*?<td.*?<span.*?>(?P<issue_area>.*?)</span>.*?'
                         r'预售套数.*?<td.*?<span.*?>(?P<issue_room_num>.*?)</span>.*?'
                         ,re.S)
        #开始匹配
        result = obj.finditer(page_content)
        for i in result :
            issue_code = (i.group("issue_code")+str(i.group("issue_code_num1"))+str(i.group("issue_code_num2"))+str(i.group("issue_code_num3"))+str(i.group("issue_code_num4"))+str(i.group("issue_code_num5"))+str(i.group("issue_code_num6"))+str(i.group("issue_code_num7"))).replace(' ','').replace('\n','')
            issue_location = (str(i.group("issue_location"))+str(i.group("issue_location1"))).replace(' ','').replace('\n','')
            if len(re.findall(r'[\u4e00-\u9fff]+',i.group("newest_name"))) <1:
                newest_name = (str(i.group("newest_name"))+str(i.group("newest_name1"))).replace(' ','').replace('\n','')
            else:
                newest_name = i.group("newest_name")
            busniss = (str(i.group("busniss"))+str(i.group("busniss1"))).replace(' ','').replace('\n','')
            issue_area = re.sub('\D','',(i.group("issue_area")).split('.')[0])
            issue_room_num = re.sub('\D','',i.group("issue_room_num"))
            issue_start_date = day_time
            issue_region =issue_location.split('区')[0]+'区' 
            if len(issue_region)>5 :
                issue_region= np.nan
            city_name = "保定市"
            print('项目名称:  '+newest_name+'      '+'开发商:  '+busniss+'      '+'预售证号:  '+str(issue_code)+'      '+'楼盘地:  '+issue_location+'      '+'楼盘区县:  '+str(issue_region)+'      '+'预售面积:  '+str(issue_area)+'      '+'预售套数: ' +str(issue_room_num)+'      '+'发证时间:  '+str(issue_start_date)+'      '+'建筑编号:  '+str(issue_building_code)+'      '+'城市名称:  '+city_name)
            new=pd.DataFrame({'url':chil_url,'region':issue_region,'gd_city':city_name,'floor_name':newest_name,'address':issue_location,'business':busniss,'issue_code':issue_code,'issue_date':issue_start_date,'issue_area':issue_area,'building_code':issue_building_code,'room_sum':issue_room_num},index=[0])
            df=df.append(new,ignore_index=True) 
            df = df.drop_duplicates(ignore_index=True)
        time.sleep(2)
        print(li.xpath("./a/text()")[0] +'    '+ href+'      '+day_time)
        # chil_resq.close()
    else:
        print("从保定审计网址的第"+str(1)+"页获取 :" + (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d') +" 登记的数据失败")
        break 

# resq.close()





#In[]
###南通---通州第一页
url = 'http://www.tongzhou.gov.cn/tzqrmzf/spgs1/spgs1.html'
data = {}
html_data = etree.HTML(respGet(url,headers,data,'text')) #读取网页源码
lis = html_data.xpath("/html/body/div[3]/div[2]/div[3]/div/div/div[1]/ul/li")#xpath获取当前页所有预售证信息
for li in lis:
    print('NTSTZ - 当前数据' +li.xpath("./a/text()")[0] + '    ' + li.xpath("./a/@href")[0] + '    ' + li.xpath("./span/text()")[0])
    if (li.xpath("./a/text()")[0]).find("南通市通州区商品房预售许可证公布") != -1 :
        href = li.xpath("./a/@href")[0]
        chil_data = respGet('http://www.tongzhou.gov.cn/'+href,headers,data,'text') #读取网页源码
        obj = re.compile(r'预售号码：(?P<issue_code>.*?)<.*?'
                         r'售房单位：(?P<busniss>.*?)<.*?'
                         r'项目名称：(?P<newest_name>.*?)<.*?'
                         r'房屋坐落：(?P<issue_location>.*?)<.*?'
                         r'房屋用途性质：(?P<house_type>.*?)<.*?'
                         r'预售总建筑面积：(?P<issue_area>.*?)平方.*?'
                         r'发证日期：(?P<issue_start_date>.*?)日.*?'
                         ,re.S)
        result = obj.finditer(chil_data)
        for i in result :
            if i.group("house_type").find("住") != -1:
                issue_code = i.group("issue_code")
                issue_location = i.group("issue_location")
                newest_name = i.group("newest_name")
                newest_name = newest_name.split('1')[0].split('2')[0].split('3')[0].split('4')[0].split('5')[0].split('6')[0].split('7')[0].split('8')[0].split('9')[0].split('A')[0].split('B')[0].split('C')[0].split('D')[0]
                busniss = i.group("busniss")
                issue_area = i.group("issue_area")
                issue_room_num = int(int(float(issue_area))/130)
                issue_start_date = i.group("issue_start_date")
                issue_start_date = re.findall(r'\d+',issue_start_date )[0]+'-'+re.findall(r'\d+',issue_start_date )[1].rjust(2,'0') +'-'+re.findall(r'\d+',issue_start_date )[2].rjust(2,'0')
                issue_building_code = i.group("newest_name")
                issue_region = "通州区"
                city_name = "南通市"
                print('项目名称:  '+newest_name+'      '+'开发商:  '+busniss+'      '+'预售证号:  '+str(issue_code)+'      '+'楼盘地:  '+issue_location+'      '+'楼盘区县:  '+str(issue_region)+'      '+'预售面积:  '+str(issue_area)+'      '+'预售套数: ' +str(issue_room_num)+'      '+'发证时间:  '+str(issue_start_date)+'      '+'建筑编号:  '+str(issue_building_code)+'      '+'城市名称:  '+city_name)
        if issue_start_date >= date[0:10] :  ## 获取指定时间之前的数据
            new=pd.DataFrame({'url':'http://www.tongzhou.gov.cn/'+href,'region':issue_region,'gd_city':city_name,'floor_name':newest_name,'address':issue_location,'business':busniss,'issue_code':issue_code,'issue_date':issue_start_date,'issue_area':issue_area,'building_code':issue_building_code,'room_sum':issue_room_num},index=[0])
            df=df.append(new,ignore_index=True) 
        else:
            print("从通州市网址的第"+str(1)+"页获取 :" + (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d') +" 登记的数据失败")
            break 
    else:
        print("从通州市网址的第"+str(1)+"页获取 :" +"南通市通州区商品房预售许可证公布 登记的数据失败")
        break 



#In[]
###南通---通州除外   
NTstart_date_time = issue_ccode['issue_date'][(issue_ccode['gd_city']=='南通市')].values[0]  # 获取爬虫开始时间
NTissue_ccode_list = issue_ccode['issue_code'][(issue_ccode['gd_city']=='南通市')].values[0]  # 获取预售证号列表
NT_sess = requests.Session()
NT_sess.mount('http://', HTTPAdapter(max_retries=3))  #添加http协议的重试次数
NT_sess.mount('https://', HTTPAdapter(max_retries=3)) #不想思考,两个协议都添加

headers_nt = {
    'Host': 'www.ntfcjy.com:9999',
    'Authorization': 'bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyTmljayI6IuW-ruS_oeeUqOaItyIsInVzZXJfbmFtZSI6IuW-ruS_oeeUqOaItyIsInNjb3BlIjpbInJlYWQiXSwidXNlclBob25lIjpudWxsLCJ1c2VyVGVsIjoiMCIsInN5c1VzZXJJZCI6MjAwNDIxMCwiZXhwIjoxNjY2ODU5NTM3LCJ1c2VySWQiOjIwOTUwLCJqdGkiOiIxZjAzMDE2Mi0wM2MxLTQ4MmUtODc3MC03NGMxZDJhNGZjODAiLCJjbGllbnRfaWQiOiJXWF9DTElFTlQifQ.SM7ab-gMx5Azvm1Dt_OKN-0ebq6Tupf7Xs2pHn6g8iwDoccNfpx90xFfh_dXQC5Z35XBcjeMYMBYnjGKwB96mXkTwbZmWHr-yIVGxDsgSj5CoYoUSleSj5opkKjtGx-O6dxoLEm5EYoOFcjkTrjOB7FiefD4wvhwTpFoBjD3t6Y8fwen0lnuGKThu5m6g2DEq5WgXspclTaPVu53jMG1HaKN57z7G0JzJj-gEvdikcaWQlFs40wKnsBU0bx6KOakiuyxcq7FZ60FXjSWhjPdvbDCZ4x0-JaDuZAWGur9ymjIzb_VnAJgzyNbKmoMJZY-CfLhtOOzcUhPGozowRN0Aw',
    'content-type': 'application/json',
    'client-type': 'WX',
    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.16(0x1800103f) NetType/WIFI Language/zh_CN',
    'Referer': 'https://servicewechat.com/wxa8947deb2350bc19/90/page-frame.html',
}
data = '{"approveName":"", \
         "applyNo":"", \
         "kfqyQymc":"", \
         "licensePernit":"", \
         "queryStartDate":"", \
         "queryEndDate":"", \
         "adminLocations":[], \
         "pages":"1", \
         "rows":15}'

jsondata = sess_repPost(NT_sess,'https://www.ntfcjy.com:9999/ys_ntzjjg/wx/record/queryLicenseList',headers_nt,data,'json')
#####转换为DtaFrame
floor_info = pd.json_normalize(jsondata['licensInfos'])
time.sleep(5)
for index, row in floor_info.iterrows():
    print(row['approveName']+'         '+ row['crtDate']) # 输出每行的项目名称
    # if row['crtDate'] > '2022-01-21' :  ## 获取指定时间之前的数据
    if (row['crtDate'] > NTstart_date_time) or (row['licensePernit'] not in NTissue_ccode_list):   ##2022-02-10 修改为大于读取的数据库时间和不存在与数据库的预售证
        pro_headers = {
            'Host': 'www.ntfcjy.com:9999',
            'Authorization': 'bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyTmljayI6IuW-ruS_oeeUqOaItyIsInVzZXJfbmFtZSI6IuW-ruS_oeeUqOaItyIsInNjb3BlIjpbInJlYWQiXSwidXNlclBob25lIjpudWxsLCJ1c2VyVGVsIjoiMCIsInN5c1VzZXJJZCI6MjAwNDIxMCwiZXhwIjoxNjY2ODU5NTM3LCJ1c2VySWQiOjIwOTUwLCJqdGkiOiIxZjAzMDE2Mi0wM2MxLTQ4MmUtODc3MC03NGMxZDJhNGZjODAiLCJjbGllbnRfaWQiOiJXWF9DTElFTlQifQ.SM7ab-gMx5Azvm1Dt_OKN-0ebq6Tupf7Xs2pHn6g8iwDoccNfpx90xFfh_dXQC5Z35XBcjeMYMBYnjGKwB96mXkTwbZmWHr-yIVGxDsgSj5CoYoUSleSj5opkKjtGx-O6dxoLEm5EYoOFcjkTrjOB7FiefD4wvhwTpFoBjD3t6Y8fwen0lnuGKThu5m6g2DEq5WgXspclTaPVu53jMG1HaKN57z7G0JzJj-gEvdikcaWQlFs40wKnsBU0bx6KOakiuyxcq7FZ60FXjSWhjPdvbDCZ4x0-JaDuZAWGur9ymjIzb_VnAJgzyNbKmoMJZY-CfLhtOOzcUhPGozowRN0Aw',
            'content-type': 'application/json',
            'client-type': 'WX',
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.16(0x1800103f) NetType/WIFI Language/zh_CN',
            'Referer': 'https://servicewechat.com/wxa8947deb2350bc19/90/page-frame.html',
        }
        pro_data = '{"approveName":"'+row['approveName']+'","pages":"1","rows":"15"}'
        pro_jsdata = sess_repPost(NT_sess,'https://www.ntfcjy.com:9999/ys_ntzjjg/wx/record/queryLandInfoList',pro_headers,pro_data.encode("utf-8"),'json')
        pro_df = pd.json_normalize(pro_jsdata)
        time.sleep(10)
        issue_headers = {
            'Host': 'www.ntfcjy.com:9999',
            'Authorization': 'bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyTmljayI6IuW-ruS_oeeUqOaItyIsInVzZXJfbmFtZSI6IuW-ruS_oeeUqOaItyIsInNjb3BlIjpbInJlYWQiXSwidXNlclBob25lIjpudWxsLCJ1c2VyVGVsIjoiMCIsInN5c1VzZXJJZCI6MjAwNDIxMCwiZXhwIjoxNjY2ODU5NTM3LCJ1c2VySWQiOjIwOTUwLCJqdGkiOiIxZjAzMDE2Mi0wM2MxLTQ4MmUtODc3MC03NGMxZDJhNGZjODAiLCJjbGllbnRfaWQiOiJXWF9DTElFTlQifQ.SM7ab-gMx5Azvm1Dt_OKN-0ebq6Tupf7Xs2pHn6g8iwDoccNfpx90xFfh_dXQC5Z35XBcjeMYMBYnjGKwB96mXkTwbZmWHr-yIVGxDsgSj5CoYoUSleSj5opkKjtGx-O6dxoLEm5EYoOFcjkTrjOB7FiefD4wvhwTpFoBjD3t6Y8fwen0lnuGKThu5m6g2DEq5WgXspclTaPVu53jMG1HaKN57z7G0JzJj-gEvdikcaWQlFs40wKnsBU0bx6KOakiuyxcq7FZ60FXjSWhjPdvbDCZ4x0-JaDuZAWGur9ymjIzb_VnAJgzyNbKmoMJZY-CfLhtOOzcUhPGozowRN0Aw',
            'content-type': 'application/json',
            'client-type': 'WX',
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.16(0x1800103f) NetType/WIFI Language/zh_CN',
            'Referer': 'https://servicewechat.com/wxa8947deb2350bc19/90/page-frame.html',
        }
        for index, pro_row in pro_df.iterrows(): 
            if pro_df.iloc[0].at['tudiId'] == None :
                issue_data = '{"tudiId":"","approveName": "'+row['approveName']+'","applyNo": "'+row['applyNo']+'","pages": "'+'1'+'","rows": "'+'5'+'"}'
            else:
                issue_data = '{"tudiId":"'+pro_df.iloc[index].at['tudiId']+'","approveName": "'+row['approveName']+'","applyNo": "'+row['applyNo']+'","pages": "'+'1'+'","rows": "'+'5'+'"}'
            issue_jsondata = sess_repPost(NT_sess,'https://www.ntfcjy.com:9999/ys_ntzjjg/wx/record/queryLicenseListByTudiId',issue_headers,issue_data.encode("utf-8"),'json')
            issue_df = pd.json_normalize(issue_jsondata)
            issue_df = issue_df[issue_df['licensePernit'] == row['licensePernit']]
            if len(issue_df) != 0 :
                data = issue_df.explode('maps', ignore_index=True)  
                data[['fangwYt', 'totalCnt','ywCnt','ksCnt','fwytCode','ysCnt']] = pd.DataFrame(data['maps'].values.tolist())
                data = data[data['ysCnt'] == '住宅'][['licensePernit', 'totalCnt']]
                data.rename(columns={'licensePernit':'issue_code', 'totalCnt':'room_sum'}, inplace = True)
                data['url'] = 'https://www.ntfcjy.com:9999/ys_ntzjjg/wx/record/queryLicenseList'
                data['region'] = row['adminLocation']
                data['gd_city'] = '南通市'
                data['floor_name'] = row['approveName']
                data['address'] = row['approvalFwzl']
                data['business'] = row['kfqyQymc']
                data['issue_date'] = row['crtDate']
                data['issue_area'] = row['pishouArea']
                data['building_code'] = row['gazhs']
                df=df.append(data,ignore_index=True) 
        print('项目名称:  '+row['approveName']+'      '+'开发商:  '+row['kfqyQymc']+'      '+'预售证号:  '+str(row['licensePernit']))
        time.sleep(10)
    else :
        print("从南通网址的第"+str(1)+"页获取 :" + (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d') +" 登记的数据失败")
        break

# df.to_csv(r'C:/Users/86133/Desktop/df.csv



#In[]
###唐山  还不确定网站数据的更新时间
# proxies = [
#     {'http':'socks5://110.82.142.191:9999'},
#     {'http':'socks5://175.147.101.254:8080'}
# ]
# proxies = random.choice(proxies)
# print(proxies)

TSstart_date_time = issue_ccode['issue_date'][issue_ccode['gd_city']=='唐山市'].values[0]  # 获取爬虫开始时间
issue_ccode_list = issue_ccode['issue_code'][issue_ccode['gd_city']=='唐山市'].values[0]  # 获取预售证号列表

# 获取上次爬到的最后一条预售证名称

url = 'http://yingyong.zhujianju.tangshan.gov.cn:8911/yushouxinxichaxun.aspx'  #第一页网址网址
# resq = requests.post(url,headers=headers,timeout=20,proxies=proxies,verify=False) #读取网页源码
TS_sess = requests.Session()
TS_sess.mount('http://', HTTPAdapter(max_retries=3))  #添加http协议的重试次数
TS_sess.mount('https://', HTTPAdapter(max_retries=3)) #不想思考,两个协议都添加
param = {}


try:
    resq = sess_repPost(TS_sess,url,headers,param,'text') #读取网页源码
    def clear_failedbyte(strs):
        if strs == None:
            return strs
        return strs.replace(' ','').replace('\r','').replace('\n','')
    for i in range(1,2):
        try:
            # resq.encoding='utf-8'
            tab=etree.HTML(resq)
            tab11=tab.xpath('//tbody/tr')
            print('tabs=',len(tab11) ,str(i))
            __VIEWSTATE=tab.xpath('//input[@name="__VIEWSTATE"]/@value')[0]
            for tr in tab11:
                button=tr.xpath('./td[6]/input/@name')[0]
                param={
                        '__EVENTTARGET':'',
                        '__EVENTARGUMENT':'',
                        '__VIEWSTATE':__VIEWSTATE,
                        'ddlSearchType':0,
                        'searchText':'',
                        button:'详情'
                    }
                # print(button)
                # chil_res=requests.post(url=url,data=param,headers=headers,proxies=proxies,verify=False)
                # chil_res=TS_sess.post(url=url,data=param,headers=headers,timeout=20,verify=False)
                chil_res = sess_repPost(TS_sess,url,headers,param,'text')
                # chil_res.encoding='UTF-8'
                newest_name = tr.xpath('./td[4]//text()')[0]
                busniss = tr.xpath('./td[3]//text()')[0]
                issue_code = tr.xpath('./td[1]//text()')[0]
                issue_location =tr.xpath('./td[5]//text()')[0]
                issue_region =  issue_location.replace('河北省唐山市','').replace('唐山市','')[0:3]
                child_tab=etree.HTML(chil_res)
                issue_room_num = clear_failedbyte(child_tab.xpath('//table/tr[3]/td[2]//text()')[0])
                issue_area = clear_failedbyte(child_tab.xpath('//table/tr[3]/td[4]//text()')[0])
                issue_start_date = clear_failedbyte(child_tab.xpath('//table/tr[1]/td[4]//text()')[0])
                issue_start_date = issue_start_date.split('/')[0].rjust(2,'0') +'-'+ issue_start_date.split('/')[1].rjust(2,'0') +'-'+ (issue_start_date.split('/')[2].replace('0:00:00','')).rjust(2,'0')
                # if issue_start_date < (datetime.datetime.now()-datetime.timedelta(days=6)).strftime('%Y-%m-%d') :
                print('项目名称:  '+newest_name+'      '+'开发商:  '+busniss+'      '+'预售证号:  '+str(issue_code)+'      '+'楼盘地:  '+issue_location+'      '+'楼盘区县:  '+str(issue_region)+'      '+'预售面积:  '+str(issue_area)+'      '+'预售套数: ' +str(issue_room_num)+'      '+'发证时间:  '+str(issue_start_date))
                if (issue_start_date <= TSstart_date_time) and (issue_code in issue_ccode_list):
                    print("从唐山网址的第"+str(1)+"页获取 :" +TSstart_date_time+" 登记的数据失败")
                    break
                issue_building_code = clear_failedbyte(child_tab.xpath('//table/tr[6]/td[2]//text()')[0])
                city_name = "唐山市"
                chil_url = chil_res.url
                print(chil_url)
                new=pd.DataFrame({'url':chil_url,'region':issue_region,'gd_city':city_name,'floor_name':newest_name,'address':issue_location,'business':busniss,'issue_code':issue_code,'issue_date':issue_start_date,'issue_area':issue_area,'building_code':issue_building_code,'room_sum':issue_room_num},index=[0])
                df=df.append(new,ignore_index=True) 
                time.sleep(2)
            ##下一页
            param={
                '__EVENTTARGET':'btnNext',
                '__EVENTARGUMENT':'',
                '__VIEWSTATE':__VIEWSTATE,
                'ddlSearchType':0,
                'searchText':'',
            }
            url = 'http://yingyong.zhujianju.tangshan.gov.cn:8911/yushouxinxichaxun.aspx'  #第一页网址网址
            resq = sess_repPost(TS_sess,url,headers,param,'text') #读取网页源码
            time.sleep(2)
        except requests.exceptions.RequestException as e:
            print(e)
            time.sleep(2)
            pass
except requests.exceptions.RequestException as e:
  print(e)










#In[]
# df=pd.read_csv('C:\\Users\\86133\\Desktop\\sche.csv')
df = df[['url','region','gd_city','floor_name','address','business','issue_code','issue_date','issue_area','building_code','room_sum']].drop_duplicates(ignore_index=True)
if df.empty:
     print("无登记数据")


to_dws(df,'ods_city_issue_code')




#In[]
##唐山的数据更新
con.updata_one("update odsdb.ods_city_issue_code a ,(select issue_code ,max(issue_date) issue_date ,max(load_data_time) load_data_time from \
    odsdb.ods_city_issue_code where dr = 0 and load_data_time>'"+TSstart_date_time+"' group by issue_code) b \
  set a.dr = 1 where a.issue_code = b.issue_code and a.load_data_time != b.load_data_time and a.gd_city = '唐山市' and a.issue_date != '9999-09-09'")  
print('唐山 update odsdb.ods_city_issue_code     >>>>>>>Load Data Done')

#In[]

# ##赣州数据处理
con.updata_one("update odsdb.ods_city_issue_code a ,(select floor_name ,room_sum ,business ,address ,building_code ,count(issue_code) issue_num ,max(issue_code) issue_code_max from odsdb.ods_city_issue_code where dr = 0 and gd_city = '赣州市' and issue_date != '9999-09-09' and issue_date>='"+GZstart_date_time+"' group by floor_name ,room_sum ,business ,address ,building_code having issue_num > 1 ) b\
    set a.room_sum = 0 where a.floor_name = b.floor_name and a.room_sum = b.room_sum and a.business = b.business and a.issue_code != b.issue_code_max")  
print('赣州 update odsdb.ods_city_issue_code 1  >>>>>>>Load Data Done')
# 新加了几栋楼，然后统计的时候全部统计进去，所以用当前的套数减去之前的到套数. 以后的预售证套数计算，当前的套数减去之前的套数
con.updata_one("update odsdb.ods_city_issue_code aa ,\
    (select a.floor_name,a.room_sum-b.room_sum result_room_sum,issue_date,issue_code from ( select substring_index(building_code,'----',1) floor_name,issue_date,room_sum,building_code,issue_code from odsdb.ods_city_issue_code where load_data_time >= '"+GZstart_date_time+"' and substring_index(building_code,'----',1) regexp('[0-9]') !=1) a \
    inner join \
        (SELECT substring_index(tt.building_code,'----',1) floor_name,sum(tt.room_sum) room_sum,tt.building_code FROM( select t1.floor_name,t1.issue_date,t1.room_sum,t1.building_code from (select floor_name,issue_date,room_sum,building_code from odsdb.ods_city_issue_code where dr = 0 and gd_city = '赣州市' and floor_name != '' group by building_code,floor_name,issue_date,room_sum) t1 \
		left join \
		  (select floor_name,max(issue_date) max_issue_date from odsdb.ods_city_issue_code where dr = 0 and gd_city = '赣州市' and floor_name != '' group by floor_name) t2 on t1.floor_name = t2.floor_name and t1.issue_date = t2.max_issue_date where t2.floor_name is null) tt group by building_code) b \
	on a.floor_name = b.floor_name where a.room_sum-b.room_sum>10 ) bb \
set aa.room_sum = bb.result_room_sum where substring_index(aa.building_code,'----',1) = bb.floor_name and aa.issue_date = bb.issue_date and aa.issue_code = bb.issue_code")  
print('赣州 update odsdb.ods_city_issue_code 2  >>>>>>>Load Data Done')











# #In[]
# ###唐山  还不确定网站数据的更新时间
# # proxies = [
# #     {'http':'socks5://110.82.142.191:9999'},
# #     {'http':'socks5://175.147.101.254:8080'}
# # ]
# # proxies = random.choice(proxies)
# # print(proxies)

# TSstart_date_time = issue_ccode['issue_date'][issue_ccode['gd_city']=='唐山市'].values[0]  # 获取爬虫开始时间
# issue_ccode_list = issue_ccode['issue_code'][issue_ccode['gd_city']=='唐山市'].values[0]  # 获取预售证号列表

# # 获取上次爬到的最后一条预售证名称

# url = 'http://yingyong.zhujianju.tangshan.gov.cn:8911/yushouxinxichaxun.aspx'  #第一页网址网址
# # resq = requests.post(url,headers=headers,timeout=20,proxies=proxies,verify=False) #读取网页源码
# TS_sess = requests.Session()
# TS_sess.mount('http://', HTTPAdapter(max_retries=3))  #添加http协议的重试次数
# TS_sess.mount('https://', HTTPAdapter(max_retries=3)) #不想思考,两个协议都添加
# param = {}


# try:
#     resq = sess_repPostHtml(TS_sess,url,headers,param) #读取网页源码
# except Exception as e:
#     print(e)

#     def clear_failedbyte(strs):
#         if strs == None:
#             return strs
#         return strs.replace(' ','').replace('\r','').replace('\n','')
#     for i in range(1,2):
#         try:
#             resq.encoding='utf-8'
#             tab=etree.HTML(resq.text)
#             tab11=tab.xpath('//tbody/tr')
#             print('tabs=',len(tab11) ,str(i))
#             __VIEWSTATE=tab.xpath('//input[@name="__VIEWSTATE"]/@value')[0]
#             for tr in tab11:
#                 button=tr.xpath('./td[6]/input/@name')[0]
#                 param={
#                         '__EVENTTARGET':'',
#                         '__EVENTARGUMENT':'',
#                         '__VIEWSTATE':__VIEWSTATE,
#                         'ddlSearchType':0,
#                         'searchText':'',
#                         button:'详情'
#                     }
#                 # print(button)
#                 # chil_res=requests.post(url=url,data=param,headers=headers,proxies=proxies,verify=False)
#                 chil_res=TS_sess.post(url=url,data=param,headers=headers,timeout=20,verify=False)
#                 chil_res.encoding='UTF-8'
#                 newest_name = tr.xpath('./td[4]//text()')[0]
#                 busniss = tr.xpath('./td[3]//text()')[0]
#                 issue_code = tr.xpath('./td[1]//text()')[0]
#                 issue_location =tr.xpath('./td[5]//text()')[0]
#                 issue_region =  issue_location.replace('河北省唐山市','').replace('唐山市','')[0:3]
#                 child_tab=etree.HTML(chil_res.text)
#                 issue_room_num = clear_failedbyte(child_tab.xpath('//table/tr[3]/td[2]//text()')[0])
#                 issue_area = clear_failedbyte(child_tab.xpath('//table/tr[3]/td[4]//text()')[0])
#                 issue_start_date = clear_failedbyte(child_tab.xpath('//table/tr[1]/td[4]//text()')[0])
#                 issue_start_date = issue_start_date.split('/')[0].rjust(2,'0') +'-'+ issue_start_date.split('/')[1].rjust(2,'0') +'-'+ (issue_start_date.split('/')[2].replace('0:00:00','')).rjust(2,'0')
#                 # if issue_start_date < (datetime.datetime.now()-datetime.timedelta(days=6)).strftime('%Y-%m-%d') :
#                 print('项目名称:  '+newest_name+'      '+'开发商:  '+busniss+'      '+'预售证号:  '+str(issue_code)+'      '+'楼盘地:  '+issue_location+'      '+'楼盘区县:  '+str(issue_region)+'      '+'预售面积:  '+str(issue_area)+'      '+'预售套数: ' +str(issue_room_num)+'      '+'发证时间:  '+str(issue_start_date))
#                 if (issue_start_date <= TSstart_date_time) and (issue_code in issue_ccode_list):
#                     print("从唐山网址的第"+str(1)+"页获取 :" +TSstart_date_time+" 登记的数据失败")
#                     break
#                 issue_building_code = clear_failedbyte(child_tab.xpath('//table/tr[6]/td[2]//text()')[0])
#                 city_name = "唐山市"
#                 chil_url = chil_res.url
#                 print(chil_url)
#                 new=pd.DataFrame({'url':chil_url,'region':issue_region,'gd_city':city_name,'floor_name':newest_name,'address':issue_location,'business':busniss,'issue_code':issue_code,'issue_date':issue_start_date,'issue_area':issue_area,'building_code':issue_building_code,'room_sum':issue_room_num},index=[0])
#                 df=df.append(new,ignore_index=True) 
#                 chil_res.close()
#                 time.sleep(2)
#             ##下一页
#             param={
#                 '__EVENTTARGET':'btnNext',
#                 '__EVENTARGUMENT':'',
#                 '__VIEWSTATE':__VIEWSTATE,
#                 'ddlSearchType':0,
#                 'searchText':'',
#             }
#             url = 'http://yingyong.zhujianju.tangshan.gov.cn:8911/yushouxinxichaxun.aspx'  #第一页网址网址
#             resq = TS_sess.post(url,headers=headers,data=param,timeout=20,verify=False) #读取网页源码
#             time.sleep(2)
#             resq.close()
#         except requests.exceptions.RequestException as e:
#             print(e)
#             chil_res.close()
#             resq.close()
#             time.sleep(2)
#             pass
# except requests.exceptions.RequestException as e:
#   print(e)

# df.at[df['region'] == '丰南城' ,'region'] = '丰南区'
# df.at[df['region'] == '曹妃甸' ,'region'] = '曹妃甸区'
# df.at[df['region'] == '滦县新' ,'region'] = '滦州市'
# df.at[df['region'] == '南堡开' ,'region'] = '南堡开发区'
# df.at[~df['region'].astype(str).isin(['丰南区','丰润区','乐亭县','古冶区','开平区','曹妃甸区','滦南县','滦州市','玉田县','路北区','路南区','迁安市','迁西县','遵化市','高新区','南堡开发区']) ,'region'] = np.nan
# df['issue'] = df['issue_code'].apply(lambda x: (x.replace('）',')')).split(')')[0].replace('(','').replace('（',''))
# df.at[df['issue'] == '唐丰南' ,'region'] = '丰南区'
# df.at[df['issue'] == '唐丰' ,'region'] = '丰润区'
# df.at[df['issue'] == '乐' ,'region'] = '乐亭县'
# df.at[df['issue'] == '开' ,'region'] = '开平区'
# df.at[df['issue'] == '曹' ,'region'] = '曹妃甸区'
# df.at[df['issue'].str.contains('滦南') ,'region'] = '滦南县'
# df.at[df['issue'] == '玉' ,'region'] = '玉田县'
# df.at[df['issue'] == '迁' ,'region'] = '迁安市'
# df.at[df['issue'] == '迁西' ,'region'] = '迁西县'
# df.at[df['issue'] == '遵' ,'region'] = '遵化市'
# df.at[df['issue'] == '玉田县' ,'region'] = '玉田县'
# df.at[df['issue'] == '唐古' ,'region'] = '古冶区'
# df.at[df['issue'] == '古' ,'region'] = '古冶区'
# #市辖县
# df.at[df['issue'] == '汉' ,'region'] = '汉沽管理区'
# df.at[df['issue'] == '港' ,'region'] = '海港开发区'
# df.at[df['issue'] == '滦' ,'region'] = '滦州市'
# df.at[df['issue'] == '迁安' ,'region'] = '迁安市'
# df.at[df['issue'] == '芦' ,'region'] = '芦台经济开发区'
# df.at[df['issue'] == '芦台' ,'region'] = '芦台经济开发区'
# df.at[df['issue'] == '高' ,'region'] = '高新区'
# df.at[df['issue'] == '南开' ,'region'] = '南堡开发区'
# df.at[df['issue'] == '岛' ,'region'] = '唐山国际旅游岛'





# #In[]
# ###南通---通州其他页
# # 根据协议类型，选择不同的代理
# # proxies = {
# #   "http": "http://180.119.92.45:9999"
# # }
# for i in range(1,30) :
#     url = 'http://www.nantong.gov.cn/truecms/searchController/getResult.do'
#     data = {
#         "flag": "1",
#         "site": "",
#         "siteId": "",
#         "siteName": "",
#         "excludeSites":"", 
#         "siteCode": "",
#         "rootCode": "",
#         "canChooseSite":"", 
#         "sysId": "",
#         "sysName": "",
#         "pageSize": "15",
#         "timeScope": "",
#         "searchScope": "title",
#         "order": "",
#         "obj": "",
#         "fileType": "",
#         "searchIndexModel": "news,xxgk",
#         "word_correct": "y",
#         "columns": "",
#         "searchStarttime": "2021-01-01", 
#         "searchEndtime": "2022-01-08",
#         "query":"商品房预售" ,
#         "page" : i
#     }

#     response=requests.post(url,data=data,headers=headers)
#     response.encoding='UTF-8'#解码
#     html_data = etree.HTML(response.text)
#     lis = html_data.xpath("/html/body/div[5]/div/div[2]/div[2]/ul/li")#xpath获取当前页所有预售证信息
#     for li in lis:
#         print(li.xpath("./a/@href")[0])
#         if (li.xpath("./a/@href")[0]).find("spgs1") != -1 :
#             obj = re.compile(r'预售号码：(?P<issue_code>.*?)'
#                             r'售房单位：(?P<busniss>.*?)'
#                             r'项目名称：(?P<newest_name>.*?)'
#                             r'房屋坐落：(?P<issue_location>.*?)'
#                             r'房屋用途性质：(?P<house_type>.*?)'
#                             r'预售总建筑面积：(?P<issue_area>.*?)平方.*?'
#                             r'发证日期：(?P<issue_start_date>.*?)日.*?'
#                             ,re.S)
#             result = obj.finditer(li.xpath("./p/text()")[0])
#             for i in result :
#                 if (i.group("house_type").find("住")) != -1:
#                     issue_code = i.group("issue_code")
#                     issue_location = i.group("issue_location")
#                     newest_name = i.group("newest_name")
#                     newest_name = newest_name.split('1')[0].split('2')[0].split('3')[0].split('4')[0].split('5')[0].split('6')[0].split('7')[0].split('8')[0].split('9')[0].split('A')[0].split('B')[0].split('C')[0].split('D')[0]
#                     busniss = i.group("busniss")
#                     issue_area = i.group("issue_area")
#                     issue_room_num = int(int(float(issue_area))/130)
#                     issue_start_date = i.group("issue_start_date")
#                     issue_start_date = re.findall(r'\d+',issue_start_date )[0]+'-'+re.findall(r'\d+',issue_start_date )[1].rjust(2,'0') +'-'+re.findall(r'\d+',issue_start_date )[2].rjust(2,'0')
#                     issue_building_code = i.group("newest_name")
#                     issue_region = "通州区"
#                     city_name = "南通市"
#                     print('项目名称:  '+newest_name+'      '+'开发商:  '+busniss+'      '+'预售证号:  '+str(issue_code)+'      '+'楼盘地:  '+issue_location+'      '+'楼盘区县:  '+str(issue_region)+'      '+'预售面积:  '+str(issue_area)+'      '+'预售套数: ' +str(issue_room_num)+'      '+'发证时间:  '+str(issue_start_date)+'      '+'建筑编号:  '+str(issue_building_code)+'      '+'城市名称:  '+city_name)
#                     new=pd.DataFrame({'url':li.xpath("./a/@href")[0],'region':issue_region,'gd_city':city_name,'floor_name':newest_name,'address':issue_location,'business':busniss,'issue_code':issue_code,'issue_date':issue_start_date,'issue_area':issue_area,'building_code':issue_building_code,'room_sum':issue_room_num},index=[0])
#                     df=df.append(new,ignore_index=True) 
#         # time.sleep(1)
#     response.close()

# df = df.drop_duplicates()


    



#In[]


# city_newest_deal = con.query("select * from odsdb.city_newest_deal where issue_code is not null and issue_code != '' and gd_city = '南通市' and url like '%newhouse%'")

# df = city_newest_deal[['url','room_sum','region','gd_city','floor_name','address','business','issue_code','issue_date','issue_area','building_code']]
# df['issue_area'] = df['issue_area'].apply(lambda x:re.sub('\D','',x.split('.')[0]))
# df['room_sum'] = df['room_sum'].apply(lambda x:re.sub('\D','',x.split('、')[0]))
# df.at[(df['issue_area'].isnull() ),'issue_area'] = 0
# df.at[(df['issue_area'] == ''  ),'issue_area'] = 0
# df.at[(df['issue_area'].isna() ),'issue_area'] = 0
# df = df[['url','region','gd_city','floor_name','address','business','issue_code','issue_date','issue_area','building_code','room_sum']].drop_duplicates(ignore_index=True)
# df['region'] = df['address'].apply(lambda x:is_nullto_value(x).replace('海南省三亚市','').replace('三亚市','').replace('海南省','')[0:3])
# data_new=df.groupby(['issue_code'])['building_code'].apply(list).to_frame()
# data_new['building_code']=data_new['building_code'].apply(lambda x:str(x).replace('[','').replace(']',''))
# data_new
# df = df[['url','region','gd_city','floor_name','address','business','issue_code','issue_date','issue_area','room_sum']].drop_duplicates(ignore_index=True)
# df = pd.merge(df,data_new,how='inner',on=['issue_code'])   
# df




# import requests

# headers = {
#     'Host': 'www.ntfcjy.com:9999',
#     'Authorization': 'bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyTmljayI6IuW-ruS_oeeUqOaItyIsInVzZXJfbmFtZSI6IuW-ruS_oeeUqOaItyIsInNjb3BlIjpbInJlYWQiXSwidXNlclBob25lIjpudWxsLCJ1c2VyVGVsIjoiMCIsInN5c1VzZXJJZCI6MjAwNDIxMCwiZXhwIjoxNjY2ODU5NTM3LCJ1c2VySWQiOjIwOTUwLCJqdGkiOiIxZjAzMDE2Mi0wM2MxLTQ4MmUtODc3MC03NGMxZDJhNGZjODAiLCJjbGllbnRfaWQiOiJXWF9DTElFTlQifQ.SM7ab-gMx5Azvm1Dt_OKN-0ebq6Tupf7Xs2pHn6g8iwDoccNfpx90xFfh_dXQC5Z35XBcjeMYMBYnjGKwB96mXkTwbZmWHr-yIVGxDsgSj5CoYoUSleSj5opkKjtGx-O6dxoLEm5EYoOFcjkTrjOB7FiefD4wvhwTpFoBjD3t6Y8fwen0lnuGKThu5m6g2DEq5WgXspclTaPVu53jMG1HaKN57z7G0JzJj-gEvdikcaWQlFs40wKnsBU0bx6KOakiuyxcq7FZ60FXjSWhjPdvbDCZ4x0-JaDuZAWGur9ymjIzb_VnAJgzyNbKmoMJZY-CfLhtOOzcUhPGozowRN0Aw',
#     'content-type': 'application/json',
#     'client-type': 'WX',
#     'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.16(0x1800103f) NetType/WIFI Language/zh_CN',
#     'Referer': 'https://servicewechat.com/wxa8947deb2350bc19/90/page-frame.html',
# }

# data = '{}'

# response = requests.post('https://www.ntfcjy.com:9999/ntzjjg/wx/querycredit/waitPjNum.do', headers=headers, data=data)
# response.text



# 获取某一页数据
# url = "http://api.tc688.net/api/services/app/merchant/LoadCategoryMerchants"
# header = {
#     "Host":"api.tc688.net",#
#     "Accept":"application/json",#
#     "Authorization":"Bearer aK8iZcwtXe0Wk7HhHDnDTsuNUls_tRsTqEqXjqtli6cBlunXBvKy_kuwwvRY-11AIjL2eSAR-TVhafmJZfIaeiOXUom3AhAz83GJNZb1pfWj6-m3WpHyPkZIhpxi9DjFcrmtVemWG8ZYXv4ZMGF2FKEg35gk4ZFfwnbvkwHSP60QDa1AavQh-XhuBSupzdnhfiThzD0QQTv-XHKpdXGHzgLp9xzqvMs7Y-CYZ962ksc-7_O9_EzoIph_1F66C9k0QNQnU8ksY0RB5L1y0v2J0L391dsQ1qrJcsB5d1Zg9czbOiTev-quaQ73PDBJPrRmVx_k0YMvNepR0E7AMA1VUjB8dJ2BQ_PhW8EYkbZo6596KO5hXbIyoXmdw9tQAxBrp92zYEnc4LrsVRVFZhVmJkexoNMQuUKr905otwwqTajnIWw9-mhQkw7S9fK6Iy8vyAXAExrY2VrSOEaEgKrvRdzYJxtn71EfsUxzUlwc8D8RuqAWFpq6RUPmMhsa_rPK",#
#     "Accept-Language":"zh-CN,zh-Hans;q=0.9",
#     "Accept-Encoding":"gzip, deflate",#
#     "Origin":"http://zazhi.tc688.net",#
#     "User-Agent":"Mozilla/5.0 (iPhone; CPU iPhone OS 15_0_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.16(0x1800103e) NetType/WIFI Language/zh_CN",#
#     "Referer":"http://zazhi.tc688.net/",#
#     "Content-Length":"139",
#     "Cookie":"UM_distinctid=17e0f12ed931d-0d8351c76b524b8-2342264d-3d10d-17e0f12ed94265; Abp.Localization.CultureName=zh-CN",#
# }
# data = {"pageIndex":1,"pageSize":50,"regionId":"8","categoryId":"21","orderRule":0,"redPackValid":"false","couponValid":"false","discountValid":"false"}
# r = requests.post(url=url,data=data,headers=header)
# data_lst = json.loads(r.text)



# url = "https://www.xiaohongshu.com/fe_api/burdock/weixin/v2/homefeed/personalNotes?category=recommend_v2&cursorScore=&geo=&page=1&pageSize=20&needGifCover=true"
# header = {
#     "User-Agent":"Mozilla/5.0 (iPhone; CPU iPhone OS 15_0_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.16(0x1800103e) NetType/WIFI Language/zh_CN",
# }
# data = {"approveName":"","applyNo":"","kfqyQymc":"","licensePernit":"","queryStartDate":"","queryEndDate":"","adminLocations":"","pages":"1","rows":15}
# r = requests.post(url=url,data=data,headers=header)
# data_lst = json.loads(r.text)
# data_lst



# import requests

# cookies = {
#     'UM_distinctid': '17e0f12ed931d-0d8351c76b524b8-2342264d-3d10d-17e0f12ed94265',
#     'Abp.Localization.CultureName': 'zh-CN',
# }

# headers = {
#     'Host': 'api.tc688.net',
#     'Accept': 'application/json',
#     'Authorization': 'Bearer aK8iZcwtXe0Wk7HhHDnDTsuNUls_tRsTqEqXjqtli6cBlunXBvKy_kuwwvRY-11AIjL2eSAR-TVhafmJZfIaeiOXUom3AhAz83GJNZb1pfWj6-m3WpHyPkZIhpxi9DjFcrmtVemWG8ZYXv4ZMGF2FKEg35gk4ZFfwnbvkwHSP60QDa1AavQh-XhuBSupzdnhfiThzD0QQTv-XHKpdXGHzgLp9xzqvMs7Y-CYZ962ksc-7_O9_EzoIph_1F66C9k0QNQnU8ksY0RB5L1y0v2J0L391dsQ1qrJcsB5d1Zg9czbOiTev-quaQ73PDBJPrRmVx_k0YMvNepR0E7AMA1VUjB8dJ2BQ_PhW8EYkbZo6596KO5hXbIyoXmdw9tQAxBrp92zYEnc4LrsVRVFZhVmJkexoNMQuUKr905otwwqTajnIWw9-mhQkw7S9fK6Iy8vyAXAExrY2VrSOEaEgKrvRdzYJxtn71EfsUxzUlwc8D8RuqAWFpq6RUPmMhsa_rPK',
#     'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
#     'Content-Type': 'application/json;charset=UTF-8',
#     'Origin': 'http://zazhi.tc688.net',
#     'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.16(0x1800103f) NetType/WIFI Language/zh_CN',
#     'Referer': 'http://zazhi.tc688.net/',
# }

# data = '{"pageIndex":1,"pageSize":50,"regionId":"8","categoryId":"21","orderRule":0,"redPackValid":false,"couponValid":false,"discountValid":false}'

# response = requests.post('http://api.tc688.net/api/services/app/merchant/LoadCategoryMerchants', headers=headers, cookies=cookies, data=data)
# response.text

# import requests

# headers = {
#     'Host': 'ssl.yzfdc.cn',
#     'content-type': 'application/json',
#     'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.16(0x1800103f) NetType/WIFI Language/zh_CN',
#     'Referer': 'https://servicewechat.com/wxadd9e69712c348cc/51/page-frame.html',
# }

# params = (
#     ('act', 'xsxkz'),
#     ('jscode', '0510Vk000qEl4N17yz100WQZEd00Vk0J'),
#     ('phoneNum', '15064847527'),
#     ('ussrid', '202201041058276346'),
#     ('userid', '202201041058276346'),
#     ('pageindex', '2'),
#     ('pagesize', '10'),
#     ('xmmckey', ''),
#     ('qymckey', ''),
#     ('gczhkey', ''),
# )

# response = requests.get('https://ssl.yzfdc.cn/json//Yzfdc/service.ashx', headers=headers, params=params)
# # response.text
# # #NB. Original query string below. It seems impossible to parse and
# # #reproduce query strings 100% accurately so the one below is given
# # #in case the reproduced version is not "correct".
# # response = requests.get('https://ssl.yzfdc.cn/json//Yzfdc/service.ashx?act=xsxkz&jscode=0510Vk000qEl4N17yz100WQZEd00Vk0J&phoneNum=15064847527&ussrid=202201041058276346&userid=202201041058276346&pageindex=2&pagesize=10&xmmckey=&qymckey=&gczhkey=', headers=headers)
# response.json()

# import requests

# headers = {
#     'Host': 'wx.jtsf.cn',
#     'content-type': 'application/json',
#     'Accept': 'application/json',
#     'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.16(0x1800103f) NetType/WIFI Language/zh_CN',
#     'Referer': 'https://servicewechat.com/wx4ce64e09c373e76a/58/page-frame.html',
# }

# params = (
#     ('per_page', '10'),
#     ('is_get', '1'),
# )

# response = requests.get('https://wx.jtsf.cn/api/schools', headers=headers, params=params)
# response.json()



# import requests

# headers = {
#     'Host': 'api.weibo.cn',
#     'content-type': 'application/x-www-form-urlencoded',
#     'X-Sessionid': '5f1ec89f24f542737bac418b5d06e4f8',
#     'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.16(0x1800103f) NetType/WIFI Language/zh_CN',
#     'Referer': 'https://servicewechat.com/wx9074de28009e1111/223/page-frame.html',
# }

# params = (
#     ('new_version', '0'),
#     ('gsid', '_2AuZrXZVipZYuRwtW6347e4cAb17NmmRd1pcbDNgfIKl9B9C5DmdQst9lTHQXjFPZRZ1ZqvhDyMqmWL6oGay37nZEZpKA'),
#     ('uid', '1011395420406'),
#     ('from', '1885396040'),
#     ('wm', '90163_90001'),
#     ('c', 'weixinminiprogram'),
#     ('networktype', 'wifi'),
#     ('v_p', '60'),
#     ('s', 'a6494027'),
#     ('lang', 'zh_CN'),
# )

# data = 'group_id=102803&extparam=discover%7Cnew_feed&count=20&containerid=&trim_level=1&trim_page_recom=0&need_jump_scheme=1&preAdInterval=-1&since_id=0&fid=102803&lfid=&no_location_permission=&lon=&lat=&refresh_type=auto'

# response = requests.post('https://api.weibo.cn/2/guest/statuses_unread_hot_timeline', headers=headers, params=params, data=data)

# #NB. Original query string below. It seems impossible to parse and
# #reproduce query strings 100% accurately so the one below is given
# #in case the reproduced version is not "correct".
# # response = requests.post('https://api.weibo.cn/2/guest/statuses_unread_hot_timeline?new_version=0&gsid=_2AuZrXZVipZYuRwtW6347e4cAb17NmmRd1pcbDNgfIKl9B9C5DmdQst9lTHQXjFPZRZ1ZqvhDyMqmWL6oGay37nZEZpKA&uid=1011395420406&from=1885396040&wm=90163_90001&c=weixinminiprogram&networktype=wifi&v_p=60&s=a6494027&lang=zh_CN', headers=headers, data=data)
# response.json()








        # chil_tree = etree.HTML(chil_resq.text) #转未html
        # tbody = chil_tree.xpath("/html/body/div[2]/div/div[2]/div/div[2]/div/table/tbody")[0] #获取数据

        #    #====================================================================
        #     if tbody.xpath("./tr[6]/td[4]/p/span[1]/text()")[0].split('.')[0].isdigit() :
        #     # if tbody.xpath("./tr[6]/td[4]/p/span[1]/font/text()")[0].split('.')[0].isdigit() :
        #         issue_area = tbody.xpath("./tr[6]/td[4]/p/span[1]/text()")[0]
        #         # issue_area = tbody.xpath("./tr[6]/td[4]/p/span[1]/font/text()")[0]
        #     else :
        #         issue_area = int(re.sub('\D','',tbody.xpath("./tr[6]/td[4]/p/span[1]/text()")[0])) + int(re.sub('\D','',tbody.xpath("./tr[6]/td[4]/p/span/span/text()")[0] ))      
        #    #=====================================================================
        #     if tbody.xpath("./tr[7]/td[4]/p/span[1]/text()")[0].isdigit() :
        #     # if tbody.xpath("./tr[7]/td[4]/p/span[1]/font/text()")[0].isdigit() :
        #         issue_room_num = tbody.xpath("./tr[7]/td[4]/p/span[1]/text()")[0]
        #         # issue_room_num = tbody.xpath("./tr[7]/td[4]/p/span[1]/font/text()")[0]
        #     else :
        #         issue_room_num = int(re.sub('\D','',tbody.xpath("./tr[7]/td[4]/p/span/span[1]/text()")[0])) + int(re.sub('\D','',tbody.xpath("./tr[7]/td[4]/p/span/span[2]/text()")[0]))
        #     #===================================================================
        #     issue_start_date = tbody.xpath("./tr[9]/td[4]/p/span[1]/text()")[0] + '-' + tbody.xpath("./tr[9]/td[4]/p/span[2]/span[1]/text()")[0].rjust(2,'0') + '-' + tbody.xpath("./tr[9]/td[4]/p/span[2]/span[2]/text()")[0].rjust(2,'0')
        # issue_start_date = tbody.xpath("./tr[9]/td[4]/p/span[1]/font/text()")[0].replace('年','') + '-' + tbody.xpath("./tr[9]/td[4]/p/span[2]/font/text()")[0].rjust(2,'0') + '-' + tbody.xpath("./tr[9]/td[4]/p/span[4]/font/text()")[0].rjust(2,'0')
        
        # print('项目名称:  '+newest_name+'      '+'开发商:  '+busniss+'      '+'预售证号:  '+issue_code+'      '+'楼盘地址:  '+issue_location+'      '+'楼盘区县:  '+issue_region+'      '+'预售面积:  '+issue_area+'      '+'预售套数:  '+issue_room_num+'      '+'发证时间:  '+issue_start_date+'      '+'建筑编号:  '+issue_building_code+'      '+'城市:  '+city_name)
#  r'发证时间.*?<span.*?>(?P<issue_start_date>.*?)<.*?>(?P<issue_start_date1>.*?)<.*?>(?P<issue_start_date2>.*?)<.*?>(?P<issue_start_date3>.*?)<.*?>(?P<issue_start_date4>.*?)<.*?>(?P<issue_start_date5>.*?)<.*?>(?P<issue_start_date6>.*?)<.*?>(?P<issue_start_date7>.*?)<.*?>(?P<issue_start_date8>.*?)<.*?>(?P<issue_start_date9>.*?)<.*?>(?P<issue_start_date10>.*?)<.*?>(?P<issue_start_date11>.*?)<.*?>(?P<issue_start_date12>.*?)<.*?>(?P<issue_start_date13>.*?)<.*?>(?P<issue_start_date14>.*?)<.*?>(?P<issue_start_date15>.*?)<.*?>(?P<issue_start_date16>.*?)<.*?>(?P<issue_start_date17>.*?)<.*?>(?P<issue_start_date18>.*?)<.*?'
#  r'预售证号.*?<span.*?>(?P<issue_code>.*?)<.*?>(?P<issue_code_num1>.*?)<.*?>(?P<issue_code_num2>.*?)<.*?>(?P<issue_code_num3>.*?)<.*?>(?P<issue_code_num4>.*?)<.*?>(?P<issue_code_num5>.*?)<.*?>(?P<issue_code_num6>.*?)<.*?>(?P<issue_code_num7>.*?)<.*?>(?P<issue_code_num8>.*?)<.*?>(?P<issue_code_num9>.*?)<.*?>(?P<issue_code_num10>.*?)<.*?'
# issue_code = (i.group("issue_code")+str(i.group("issue_code_num1"))+str(i.group("issue_code_num2"))+str(i.group("issue_code_num3"))+str(i.group("issue_code_num4"))+str(i.group("issue_code_num5"))+str(i.group("issue_code_num6"))+str(i.group("issue_code_num7"))+str(i.group("issue_code_num8"))+str(i.group("issue_code_num9"))+str(i.group("issue_code_num10"))).replace(' ','').replace('\n','')
# issue_start_date = (i.group("issue_start_date")+str(i.group("issue_start_date1"))+str(i.group("issue_start_date2"))+str(i.group("issue_start_date3"))+str(i.group("issue_start_date4"))+str(i.group("issue_start_date5"))+str(i.group("issue_start_date6"))+str(i.group("issue_start_date7"))+str(i.group("issue_start_date8"))+str(i.group("issue_start_date9"))+str(i.group("issue_start_date10"))+str(i.group("issue_start_date11"))+str(i.group("issue_start_date12"))+str(i.group("issue_start_date13"))+str(i.group("issue_start_date14"))+str(i.group("issue_start_date15"))+str(i.group("issue_start_date16"))+str(i.group("issue_start_date17"))+str(i.group("issue_start_date18"))).replace(' ','').replace('\n','').replace('年','-').replace('月','-').replace('日','')




#In[]
###保定---历史
# for i in range(78,113):
#     url = 'http://www.bdfdc.net/permitPageList.jspx?pageIndex='+str(i)+'&orderType=1&orderFile=permit_start_date'  #第一页网址网址
#     url_master = 'http://www.bdfdc.net' #首页
#     # proxies = {
#     #     "http":"110.82.142.191:9999",
#     #     "https":"110.82.142.191:9999",
#     #     }
#     resq = requests.get(url,headers=headers,timeout=20,verify=False) #读取网页源码
#     resq.encoding='UTF-8'#解码
#     tree = etree.HTML(resq.text) #转未html
#     lis = tree.xpath("/html/body/div[4]/div/div[2]/div[2]/ul/li")#xpath获取当前页所有预售证信息
#     for li in lis:
#         href = li.xpath("./div/a/@href")[0]  #子链接
#         chil_url = url_master+href
#         chil_resq = requests.get(chil_url,headers=headers,timeout=20,verify=False) #读取网页源码
#         chil_resq.encoding='UTF-8'#解码
#         chil_tree = etree.HTML(chil_resq.text) #转未html
#         table = chil_tree.xpath("/html/body/div[4]/div/div[2]/div[2]/table")[0] #获取数据 
#         newest_name = table.xpath("./tr[3]/td[2]/text()")[0] 
#         busniss = table.xpath("./tr[2]/td[2]/text()")[0] 
#         issue_code = table.xpath("./tr[1]/td[2]/text()")[0] 
#         issue_location = table.xpath("./tr[4]/td[2]/text()")[0] 
#         issue_region = table.xpath("./tr[3]/td[2]/text()")[0] 
#         issue_region =issue_location.split('区')[0]+'区' 
#         # if len(issue_region)>5 :
#         issue_region= np.nan
#         issue_area = table.xpath("./tr[5]/td[2]/text()")[0] 
#         issue_room_num = table.xpath("./tr[6]/td[2]/text()")[0] 
#         issue_start_date = table.xpath("./tr[8]/td[2]/text()")[0] 
#         if len(table.xpath("./tr[9]/td[2]/text()")) != 0:
#             issue_building_code = table.xpath("./tr[9]/td[2]/text()")[0]
#         else : 
#             issue_building_code = np.nan
#         city_name = "保定市"
#         print('项目名称:  '+newest_name+'      '+'开发商:  '+busniss+'      '+'预售证号:  '+str(issue_code)+'      '+'楼盘地:  '+issue_location+'      '+'楼盘区县:  '+str(issue_region)+'      '+'预售面积:  '+str(issue_area)+'      '+'预售套数: ' +str(issue_room_num)+'      '+'发证时间:  '+str(issue_start_date)+'      '+'建筑编号:  '+str(issue_building_code)+'      '+'城市名称:  '+city_name)
#         new=pd.DataFrame({'url':chil_url,'region':issue_region,'gd_city':city_name,'floor_name':newest_name,'address':issue_location,'business':busniss,'issue_code':issue_code,'issue_date':issue_start_date,'issue_area':issue_area,'building_code':issue_building_code,'room_sum':issue_room_num},index=[0])
#         df=df.append(new,ignore_index=True) 
#         time.sleep(5)
#         # print(href)
#     chil_resq.close()
#     resq.close()







# df.to_csv(r'C:/Users/86133/Desktop/df.csv')


# city_newest_deal = con.query("select * from odsdb.ods_city_issue_code where issue_code is not null and issue_code != '' and gd_city = '保定市' and url like '%zzdcxh%'")

# df = city_newest_deal[['url','room_sum','region','gd_city','floor_name','address','business','issue_code','issue_date','issue_area','building_code']]
# # df['issue_area'] = df['issue_area'].apply(lambda x:re.sub('\D','',x.split('.')[0]))
# # df['room_sum'] = df['room_sum'].apply(lambda x:re.sub('\D','',x.split('、')[0]))
# df['building_code'] = df['floor_name']
# df['floor_name'] = df['floor_name'].apply(lambda x:x.split('1')[0])
# df['floor_name'] = df['floor_name'].apply(lambda x:x.split('2')[0])
# df['floor_name'] = df['floor_name'].apply(lambda x:x.split('3')[0])
# df['floor_name'] = df['floor_name'].apply(lambda x:x.split('4')[0])
# df['floor_name'] = df['floor_name'].apply(lambda x:x.split('5')[0])
# df['floor_name'] = df['floor_name'].apply(lambda x:x.split('6')[0])
# df['floor_name'] = df['floor_name'].apply(lambda x:x.split('7')[0])
# df['floor_name'] = df['floor_name'].apply(lambda x:x.split('8')[0])
# df['floor_name'] = df['floor_name'].apply(lambda x:x.split('9')[0])
