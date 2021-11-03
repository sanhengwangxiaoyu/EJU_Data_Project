#-*- coding: utf-8 -*-
import requests,os, time
from queue import Queue
from retry import retry
import sys
from geopy.distance import geodesic

#types说明：可以是中文地点文本搜索接口，支持按照设定的POI类型限定地点搜索结果；地点类型与poi typecode是同类内容，可以传入多个poi typecode，相互之间用“|”分隔，内容可以参考POI分类码表；地点（POI）列表的排序会按照高德搜索能力进行综合权重排序；
#lat_lng:39.5829640,115.8968240
#按坐标+范围搜索,无返回页数：https://restapi.amap.com/v5/place/around?key={key}&keywords=&types={type}&location={lat_lng}&radius=3000&sortrule=&region=&show_fields&page_size=25&page_num={page}&sig=&output=&callback=

latlng_list='search_gps.txt'#文本有4列：new,addr,lat_lng 
outfile = 'latlng_rs.txt'
keylog='keylog.txt'
oklist=set()
urls=Queue()
keys=Queue()

@retry()
def main():
    #获取当天已用key，生成可用key列表
    date=time.strftime("%Y-%m-%d", time.localtime())
    keynot={}
    of=open(keylog,'r', encoding='utf-8')
    lines=of.readlines()
    for line in lines:
        s=line.split('\t')
        if date==s[0]:
            keynot[s[2].strip()]=int(s[1])
    #https://console.amap.com/dev/key/app 到高德开放平台注册账号，然后在控制台-》应用管理-》我的应用 ，在占右侧添加一个web服务，添加成功后，把产生的key放在这里，
    #每个key每天免费2000次调用，可用多个账号申请多个key
    keylist=['d9aa6e41d00153b3b466e408aea92b39',
    '61d7e8cf769016c6904b3cea7b719e3d',
    'ecc2bff0af86a26467024657ff163c76',
    '7ef88a0bce43b7f38bce06a3e5583915',
    '062aa35328a7748b6f05569a8502fb53',
    '8872d33d9d2cc8841e3a48f569c35b66',
    '7207f57def437364852fc2e95fb211d6'
            
    ]
    for key in keylist:
        num=keynot.get(key,1)
        print(key,'used',num)
        for i in range(num,29900):
            keys.put([str(i),key])
    #需要爬的类型列表
    types = {
        '综合医院':['090100', '090101', '090102'],
        '专科医院':['090200', '090201', '090202', '090203', '090204', '090205', '090206', '090207', '090208', '090209', '090210', '090211','090300','090400'],
        '酒店':['100100', '100101', '100102', '100103', '100104', '100105', '100200', '100201'],
        '学校':['141200', '141201', '141202', '141203', '141204', '141205', '141206', '141207', '141300', '141400'],
        '公交车站': ['150700', '150705', '150704', '150703'],
        '地铁站':['150500'],
        ## '火车站':['150200', '150201', '150202', '150203', '150204', '150205', '150206', '150207', '150208', '150209', '150210'],
        ## '飞机场':['150100', '150101', '150102', '150104', '150105', '150106', '150107'],
        '商场':['060100','060101','060102','060103','060300','060301','060302','060303','060304','060400','060401','060402','060403','060404','060405','060406','060407','060408','060409','060600','060601','061000','061001']
    }
    #已爬经纬度及类型:
    if os.path.exists(outfile):
        with open(outfile, 'r', encoding='utf-8') as f:
            for i in f:
                oklist.add(i.split('\t')[0])
    #读取待爬gps
    if os.path.exists(latlng_list):
        with open(latlng_list,'r',encoding='UTF-8') as txtData: 
            for line in txtData.readlines():
                s=line.split('\t')
                if s[0] in oklist:
                    continue
                urls.put(s)
        #       
        print("qsize="+str(urls.qsize()))           
        time.sleep(5)
        keyof=open(keylog,'a', encoding='utf-8')
        of=open(outfile,'a', encoding='utf-8')
        of.write('\t'.join(['newest_id','lat_lng','typename','typename1','name','gps','pure_distance','cityname']))
        of.write('\n')
        while urls.qsize()>0:
            print('qsize less=',urls.qsize())
            url=urls.get()
            rs=run(url,types,keyof)
            of.write('\n'.join(rs))
            of.flush()
            # time.sleep(11111)
        # ths = []
        # for i in range(1):
            # t = Thread(target=run, args=())
            # t.start()
            # ths.append(t)
        # for t in ths:
            # t.join()
 

def run(url,types,keyof):
    print(url)
    date=time.strftime("%Y-%m-%d", time.localtime())
    rslist=[]
    newest_id=url[0]
    newest_name=url[1]
    address=url[2]
    lat_lng=url[3].strip()
    
    #遍历分类大项
    for typename in types:
        typeids=types[typename] #type分类id列表
        #遍历分类id
        end='no'  #如果大分类搜索到的个数少于900个，直接用大分类搜索。否则需要加上小分类搜索
        num=0
        for typeid in typeids:
            #第一个是主分类，当主分类结果小于900时，中断
            num+=1
            if num==2 and end=='yes':
                break
            page=0
            count=-1
            #翻页
            while True:
                page+=1
                key=keys.get()#num,key:每次请都，都需要获取一次当天可用的key，单个key请求2000次上限
                if key==None:
                    print('no key less')
                    sys.exit()
                url=f'https://restapi.amap.com/v5/place/around?key={key[1]}&keywords=&types={typeid}&location={lat_lng}&radius=3000&sortrule=&region=&show_fields&page_size=25&page_num={page}&sig=&output=&callback='
                # url=f'https://restapi.amap.com/v3/place/around?key={key[1]}&keywords=&types={typeid}&location={lat_lng}&radius=3000&sortrule=&region=&show_fields&page_size=25&page_num={page}&sig=&output=&callback='
                print(url)
                requests.adapters.DEFAULT_RETRIES = 5
                res = requests.get(url).json()
                if count==-1:
                    count=int(res.get('count',0))
                    print(typename,typeid,count)
                    #列表少于900，只爬大分类
                    if count<900 :
                        print(typename,'less 900')
                        end='yes'
                #保存当天已用过的key                
                keyof.write(f'{date}\t{key[0]}\t{key[1]}\n')
                keyof.flush()
                #无结果，中断翻页
                pois=res.get('pois', [])
                if len(pois)==0:
                    break
                for poi in pois:
                    try:                    
                        name=poi['name']
                        gps=poi['location']
                        typename1=poi['type']
                        pname=poi['pname']
                        cityname=poi['cityname']
                        adname=poi['adname']
                        address1=poi['address'] 
                        ods_lng_x=lat_lng.split(',')[0]
                        ods_lat_x=lat_lng.split(',')[1]
                        # print("楼盘经纬度: "+ods_lat_x)   
                        # print("楼盘经纬度: "+ods_lng_x)                        
                        gps_lng_y=gps.split(',')[0]
                        gps_lat_y=gps.split(',')[1]
                        # print("POI经纬度: "+gps_lat_y)
                        # print("POI经纬度: "+gps_lng_y)
                        pure_distance=str(geodesic((ods_lat_x,ods_lng_x),(gps_lat_y,gps_lng_y)).km)
                        # print(pure_distance)
                        # rslist.append('\t'.join([gps_lng_y,newest_id,newest_name,address,lat_lng,typename,typename1,name,gps,pname,cityname,adname,address1]))
                        rslist.append('\t'.join([newest_id,lat_lng,typename,typename1,name,gps,pure_distance,cityname]))
                    except :
                        print("Error: POI数据解析失败")
    if len(rslist)==0:
        rslist.append('\t'.join([newest_name,address,lat_lng])+'\t\t\t\t\t\t\t')
    rslist.append('')
    return rslist
if __name__ == '__main__':
    main()
    
    