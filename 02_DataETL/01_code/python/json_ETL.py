# In[]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-

from json.decoder import JSONDecodeError
from numpy import maximum
import pymysql,pandas as pd,json


##设置变量初始值##
user = 'root'
password = '000000'
db_host = '47.96.87.7'
database = 'ST'

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
    def close(self):
        self.conn.close()

con = MysqlClient(db_host,database,user,password)

deal=con.query("select @rowNo:=@rowNo+1 id,a.* from fang_detail a,(select @rowNo:=0) b")



#In[]
##basic_info    record	planning	sale	peitao	trends
deal_basic_info = deal[['id','newest_id','basic_info']]
deal_basic_info[['id']] = deal_basic_info[['id']].astype('int')
deal_basic_info = (deal_basic_info.to_json(orient = "records",force_ascii=False))
deal_basic_info = deal_basic_info.replace(':"{',':{').replace('"}','}').replace('\'','"')
# deal_basic_info
json_datas_basic_info = json.loads(deal_basic_info)
result_basic_info = pd.json_normalize(json_datas_basic_info)
# result_basic_info


#In[]
deal_record = deal[['id','newest_id','record']]
deal_record[['id']] = deal_record[['id']].astype('int')
deal_record = (deal_record.to_json(orient = "records",force_ascii=False))
# deal_record
deal_record = deal_record.replace(':"{',':{').replace('"}','}').replace('\'','"')
# deal_record
json_datas_record = json.loads(deal_record)
# json_datas
tmp_record = pd.json_normalize(json_datas_record)
# pd.json_normalize(json_datas,'record.楼盘纪事',['id','newest_id','record.分期信息'])
tmp_record.columns = ['id','newest_id','record_story','record_split']
# pd.json_normalize(tmp_record,'record_story',['id','newest_id','record_split'])
tmp_record_split = tmp_record[['id','newest_id','record_split']]


tmp_record = pd.DataFrame(tmp_record)
# tmp_record
tmp_record_s = tmp_record[['id','newest_id','record_story']]
record_s = (tmp_record_s.to_json(orient = "records",force_ascii=False))
# record_s = record_s.replace(':"{',':{').replace('"}','}').replace('\'','"').replace('"(','').replace(',)"','').replace('}]}','"}]}').replace('},{"','"},{"').replace('"}]"}','"}]}')
record_s = record_s.replace('(','').replace(',)','').replace('\\/','/').replace('"\'','"').replace('\'"','"')
# record_s
# json.loads(record_s)
json_datas_record_s = json.loads(record_s)
# json_datas_record_s
json_datas_record_s = pd.json_normalize(json_datas_record_s,'record_story',['id','newest_id'])
json_datas_record_s.columns = ['d','open_give','building','id','newest_id']
json_datas_record_s = pd.DataFrame(json_datas_record_s)
result_record = json_datas_record_s[['id','newest_id','open_give','d','building']]
result_record = pd.merge(result_record,tmp_record_split,how='left',on=['id','newest_id'])
result_record


#In[]
#trends
deal_trends = deal[['id','newest_id','trends']]
deal_trends[['id']] = deal_trends[['id']].astype('int')
deal_trends = (deal_trends.to_json(orient = "records",force_ascii=False))
# deal_trends = deal_trends.replace(':"{',':{').replace('"}','}').replace('\'','"')
deal_trends = json.loads(deal_trends)
pd.json_normalize(deal_trends,max_level=0)




