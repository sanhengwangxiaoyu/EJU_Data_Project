# In[]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-

from pickletools import read_unicodestringnl
import pymysql, pandas as pd, json, re
from sqlalchemy import create_engine


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

deal=con.query("select @rowNo:=@rowNo+1 id,a.* from fang_detail_new a,(select @rowNo:=0) b")


#In[]
#trends
deal_trends = deal[deal['trends'] != "[]"][['newest_id','newest_name','search_result','url','trends']]
deal_trends['trends'] = deal_trends['trends'].apply(lambda x: re.sub('\[|\]|\\n','',x))
deal_trends['data'] = deal_trends['trends'].map(lambda x: list(eval(x)))
deal_trends.at[deal_trends['data'].astype(str) == str(['tag', 'title', 'date', 'content']),'data'] = deal_trends['trends']

deal_trends['ty'] = deal_trends['data'].apply(lambda x: str(isinstance(x,str)))
deal_trends.at[deal_trends['ty'].astype(str) == 'True','data'] = deal_trends['trends'].map(lambda x: [eval(x)])
deal_trends
data = deal_trends.explode('data', ignore_index=True)
data[['tag','title','date','provide_sche']] = pd.DataFrame(data['data'].values.tolist())
data['provide_title'] = data.apply(lambda x: x.tag+' '+x.title,axis=1)
rsult = data[['newest_id','newest_name','search_result','url','provide_title','date','provide_sche']]
rsult = rsult[rsult['date'] >= '2022年01月01日']
rsult.rename(columns={'search_result':'bk_name','url':'dongtai'},inplace=True)


#In[]
#sale
deal_sales = deal[deal['sale'] != "[]"][['newest_id','newest_name','search_result','url','sale']]
deal_sales['sale'] = deal_sales['sale'].apply(lambda x: re.sub('\[|\]|\\n','',x))
deal_sales['data'] = deal_sales['sale'].map(lambda x: list(eval(x)))
deal_sales['index'] = deal_sales['data'].apply(lambda x: 1 if str(x).find('{') != -1 else 0)
deal_sales = deal_sales[deal_sales['index'] == 1]
deal_sales = deal_sales.explode('data', ignore_index=True)
deal_sales[['issue_code','issue_date','sale_building']] = pd.DataFrame(deal_sales['data'].values.tolist())
deal_sales
rsult_sales = deal_sales[deal_sales['issue_date'] >= '2022-01-01']
rsult_sales.groupby(['newest_id'])['issue_code'].count().reset_index()
rsult_sales = rsult_sales[['newest_id','newest_name','search_result','issue_code','issue_date','sale_building','url']]
rsult_sales = rsult_sales[(rsult_sales['issue_date']!='暂无信息')&(rsult_sales['issue_date']>='2022-01-01')]
rsult_sales['quater'] = '2022Q1'



#In[]
def to_dws(result,table):
    engine = create_engine("mysql+pymysql://wanganming:NDR_AhfzXT3MSxfh@172.28.36.77:3306/odsdb?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

rsult = rsult.drop_duplicates()
database = 'odsdb'
to_dws(rsult,'bk_provide_sche_2022q1')


#In[]
def to_dws(result,table):
    engine = create_engine("mysql+pymysql://wanganming:NDR_AhfzXT3MSxfh@172.28.36.77:3306/dwb_db?charset=utf8")
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)
rsult_sales = rsult_sales.drop_duplicates()
database = 'dwb_db'
to_dws(rsult_sales,'dwb_ali7_st_fang_detail_issue')



#In[]
# ##basic_info    record	planning	sale	peitao	trends
# deal_basic_info = deal[['id','newest_id','basic_info']]
# deal_basic_info[['id']] = deal_basic_info[['id']].astype('int')
# deal_basic_info = (deal_basic_info.to_json(orient = "records",force_ascii=False))
# deal_basic_info = deal_basic_info.replace(':"{',':{').replace('"}','}').replace('\'','"')
# # deal_basic_info
# json_datas_basic_info = json.loads(deal_basic_info)
# result_basic_info = pd.json_normalize(json_datas_basic_info)
# result_basic_info


# #In[]
# deal_record = deal[['id','newest_id','record']]
# deal_record[['id']] = deal_record[['id']].astype('int')
# deal_record = (deal_record.to_json(orient = "records",force_ascii=False))
# # deal_record
# deal_record = deal_record.replace(':"{',':{').replace('"}','}').replace('\'','"')
# # deal_record
# json_datas_record = json.loads(deal_record)
# # json_datas
# tmp_record = pd.json_normalize(json_datas_record)
# # pd.json_normalize(json_datas,'record.楼盘纪事',['id','newest_id','record.分期信息'])
# tmp_record.columns = ['id','newest_id','record_story','record_split']
# # pd.json_normalize(tmp_record,'record_story',['id','newest_id','record_split'])
# tmp_record_split = tmp_record[['id','newest_id','record_split']]


# tmp_record = pd.DataFrame(tmp_record)
# # tmp_record
# tmp_record_s = tmp_record[['id','newest_id','record_story']]
# record_s = (tmp_record_s.to_json(orient = "records",force_ascii=False))
# # record_s = record_s.replace(':"{',':{').replace('"}','}').replace('\'','"').replace('"(','').replace(',)"','').replace('}]}','"}]}').replace('},{"','"},{"').replace('"}]"}','"}]}')
# record_s = record_s.replace('(','').replace(',)','').replace('\\/','/').replace('"\'','"').replace('\'"','"')
# # record_s
# # json.loads(record_s)
# json_datas_record_s = json.loads(record_s)
# # json_datas_record_s
# json_datas_record_s = pd.json_normalize(json_datas_record_s,'record_story',['id','newest_id'])
# json_datas_record_s.columns = ['d','open_give','building','id','newest_id']
# json_datas_record_s = pd.DataFrame(json_datas_record_s)
# result_record = json_datas_record_s[['id','newest_id','open_give','d','building']]
# result_record = pd.merge(result_record,tmp_record_split,how='left',on=['id','newest_id'])
# result_record[result_record['open_give']=='交房'][['newest_id','open_give','d']].groupby(['newest_id'])['d'].max().reset_index()



