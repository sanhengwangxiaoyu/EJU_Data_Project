#In[]
import configparser,os,pymysql,pandas as pd,re,time
from sqlalchemy import create_engine
from difflib import SequenceMatcher#导入库
import numpy as np


###创建比较函数
def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()#引用ratio方法，返回序列相似性的度量
df1=pd.read_excel('C:\\Users\\86133\\Desktop\\楼盘经纬度效验.xlsx',engine='openpyxl')



#In[]
####第二阶段

# df2 = df1[(df1['level'] == 1) & (~df1['change_newest_name'].isna())]
# df3 = df2[(df2['compare'] == False) & (~df2['change_newest_name'].str.contains("·")) & (~df2['newest_name'].str.contains("-"))]

# df3.to_csv("C:\\Users\\86133\\Desktop\\tmp.csv")

df4 = df1[~df1['change_lng_lat'].isna()][['city_name','change_lng_lat']]
df4['change_lng'] = df4['change_lng_lat'].map(lambda x:x. split(',')[0]).astype(float).astype(int)
df5_sum = df4.groupby(['city_name'])['change_lng'].sum().reset_index()
df5_count = df4.groupby(['city_name'])['change_lng'].count().reset_index()
df5 = pd.merge(df5_count,df5_sum,how='inner',on=['city_name'])
df5.columns=['city_name','lng_count','lng_sum']
df5['lng_avg'] = (df5['lng_sum']/df5['lng_count']).astype(float).astype(int)


df1['change_lng'] = df4['change_lng_lat'].map(lambda x:x. split(',')[0]).astype(float).astype(int)
df6 = pd.merge(df1,df5,how='left',on=['city_name'])
df6['comp'] = df6['change_lng']-df6['lng_avg']


# df6_result = df6.groupby(['city_name','comp'])['lng_count'].count().reset_index()

df6.to_csv("C:\\Users\\86133\\Desktop\\tmp.csv")




#In[]


###第一阶段


###读取excel到dataframe中
# df=pd.read_excel('C:\\Users\\86133\\Desktop\\newest_address_lnglat.xlsx',engine='openpyxl')
df=pd.read_excel('C:\\Users\\86133\\Desktop\\newest_city_lng_lat_empty_3.xlsx',engine='openpyxl')

###截取指定列
df = df[['newest_id','newest_name','city_name','address','change_lng_lat','Unnamed: 11']]
###过滤脏数据
df = df[~df['Unnamed: 11'].isnull()]
###修改列名
df.columns=['newest_id','newest_name','city_name','address','lng_lat','search_newest_name']
df['search_newest_name'] = df['search_newest_name'].map(lambda x:x. split('(')[0])
df['search_newest_name'] = df['search_newest_name'].map(lambda x:x. split('营销')[0])
###进行比较
df['name_com_rate'] = df.apply(lambda x:similarity(str(x.newest_name),x.search_newest_name),axis=1)
df


# %%
##基本准确的
df_true = df[df['name_com_rate']>0.8]
##不确定的
df_true_level_1 = df[(df['name_com_rate']<=0.8)&(df['name_com_rate']>=0.6)]
df_true_level_2 = df[(df['name_com_rate']<0.6)]
##错误的
df_false = df[df['name_com_rate']==0.0]

###加载数据
df_true.to_excel('C:\\Users\\86133\\Desktop\\df_true.xlsx')
df_true_level_1.to_excel('C:\\Users\\86133\\Desktop\\df_true_level_1.xlsx')
df_true_level_2.to_excel('C:\\Users\\86133\\Desktop\\df_true_level_2.xlsx')


#In[]
from pyspark.sql import SparkSession
import pandas as pd
import os

os.environ['JAVA_HOME'] = 'D:\\java\\bin'  # 这里的路径为java的bin目录所在路径
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# df = spark.read.format("com.crealytics.spark.excel") .option("useHeader", "true") .option("inferSchema", "true") .option("dataAddress", "NameOfYourExcelSheet").load("C:\\Users\\86133\\Desktop\\newest_city_lng_lat_city_4.xlsx")

# df=pd.read_excel('C:\\Users\\86133\\Desktop\\newest_city_lng_lat_city_4.xlsx',engine='openpyxl')
# sdf = spark.createDataFrame(df)
# sdf.show()
# spark.read.csv('C:\\Users\\86133\\Desktop\\newest_city_lng_lat_city_4.csv')


df = spark.read.load("C:\\Users\\86133\\Desktop\\newest_city_lng_lat_city_4.csv",
                     format="csv", sep=";", inferSchema="true", header="true")



