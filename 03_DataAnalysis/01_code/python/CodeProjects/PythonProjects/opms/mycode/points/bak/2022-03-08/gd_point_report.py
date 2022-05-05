# In[]:
#!/usr/bin/env python
# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Dec 07 15:44:47 2021 
    Changed  on Dec 20 99:00:00 2021
      凌晨的格林尼治时间转换问题，判断格林尼治时间为16时结果为0 并且天数+1，反之就+8.    关键字web_pages
      dwb_et_funnel新加一个客户浏览预览页标识
    Changed on Feb 10 15:29:00 2022
      2022-02-10 补充没有通过邀请码新增的用户
"""
import configparser,os,sys,pymysql,pandas as pd,getopt,time,numpy as np
from sqlalchemy import create_engine
from datetime import datetime, date, timedelta

##读取配置文件##
pymysql.install_as_MySQLdb()
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)  # 读取配置文件，如果写文件的绝对路径，就可以不用os模块

read_user = cf.get("Read_Mysql", "user")  # 获取user对应的值
read_password = cf.get("Read_Mysql", "password")  # 获取password对应的值
read_db_host = cf.get("Read_Mysql", "host")  # 获取host对应的值
read_database = cf.get("Read_Mysql", "database")  # 获取dbname对应的值
write_user = cf.get("Write_Mysql", "user")  # 获取user对应的值
write_password = cf.get("Write_Mysql", "password")  # 获取password对应的值
write_db_host = cf.get("Write_Mysql", "host")  # 获取host对应的值
write_database = cf.get("Write_Mysql", "database")  # 获取dbname对应的值
# date_time = (date.today() + timedelta(days = -2)).strftime("%Y-%m-%d")
date_time = '2022-02-26'

# -*- coding: utf-8 -*-
class MysqlClient:
    def __init__(self, read_db_host,read_database,read_user,read_password):
        """
        create connection to hive server
        """
        self.conn = pymysql.connect(host=read_db_host, user=read_user,password=read_password,database=read_database,charset="utf8")
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

def to_dws(result,table):
    engine = create_engine('mysql+mysqldb://'+write_user+':'+write_password+'@'+write_db_host+':'+'3306'+'/'+write_database+'?charset=utf8')
    result.to_sql(name = table,con = engine,if_exists = 'append',index = False,index_label = False)

con = MysqlClient(read_db_host,read_database,read_user,read_password) #链接读取的数据库
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  + '  reading data to : a_dwb_customer_browse_log') #程序开始时间
pages_date_time = date_time+"T16:00:00Z"   # 格林尼治时间
yesterday_date_time=(datetime.strptime(date_time, '%Y-%m-%d') + timedelta(days = 1)).strftime("%Y-%m-%d")  #昨天
loagoday_date_time=(datetime.strptime(date_time, '%Y-%m-%d') + timedelta(days = -1)).strftime("%Y-%m-%d")  #大前天
pages_yesterdaydate_time = yesterday_date_time+"T16:00:00Z"   # 格林尼治时间

# pages_date_time = '2022-01-05'+"T16:00:00Z"   # 格林尼治时间
# yesterday_date_time= '2022-01-05' #昨天
# loagoday_date_time='2022-01-04'  #大前天
# pages_yesterdaydate_time = yesterday_date_time+"T16:00:00Z"   # 格林尼治时间






# In[]
#######################################====dwb_et_user_region====############################################################
auth_user = con.query("select user_id,register_mobile,`type` user_type,dr,create_date login_date from odsdb.ods_gd_points_goingdata_auth_user where dr = 0 and register_mobile is not null") #用户基础表
auth_user_log = con.query("select user_id,login_date ,case when user_id in ('67809a85e01b4d7eae501a73a3aac1ff','e306c916391c408f9acab16bba70cd97') then 110000 when login_city_id = 450000 then 450100 when login_city_id = 440000 then 440100 when login_city_id = 330000 then 330100 when login_city_id = 220000 then 220100 else login_city_id end city_id,login_county_id region_id from odsdb.ods_gd_points_goingdata_auth_user_log where dr = 0 and  login_city_id!= '' and login_city_id is not null") #用户浏览日志表
dim_gra=con.query("select province_id ,province_name ,city_id ,city_name ,region_id ,region_name from dws_db_prd.dim_geography where region_id is not null group by province_id ,province_name ,city_id ,city_name ,region_id ,region_name")  #地域维度表
dim_gra_city = dim_gra.groupby(['province_id','province_name','city_id'])['city_name'].max().reset_index()#取省份和城市
dim_gra_city['region_id'] = np.nan
dim_gra_city['region_name'] = np.nan
dim_gra_city[['city_id']] = dim_gra_city[['city_id']].astype('str')   
dim_gra[['region_id']] = dim_gra[['region_id']].astype('str')  


# In[]:
##注册用户区域分布表
#取最早登录时间的区县
#历史脏数据处理
auth_user_log.at[auth_user_log['user_id'].isin([]),'login_city_id']=110000
auth_user_log.at[auth_user_log['login_city_id'] == '450000','login_city_id']='450100'
auth_user_log.at[auth_user_log['login_city_id'] == '440000','login_city_id']='440100'
auth_user_log.at[auth_user_log['login_city_id'] == '330000','login_city_id']='330100'
auth_user_log.at[auth_user_log['login_city_id'] == '220000','login_city_id']='220100'
#根据用户地理分组获取最小时间的数据
auth_userlog_min = auth_user_log.groupby(['user_id','city_id','region_id'])['login_date'].min().reset_index()  #取最小的登录时间
#区分region_id是否为空
auth_userlog_noregion = auth_userlog_min[auth_userlog_min['region_id'] == ''] #没有返回区县id
auth_user_log_min = auth_userlog_min[auth_userlog_min['region_id'] != '']#返回区县id
#使用分组排序,找到最早的那条数据
auth_user_log_min['last_login_date']=auth_user_log_min.sort_values(by=['user_id','login_date']).groupby(['user_id'])['login_date'].shift(1)
#找出最新的数据
auth_user_log_min = auth_user_log_min[auth_user_log_min['last_login_date'].isna()][['user_id','region_id']]
#匹配城市区县
auth_user_log_min[['region_id']] = auth_user_log_min[['region_id']].astype('str')
df_user_log = pd.merge(dim_gra,auth_user_log_min,how='inner',on=['region_id']) #地域信息合并
#判断region_id为空的数据是否已经找到了
auth_userlog_noregion = auth_userlog_noregion[~auth_userlog_noregion['user_id'].isin(df_user_log['user_id'])][['user_id','city_id','login_date']]
#使用分组排序,找到最早的那条数据
auth_userlog_noregion['last_login_date']=auth_userlog_noregion.sort_values(by=['user_id','login_date']).groupby(['user_id'])['login_date'].shift(1)
#找出最新的数据
auth_userlog_noregion = auth_userlog_noregion[auth_userlog_noregion['last_login_date'].isna()][['user_id','city_id']]
#没找到的匹配城市添加区县列
auth_userlog_noregion[['city_id']] = auth_userlog_noregion[['city_id']].astype('str')
auth_userlog_noregion = pd.merge(dim_gra_city,auth_userlog_noregion,how='right',on=['city_id']) #地域信息合并
#两张表append
df_user_log = df_user_log.append(auth_userlog_noregion,ignore_index=True)#合并登录时间
#和全量用户做匹配合并
df_user = pd.merge(auth_user,df_user_log,how='left',on=['user_id']) #合并最终结果
df_user['create_time'] = yesterday_date_time
df_user['update_time'] = yesterday_date_time
# df_user.to_csv(r'C:\Users\86133\Desktop\df_user.csv')
# df_user['login_date'] = df_user[['login_date']].astype(str)
# df_user = df_user[df_user['login_date'].str[0:10]<=yesterday_date_time]
# df_user_log = df_user_log[~df_user_log['login_date'].isna()]
# df_user[(datetime.strptime(df_user['login_date'], '%Y-%m-%d %HH:%MM:%SS') + timedelta(days = -1)).strftime("%Y-%m-%d").strftime("%Y-%m-%d") == yesterday_date_time]











#In[]


#########################################################################以下程序公用变量  过滤掉内部用户和老用户
df_user['login_date'] = df_user[['login_date']].astype(str)
df_user_no_1 = df_user[df_user['user_type']!=1]
df_user_new = df_user_no_1[df_user_no_1['login_date'].str[0:10]>='2021-12-06']














# In[]:
#######################################====dwb_et_user_invite_cc====############################################################
##依赖于auth_user_log
auth_user_invite = con.query("select invite_number,invite_code,invite_user_id user_id from odsdb.ods_gd_points_goingdata_auth_user_invite where dr = 0 ")#用户拥有激活码信息表  关联出激活码
auth_user_invite_used = con.query("select invite_number,register_user_id,substr(register_time,1,10) register_time from odsdb.ods_gd_points_goingdata_auth_user_invite_used where dr = 0 ") #激活码被使用情况信息表   主要表但是只有激活码id,需要关联出激活码
wechat_invite_code=con.query("select invite_code,wechat_group invite_code_channel from odsdb.ods_gd_points_wechat_invite_code group by invite_code,wechat_group")#激活码所在渠道信息表


# In[]:
##dwb_et_user_invite_cc
#过滤掉内部用户和老用户
df_invite_used = auth_user_invite_used[(auth_user_invite_used['register_user_id'].isin(df_user_new['user_id']))]
df_auth_user_invite_used = df_invite_used.groupby(['invite_number'])['register_user_id'].count().reset_index()
#合并
df_user_invite = pd.merge(auth_user_invite,df_auth_user_invite_used,how='left',on=['invite_number'])
df_invite = pd.merge(wechat_invite_code,df_user_invite,how='right',on=['invite_code'])
df_invite = df_invite.rename(columns={'register_user_id':'invite_code_num'})  #修改列名
df_invite = df_invite[['invite_code','invite_code_channel','user_id','invite_code_num']]
df_invite['create_time'] = yesterday_date_time
df_invite['update_time'] = yesterday_date_time

# df_invite.to_csv(r'C:\Users\86133\Desktop\df_invite.csv')
#去重
# df_auth_user_invite = auth_user_invite.groupby(['user_id','invite_code'])['invite_number'].max().reset_index()
# df_auth_user_invite_used = df_invite_used.groupby(['invite_number','register_user_id'])['register_time'].max().reset_index()
#统计
# df_invite = df_user_invite.groupby(['user_id','invite_code'])['register_user_id'].count().reset_index()
# df_invite_used = df_invite_used.groupby(['invite_number','register_time'])['register_user_id'].count().reset_index()#分日期统计激活码使用数量
# df_auth_user_invite_used = df_invite_used[df_invite_used['register_time'] <= yesterday_date_time]#筛选














#In[]
#######################################====dwb_et_user_browse_log====############################################################
####依赖  auth_user!!!!!!!!!!!!!!!!!!!!!
web_pages = con.query("select ods_id,concat('"+yesterday_date_time+"',' ',LPAD(case when substr(create_time,12,2) = 16 then '00' when substr(create_time,12,2) > 16 then substr(create_time,12,2)-16 else  substr(create_time,12,2)+8 end,2,'0') ,substr(create_time,14,6)) as date_time,url,app_mobile register_mobile,track_behaviorTag from odsdb.ods_gd_points_web_pages where create_time >= '"+pages_date_time+"' and create_time <'"+pages_yesterdaydate_time+"' and app_mobile is not null and dr = 0 group by ods_id,create_time,url,app_mobile,track_behaviorTag")#用户浏览url


# In[]:
df_web_pages = web_pages.groupby(['ods_id','url','register_mobile'])['date_time'].max().reset_index()#根据原始id对现有的拆分后的数据进行去重
##dwb_et_user_browse_log
#截取列
df_browse_log_user = df_user_no_1[['user_id','register_mobile']]#获取内部用户的用户和手机号码
#过滤掉内部用户的浏览记录
df_browse_log=pd.merge(df_browse_log_user,df_web_pages,how='left',on=['register_mobile'])#根据浏览表的手机号关联到user_id
#过滤没有浏览的用户
df_browse_log=df_browse_log[~df_browse_log['url'].isna()]
#添加url模块
df_browse_log['url_model'] = np.nan
df_browse_log.at[df_browse_log['url'].str.contains("/insight/#/") ,'url_model'] = '客户洞察'
df_browse_log.at[(df_browse_log['url'].str.contains("/#/myOrder")) | (df_browse_log['url'].str.contains("/#/myorder")) ,'url_model'] = '我的订单'
df_browse_log.at[df_browse_log['url'].str.contains("/ranking/#/") ,'url_model'] = '榜单模块'
df_browse_log.at[df_browse_log['url'].str.contains("/insight/report/#/") ,'url_model'] = '报告模块'
df_browse_log.at[df_browse_log['url'].str.contains("/#/portal") ,'url_model'] = '模块选择'
df_browse_log.at[df_browse_log['url'].str.contains("/insight/report/#/dashboard") ,'url_model'] = '项目预览'
df_browse_log.at[df_browse_log['url'].str.contains("/#/login") ,'url_model'] = '登录首页'
df_browse_log.at[df_browse_log['url'].str.contains("/#/order-detail") ,'url_model'] = '订单详情'
df_browse_log=df_browse_log[['user_id','register_mobile','date_time','url','url_model']]
df_browse_log['create_time'] = yesterday_date_time
df_browse_log['update_time'] = yesterday_date_time
# df_browse_log.to_csv(r'C:\Users\86133\Desktop\df_browse_log.csv')

















#In[]
#######################################====dwb_et_funnel====############################################################
####依赖  auth_user  !!!!!!!!!!!!!!!!!!!!!  和  auth_user_log   !!!!!!!!!!!!!!!!!!!!!
tra_order_all = con.query("select order_id,user_id,complete_time from odsdb.ods_gd_points_goingdata_tra_tra_order where payment_status = 1 and dr = 0 and substr(complete_time,1,10) <= '"+yesterday_date_time+"' group by order_id,user_id,complete_time")
look_report_mob = con.query("select app_mobile register_mobile,1  look_report from odsdb.ods_gd_points_web_pages where dr = 0 and url like '%/insight/report/#/dashboard%' and create_time < '"+pages_yesterdaydate_time+"'")

#In[]
##dwb_et_funnel
#过滤掉内部用户
df_et_funnel = df_user_new[['user_id','register_mobile','login_date']]
df_et_funnel['login_index'] = 1
#统计
df_tra_order = tra_order_all.groupby(['user_id'])['order_id'].count().reset_index()
#判断
df_tra_order['buy_first_report']=np.nan
df_tra_order['buy_thired_report']=np.nan
df_tra_order.at[df_tra_order['order_id']>=1,'buy_first_report']=1
df_tra_order.at[df_tra_order['order_id']>=3,'buy_thired_report']=1
#合并
df_et_funnel = pd.merge(df_et_funnel,df_tra_order,how='left',on=['user_id'])
df_et_funnel.at[df_et_funnel['buy_first_report'].isna(),'buy_first_report']=2
df_et_funnel.at[df_et_funnel['buy_thired_report'].isna(),'buy_thired_report']=2
##2021-12-20新增 look_report
look_report_mob = look_report_mob.groupby(['register_mobile'])['look_report'].max().reset_index()
df_et_funnel = pd.merge(df_et_funnel,look_report_mob,how='left',on=['register_mobile'])
df_et_funnel.at[df_et_funnel['look_report']!=1 ,'look_report'] = 2
#截取列
df_et_funnel = df_et_funnel[['user_id','login_date','login_index','buy_first_report','buy_thired_report','look_report']]
df_et_funnel = df_et_funnel[df_et_funnel['login_date'].str[0:10] <= yesterday_date_time]#筛选
df_et_funnel['create_time'] = yesterday_date_time
df_et_funnel['update_time'] = yesterday_date_time




















#In[]
#######################################====dwb_et_user_log_order====############################################################
####依赖  auth_user  和  auth_user_log   !!!!!!!!!!!!!!!!!!!!!
tra_order = con.query("select order_id,user_id from odsdb.ods_gd_points_goingdata_tra_tra_order where payment_status = 1 and dr = 0 and substr(complete_time,1,10)='"+yesterday_date_time+"' group by order_id,user_id")#订单完成表
tra_order = tra_order[(tra_order['user_id'].isin(df_user_new['user_id']))]#过滤掉内部用户和老用户


#In[]
##dwb_et_user_log_order
########当天浏览的用户
df_log_order_user_log = auth_user_log[auth_user_log['login_date'].str[0:10] == yesterday_date_time].groupby(['user_id'])['login_date'].max().reset_index()#获取当天交易的用户
df_browse_userlog = pd.merge(df_user_new,df_log_order_user_log,how='inner',on=['user_id'])#浏览表关联出新的外部用户的地理
df_order_userlog = pd.merge(df_user_new,tra_order,how='inner',on=['user_id'])#交易表关联出新的外部用户的地理
#过滤掉城市为空的
df_browse_num=df_browse_userlog[~df_browse_userlog['city_id'].isna()]
df_order_num=df_order_userlog[~df_order_userlog['city_id'].isna()]
######浏览量
#城市
df_browse_num_city = df_browse_num.groupby(['province_id','province_name','city_id','city_name'])['user_id'].count().reset_index()
#省份
df_browse_num_province=df_browse_num.groupby(['province_id','province_name'])['user_id'].count().reset_index()
######成交量
#城市
df_order_num_city = df_order_num.groupby(['province_id','province_name','city_id','city_name'])['order_id'].count().reset_index()
#省份
df_order_num_province=df_order_num.groupby(['province_id','province_name'])['order_id'].count().reset_index()
df_order_city = pd.merge(df_browse_num_city,df_order_num_city,how='left',on=['province_id','province_name','city_id','city_name'])
df_order_province = pd.merge(df_browse_num_province,df_order_num_province,how='left',on=['province_id','province_name'])
#区县为空：城市汇总  城市和区县为空: 省份汇总
df_order_city['region_id']=np.nan
df_order_city['region_name']=np.nan
df_order_province['city_id']=np.nan
df_order_province['city_name']=np.nan
df_order_province['region_id']=np.nan
df_order_province['region_name']=np.nan
#合并
df_log_order = df_order_city[['province_id','province_name','city_id','city_name','region_id','region_name','user_id','order_id']].append([df_order_province[['province_id','province_name','city_id','city_name','region_id','region_name','user_id','order_id']]],ignore_index=True)
#添加列
df_log_order['date_time'] = yesterday_date_time
df_log_order['create_time'] = yesterday_date_time
df_log_order['update_time'] = yesterday_date_time
#改列名
df_log_order = df_log_order.rename(columns={'user_id':'browse_num','order_id':'order_num'})
#区县
# df_browse_num_region = df_browse_num.groupby(['province_id','province_name','city_id','city_name','region_id','region_name'])['user_id'].count().reset_index()
#区县
# df_order_num_region = df_order_num.groupby(['province_id','province_name','city_id','city_name','region_id','region_name'])['order_id'].count().reset_index()
#####省市区初步合并
# df_order_region = pd.merge(,df_order_num_region,how='left',on=['province_id','province_name','city_id','city_name','region_id','region_name'])















#In[]
#######################################====dwb_et_day_report====############################################################
####依赖   wechat_invite_code 和  auth_user 和 df_et_funnel !!!!!!!!!!!!!!!!!!!!!
# dwb_et_day_report
##获取所有渠道名称和user_id的映射关系
df_othor_user_invite_used = auth_user[(auth_user['user_type'] != 1)&(auth_user['login_date'] >= '2021-12-06')&(~auth_user['user_id'].isin(auth_user_invite_used['register_user_id']))][['user_id']]#找出未通过邀请码新增的用户
df_othor_user_invite_used['invite_number'] = '00000000000'   ##给这些用户复制一个不存在的邀请码
df_othor_user_invite_used.rename(columns={'user_id':'register_user_id'},inplace=True) ##修改列名
df_etday_report = auth_user_invite_used[auth_user_invite_used['register_user_id'].isin(auth_user[auth_user['login_date'] >= '2021-12-06']['user_id'])][['invite_number','register_user_id']].append(df_othor_user_invite_used) ##合并到从邀请码表中读取的数据
df_etday_report = pd.merge(df_etday_report,auth_user_invite[['invite_number','invite_code']],how='left',on=['invite_number'])#关联code
df_etday_report = pd.merge(wechat_invite_code,df_etday_report,how='right',on=['invite_code'])#关联渠道
df_etday_report.at[df_etday_report['invite_code_channel'].isna(),'invite_code_channel']='其他渠道'#赋值其他渠道

# auth_user['date_time'] = auth_user['login_date'].apply(lambda x:str(x)[:10])
###确定邀请码的总数和增量

# df_othor_user_invite_used = df_othor_user_invite_used.groupby(['invite_number'])['register_user_id'].count().reset_index() ##统计总数量
# df_etday_report = df_auth_user_invite_used.append(df_othor_user_invite_used) ##合并到从邀请码表中读取的数据
# df_etday_report.columns=['invite_number','sum_cust_num']#修改列名
# df_etday_report = pd.merge(df_etday_report,auth_user_invite[['invite_number','invite_code']],how='left',on=['invite_number'])#关联code
# df_etday_report = pd.merge(wechat_invite_code,df_etday_report,how='right',on=['invite_code'])#关联渠道
# df_etday_report.at[df_etday_report['invite_code_channel'].isna(),'invite_code_channel']='其他渠道'#赋值其他渠道
# df_etday_report = df_etday_report.groupby(['invite_code_channel'])['sum_cust_num'].sum().reset_index() ##统计总数量
# # df_etday_report = pd.merge(df_auth_user_invite_used,df_invite_used[df_invite_used['register_time'] == yesterday_date_time],how='left',on=['invite_number'])#获取预售证的总用户数和当天新增的用户id


###获取用户的注册日期，看的页面，买过几个报告，最近三天是否有登录
df_day_report_today = df_et_funnel[['user_id','login_date']]#所有用户的漏斗标签
df_day_report_today['login_today_index']=np.nan
df_day_report_today.at[df_day_report_today['login_date'].str[0:10] == yesterday_date_time , 'login_today_index']=1#新增用户标识
df_day_report_today = df_day_report_today[df_day_report_today['login_today_index'] == 1]
#用户当天交易数量
df_report_traorder = tra_order_all[tra_order_all['complete_time'].str[0:10] == yesterday_date_time].groupby(['user_id'])['order_id'].count().reset_index()#计算数量
df_report_traorder['buy_first_report'] = np.nan
df_report_traorder['buy_thired_report'] = np.nan 
if len(df_report_traorder) == 0 :
    pass
else :
    df_report_traorder.at[df_report_traorder['order_id']>= 1 , 'buy_first_report']=1
    df_report_traorder.at[df_report_traorder['order_id']>= 3 , 'buy_thired_report']=1
#打标签
#看的页面
df_day_report_browae = df_browse_log[df_browse_log['url_model'] == '项目预览'][['user_id','url','url_model']].drop_duplicates()#记录用户查看报告预览的次数,并对相同报告去重
df_day_report_browae.at[df_day_report_browae['url_model']=='项目预览','url_model']=1#对用户打标签
df_day_report_browae = df_day_report_browae[['user_id','url_model']].drop_duplicates()
# 最近三天是否有登录
auth_userlog_max = auth_user_log.groupby(['user_id','region_id'])['login_date'].max().reset_index()#获取用户最新登录时间
df_day_report_userlog = auth_userlog_max[auth_userlog_max['login_date'].str[0:10]>=loagoday_date_time][['user_id']].drop_duplicates()#筛选是否大于等于往前第三天的时间
df_day_report = pd.merge(df_etday_report[['invite_code_channel','register_user_id']],df_day_report_browae,how='left',left_on=['register_user_id'],right_on=['user_id'])#两个标签合并
df_day_report = pd.merge(df_day_report,df_day_report_today,how='left',left_on=['register_user_id'],right_on=['user_id'])[['invite_code_channel','register_user_id','url_model','login_today_index']]#两个标签合并
df_day_report['lost_index'] = np.nan
df_day_report.at[~df_day_report['register_user_id'].isin(df_day_report_userlog['user_id']),'lost_index']=1#打丢失标签
df_day_report = pd.merge(df_day_report,df_report_traorder,how='left',left_on=['register_user_id'],right_on=['user_id'])


# #######各渠道总登录用户
# df_report_sum = df_etday_report[['invite_code_channel','invite_number','new_cust_num_id']]#获取当天新增明细
# df_report_sum.at[df_report_sum['invite_code_channel'].isna(),'invite_code_channel']='其他渠道'#赋值其他渠道
# user_type_not_invites_today = pd.merge(auth_user[auth_user['user_type'] == '0'][['user_id']],df_day_report[df_day_report['login_today_index']==1],how='inner',on=['user_id'])[['user_id']] #找出未通过邀请码新增的用户
# user_type_not_invites_today['invite_code_channel'] = '其他渠道'
# user_type_not_invites_today['invite_number'] = '非邀请码用户'
# user_type_not_invites_today.rename(columns={'user_id':'new_cust_num_id'},inplace=True)
# df_report_sum = df_report_sum.append(user_type_not_invites_today[['invite_code_channel','invite_number','new_cust_num_id']]) # 2022-02-10 补充没有通过邀请码新增的用户
# df_report_new =  df_report_sum.groupby(['invite_code_channel'])['new_cust_num_id'].count().reset_index()#统计各渠道的新增用户数量
# df_reportsum_sum = pd.merge(df_auth_user_invite_used,auth_user_invite,how='left',on=['invite_number'])#总数关联code
# df_reportsum_sum = pd.merge(df_reportsum_sum,wechat_invite_code,how='left',on=['invite_code'])#关联渠道
# df_reportsum_sum.at[df_reportsum_sum['invite_code_channel'].isna(),'invite_code_channel']='其他渠道'#赋值其他渠道
# user_type_not_invites = auth_user[(auth_user['user_type'] != '1')&(auth_user['login_date'] >= '2021-12-06')&(~auth_user['user_id'].isin(df_invite_used['register_user_id']))][['user_id']]#找出未通过邀请码新增的用户
# user_type_not_invites['invite_code_channel'] = '其他渠道'
# user_type_not_invites['invite_number'] = '非邀请码用户'
# user_type_not_invites['register_user_id'] = 1
# user_type_not_invites['invite_code'] = '非邀请码用户'
# df_reportsum_sum = df_reportsum_sum.append(user_type_not_invites[['invite_number','register_user_id','invite_code','user_id','invite_code_channel']]) # 2022-02-10 补充没有通过邀请码新增的用户
# df_reportsum_sum =  df_reportsum_sum.groupby(['invite_code_channel'])['register_user_id'].sum().reset_index()#统计各渠道的新增用户数量
# #合并邀请渠道
# df_report = pd.merge(auth_user_invite_used[(auth_user_invite_used['register_user_id'].isin(df_user_new['user_id']))].rename(columns={'register_user_id':'user_id'}),df_day_report,how='right',on=['user_id'])#合并标签
# df_report = pd.merge(auth_user_invite[['invite_number','invite_code']],df_report,how='right',on=['invite_number'])#联code
# df_report = pd.merge(wechat_invite_code,df_report,how='right',on=['invite_code'])#关联渠道
# df_report.at[df_report['invite_code_channel'].isna(),'invite_code_channel']='其他渠道'#赋值其他渠道
# df_report = pd.merge(df_report_traorder,df_report,how='right',on=['user_id'])#关联渠道
# #各个渠道的浏览预览页总数
# df_report_model_sum = df_report[df_report['url_model'] == 1 ].groupby(['invite_code_channel'])['url_model'].sum().reset_index()
# #######购买一个报告用户
# df_report_first_sum = df_report[df_report['buy_first_report'] == 1 ].groupby(['user_id','invite_code_channel'])['buy_first_report',].max().groupby(['invite_code_channel'])['buy_first_report'].sum().reset_index()
# #######购买三个报告用户
# df_report_third_sum = df_report[df_report['buy_thired_report'] == 1 ].groupby(['user_id','invite_code_channel'])['buy_thired_report',].max().groupby(['invite_code_channel'])['buy_thired_report'].sum().reset_index()
# #######流失用户
# df_report_lost_sum = df_report[df_report['lost_index'] == 1 ].groupby(['invite_code_channel'])['lost_index'].sum().reset_index()

df_reportsum_sum = df_day_report.groupby(['invite_code_channel'])['register_user_id'].count().reset_index() ##计算总用户数量
df_report_new = df_day_report.groupby(['invite_code_channel'])['login_today_index'].count().reset_index() ##计算新增用户数量
df_report_model_sum = df_day_report.groupby(['invite_code_channel'])['url_model'].count().reset_index() ##计算动作过的用户数量
df_report_first_sum = df_day_report.groupby(['invite_code_channel'])['buy_first_report'].count().reset_index() ##计算当天买了一份报告的用户数量
df_report_third_sum = df_day_report.groupby(['invite_code_channel'])['buy_thired_report'].count().reset_index() ##计算当天买了三份报告的用户数量
df_report_lost_sum = df_day_report.groupby(['invite_code_channel'])['lost_index'].count().reset_index() ##计算截止当天没有登录三天的用户数量
####合并结果
df_report_result = pd.merge(df_reportsum_sum,df_report_new,how='right',on=['invite_code_channel'])
df_report_result = pd.merge(df_report_result,df_report_model_sum,how='left',on=['invite_code_channel'])
df_report_result = pd.merge(df_report_result,df_report_first_sum,how='left',on=['invite_code_channel'])
df_report_result = pd.merge(df_report_result,df_report_third_sum,how='left',on=['invite_code_channel'])
df_report_result = pd.merge(df_report_result,df_report_lost_sum,how='left',on=['invite_code_channel'])
#替换空
df_report_result.at[df_report_result['register_user_id']==0,'register_user_id']=np.nan
df_report_result.at[df_report_result['login_today_index']==0,'login_today_index']=np.nan
df_report_result.at[df_report_result['url_model']==0,'url_model']=np.nan
df_report_result.at[df_report_result['buy_first_report']==0,'buy_first_report']=np.nan
df_report_result.at[df_report_result['buy_thired_report']==0,'buy_thired_report']=np.nan
df_report_result.at[df_report_result['lost_index']==0,'lost_index']=np.nan
# df_report_result
df_report_result = df_report_result.rename(columns={'register_user_id':'sum_cust_num','login_today_index':'new_cust_num','url_model':'show_report_cust_num','buy_first_report':'active_cust_num','buy_thired_report':'active_super_cust_num','lost_index':'lost_cust_num'})  #修改列名
df_report_result = df_report_result[['invite_code_channel','sum_cust_num','new_cust_num','show_report_cust_num','active_cust_num','active_super_cust_num','lost_cust_num']]
df_report_result['date_time'] = yesterday_date_time
df_report_result['create_time'] = yesterday_date_time
df_report_result['update_time'] = yesterday_date_time
# # 判断使用的邀请码是不是在那几个邀请码之内
# df_et_day_report['channel_beyang_index'] = np.nan
# df_et_day_report.at[df_et_day_report['invite_code'].isin(wechat_invite_code['invite_code']),'channel_beyang_index']=1
# df_et_day_report.at[df_et_day_report['channel_beyang_index'].isna(),'channel_beyang_index']=2
# df_et_day_report=df_et_day_report[['invite_code_channel','register_user_id','channel_beyang_index']]
####排除老用户
# df_day_report=df_day_report[df_day_report['login_date'].str[0:10]>'2021-12-05']
####排除老用户
# df_day_report=df_day_report[df_day_report['login_date'].str[0:10]>'2021-12-05']
#改列名
# df_et_day_report = df_etday_report.rename(columns={'register_user_id_x':'invite_code_num','register_user_id_y':'login_today_index'})
# df_report_sum = df_report_sum.groupby(['invite_code_channel'])['sum_cust_num'].max().reset_index()
# #######非n个邀请码的用户量
# df_report_notin_sum = df_report[df_report['channel_beyang_index'] != 1 ].groupby(['invite_code_channel'])['channel_beyang_index'].count().reset_index()
# #######非n个邀请码的用户截止当前 解锁1份报告的用户数
# df_report_notin_first_sum = df_report[(df_report['channel_beyang_index'] != 1) & (df_report['buy_first_report'] != 1) ].groupby(['invite_code_channel'])['channel_beyang_index'].count().reset_index()
# #######非n个邀请码的用户截止当前 解锁3份报告的用户数
# df_report_notin_third_sum = df_report[(df_report['channel_beyang_index'] != 1) & (df_report['buy_thired_report'] == 1) ].groupby(['invite_code_channel'])['channel_beyang_index'].count().reset_index()
# df_report_result = pd.merge(df_report_result,df_report_notin_sum,how='left',on=['invite_code_channel'])
# df_report_result = pd.merge(df_report_result,df_report_notin_first_sum,how='left',on=['invite_code_channel'])
# df_report_result = pd.merge(df_report_result,df_report_notin_third_sum,how='left',on=['invite_code_channel'])




















#In[]
#######################################====dwb_et_user_action_count====############################################################
#### 依赖于 df_user web_pages 和 df_et_funnel 和 df_browse_log ！！！！！！！！！！！！！！
points_web_pvuvip = con.query("select '"+yesterday_date_time+"' date_time,pv from odsdb.ods_gd_points_web_pvuvip where `type` = 2 and create_time='"+pages_date_time+"'")#获取最新一天的pv数据
today_user_log = con.query("select user_id,min(login_date) login_today_date from odsdb.ods_gd_points_goingdata_auth_user_log where substr(login_date,1,10) = '"+yesterday_date_time+"' group by user_id union select user_id,max(login_date) login_today_date from odsdb.ods_gd_points_goingdata_auth_user_log where substr(login_date,1,10) < '"+yesterday_date_time+"' group by user_id")#获取用户当天的最早登录时间，合并 用户没有在当天登录的最大时间
perfor_web_pages = con.query("select url,visit_time date_time ,behavior_tag_code,user_mobile register_mobile from odsdb.ods_gd_points_goingdata_perforweb_pages where dr = 0 and visit_date = '"+yesterday_date_time+"'")#客户浏览日志表


 
#In[]
# dwb_et_user_action_count
#最后记录的访问时间
df_action_pages = web_pages[['date_time','register_mobile']].groupby(['register_mobile'])['date_time'].max().reset_index()
#去重
df_log_user = today_user_log.groupby(['user_id'])['login_today_date'].max().reset_index()
#和当天登录时间做合并
df_action_pages = pd.merge(df_action_pages,df_user_new,how='inner',on=['register_mobile'])#获取外部用户的id

if len(df_action_pages) != 0 :
    df_action_pages = pd.merge(df_action_pages[~df_action_pages['user_id'].isna()],df_log_user,how='left',on=['user_id'])#过滤内部用户，连表取用户登录时间
    #计算时间差
    df_action_time = df_action_pages[['date_time','login_today_date']]
    #过滤无效登录时间
    df_action_time = df_action_time[~df_action_time['login_today_date'].isna()]
    df_action_time['diff'] = (df_action_time['date_time'].apply(lambda x:time.mktime(time.strptime(x,'%Y-%m-%d %H:%M:%S')))).astype(int) - (df_action_time['login_today_date'].apply(lambda x:time.mktime(time.strptime(x,'%Y-%m-%d %H:%M:%S')))).astype(int)
    df_action_time['date_time'] = yesterday_date_time
    #求和
    df_action_time = df_action_time.groupby(['date_time'])['diff'].sum().reset_index()
    #求平均时间
    df_action_pages['date_time'] = yesterday_date_time
    df_action_page = df_action_pages.groupby(['date_time'])['user_id'].count().reset_index()
    df_action_time = pd.merge(df_action_page,df_action_time,how='inner',on=['date_time'])
    df_action_time['avg_url_time']=int(df_action_time['diff']/df_action_time['user_id'])
    #求平均页面数
    df_action_time = pd.merge(df_action_time,points_web_pvuvip,how='inner',on=['date_time'])
    df_action_time['avg_url_num']=int(df_action_time['pv']/df_action_time['user_id'])
    # 查看报告跳出率
    df_user_new['date_time'] = yesterday_date_time
    df_action_new = df_user_new.groupby(['date_time'])['user_id'].count().reset_index()  ## 总的注册数量
    df_action_new = df_action_new.rename(columns={'user_id':'user_id_sum'})
    df_action_funnel = df_et_funnel[['user_id','buy_first_report']]
    df_action_funnel = pd.merge(df_action_pages,df_action_funnel,how='left',on=['user_id'])
    df_action_sum = df_action_funnel[df_action_funnel['buy_first_report'] == 1 ].groupby(['date_time'])['buy_first_report'].sum().reset_index()  ##首次购买人数
    df_action_time = pd.merge(df_action_time,df_action_sum,how='left',on=['date_time'])
    df_action_time = pd.merge(df_action_time,df_action_new,how='inner',on=['date_time'])
    df_action_time['jump_rate'] = 1 - df_action_time['buy_first_report']/df_action_time['user_id_sum']
    #模块点击量
    df_action_browse_log = df_browse_log[df_browse_log['url_model'] == '项目预览'][['date_time','url_model']] #项目预览页点击量
    df_action_browse_log['date_time'] = yesterday_date_time
    df_action_browse_log = df_action_browse_log.groupby(['date_time'])['url_model'].count().reset_index()
    df_action_time = pd.merge(df_action_time,df_action_browse_log,how='left',on=['date_time'])
    df_action_time = df_action_time.rename(columns={'url_model':'est_show_num'})
    #其他模块点击量
    # web_pages_new = pd.merge(web_pages,perfor_web_pages,how='right',on=['url','register_mobile'])
    # web_pages_new.to_csv(r'C:\Users\86133\Desktop\web_pages_new.csv')
    web_pages_new = pd.merge(web_pages,perfor_web_pages,how='right',on=['url','register_mobile','date_time'])
    # web_pages_new[(web_pages_new['behavior_tag_code'] == '') &(web_pages_new['track_behaviorTag'].str[0:3] == 'CKB' )]
    # web_pages_new[~web_pages_new['ods_id'].isna()].to_csv(r'C:\Users\86133\Desktop\web_pages_new.csv')
    web_pages_new.at[web_pages_new['behavior_tag_code'] == '' , 'behavior_tag_code'] = web_pages_new['track_behaviorTag']
    df_actioncount_pages = web_pages_new[~web_pages_new['behavior_tag_code'].isna()]
    df_actioncount_pages = df_actioncount_pages.groupby(['behavior_tag_code'])['register_mobile'].count().reset_index()
    #找项目点击量
    df_actioncount_zxm = df_actioncount_pages[df_actioncount_pages['behavior_tag_code'].str[0:3] == 'ZXM']#找项目
    df_actioncount_zxm['behavior_tag_code'] = 'ZXM'
    df_actioncount_zxm = df_actioncount_zxm.groupby('behavior_tag_code')['register_mobile'].sum().reset_index()
    df_action_time['find_est_num'] = df_actioncount_zxm[df_actioncount_zxm.behavior_tag_code=='ZXM'].reset_index()[['register_mobile']]
    df_action_time['find_est_search_num'] = df_actioncount_pages[df_actioncount_pages.behavior_tag_code== 'ZXM0002'].reset_index()[['register_mobile']]#查询
    df_action_time['find_est_choise_num'] = df_actioncount_pages[df_actioncount_pages.behavior_tag_code == 'ZXM0001'].reset_index()[['register_mobile']]#搜索
    df_actioncount_zxmrn = df_actioncount_pages[(df_actioncount_pages.behavior_tag_code == 'ZXM0003') | (df_actioncount_pages['behavior_tag_code'].str == 'ZXM0004')]#榜单
    df_actioncount_zxmrn['behavior_tag_code'] = 'ZXM_RN'
    df_actioncount_zxmrn = df_actioncount_zxmrn.groupby('behavior_tag_code')['register_mobile'].sum().reset_index()
    df_action_time['find_est_rownum_num'] = df_actioncount_zxmrn[df_actioncount_zxmrn.behavior_tag_code == 'ZXM_RN'].reset_index()[['register_mobile']]
    df_actioncount_zrq = df_actioncount_pages[df_actioncount_pages.behavior_tag_code.str[0:3] == 'ZRQ']#找人群
    df_actioncount_zrq['behavior_tag_code'] = 'ZRQ'
    df_actioncount_zrq = df_actioncount_zrq.groupby('behavior_tag_code')['register_mobile'].sum().reset_index()
    df_action_time['find_people_num'] = df_actioncount_zrq[df_actioncount_zrq.behavior_tag_code=='ZRQ'].reset_index()[['register_mobile']]
    df_action_time['est_show_cust_count_num'] = df_actioncount_pages[df_actioncount_pages.behavior_tag_code == 'CKBG0005'].reset_index()[['register_mobile']].astype(int)#报告的客流统计模块点击量
    df_action_time['est_show_portrait_num'] = df_actioncount_pages[df_actioncount_pages.behavior_tag_code == 'CKBG0006'].reset_index()[['register_mobile']]#报告的画像模块点击量
    df_action_time['est_show_compete_num'] = df_actioncount_pages[df_actioncount_pages.behavior_tag_code == 'CKBG0007'].reset_index()[['register_mobile']]#报告的竞品概览模块点击量
    df_action_time['est_show_compete_link_num'] = df_actioncount_pages[df_actioncount_pages.behavior_tag_code == 'CKBG0008'].reset_index()[['register_mobile']]#报告的竞品关系模块点击量

else :
    df_action_time = pd.DataFrame(columns=['date_time','pv','avg_url_num','avg_url_time','jump_rate','find_est_num','find_est_search_num','find_est_choise_num','find_est_rownum_num','find_people_num','est_show_num','est_show_cust_count_num','est_show_portrait_num','est_show_compete_num','est_show_compete_link_num'])
    new=pd.DataFrame({'date_time':yesterday_date_time},index=[0])
    df_action_time=df_action_time.append(new,ignore_index=True) 


df_action_time_result = df_action_time[['date_time','pv','avg_url_num','avg_url_time','jump_rate','find_est_num','find_est_search_num','find_est_choise_num','find_est_rownum_num','find_people_num','est_show_num','est_show_cust_count_num','est_show_portrait_num','est_show_compete_num','est_show_compete_link_num']]
df_action_time_result['date_time'] = yesterday_date_time
df_action_time_result['dr'] = 0 
df_action_time_result['create_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
df_action_time_result['update_time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 

# df_action_pages = df_action_pages[df_action_pages['login_today_date'].isna()]





#In[]
#加载数据
to_dws(df_user,'dwb_et_user_region')
print('>>> load data to dwb_et_user_region Done')


#In[]
#加载数据
to_dws(df_invite,'dwb_et_user_invite_cc')
print('>>> load data to dwb_et_user_invite_cc Done')


#In[]
#加载数据
to_dws(df_browse_log,'dwb_et_user_browse_log')
print('>>> load data to dwb_et_user_browse_log Done')



#In[]
#加载数据
to_dws(df_log_order,'dwb_et_user_log_order')
print('>>> load data to dwb_et_user_log_order Done')



#In[]
#加载数据
to_dws(df_et_funnel,'dwb_et_funnel')
print('>>> load data to dwb_et_funnel Done')



#In[]
#加载数据
to_dws(df_report_result,'dwb_et_day_report')
print('>>> load data to dwb_et_day_report Done')




#In[]
#加载数据
to_dws(df_action_time_result,'dwb_et_user_action_count')
print('>>> load data to dwb_et_user_action_count Done')


