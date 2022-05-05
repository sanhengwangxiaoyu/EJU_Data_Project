# coding: utf-8
# -*- coding: utf-8 -*-
"""
Created on Feb 08 14:53:31 2022
"""
import configparser,os,sys,pymysql,pandas as pd,numpy as np,re,time
from sqlalchemy import create_engine
from dateutil.parser import parse
### 读取配置文件
cf = configparser.ConfigParser()
path = os.path.abspath(os.curdir)
confpath = path + "/conf/config4.ini"
cf.read(confpath)
##设置配置信息##
user = cf.get("Mysql", "user")  # 获取user对应的值
password = cf.get("Mysql", "password")  # 获取password对应的值
db_host = cf.get("Mysql", "host")  # 获取host对应的值
database = cf.get("Mysql", "database")  # 获取dbname对应的值
###创建mysql客户端类
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
        cur.close()
        return res,columnNames
    def close(self):
        self.conn.close()

pymysql.install_as_MySQLdb()
engine = create_engine('mysql+mysqldb://'+user+':'+password+'@'+db_host+':'+'3306'+'/'+database)# 初始化引擎



"""
通过 居住小区的价格和是否有车有房毕业以及使用app的偏好来判断客户的价值高低

"""
periods = ['2021Q4']
for period in periods:
    tag_period = period
    if period in ('2018Q1','2018Q2','2018Q3','2018Q4','2019Q1','2019Q2','2019Q3','2019Q4','2020Q1','2020Q2'):
        tag_period = '2020Q3'
    con = MysqlClient(db_host,database,user,password) #实现数据库连接
    oprtion_start_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    start_date_month = str(pd.to_datetime(period))[0:7].replace('-','')  ##根据季度获取季度开始时间
    end_date_month = ''.join(re.findall(r"\d+\.?\d*",str(pd.to_datetime(str(pd.to_datetime(period))[0:10]) + pd.offsets.QuarterEnd(0))[0:7])) ##根据季度获取季度结束时间
    """
        连接数据库获取数据: 
           dws_db_prd.dws_newest_layout
           dwb_db.dwb_customer_lookest_list_m
           dwb_db.a_dwb_customer_browse_log
           dwb_db.b_dwb_customer_imei_tag
    """
    res,columnNames  = con.query("select newest_id,min(layout_price) layout_price from dws_db_prd.dws_newest_layout where dr = 0 and layout_price is not null group by newest_id")
    layout_price = pd.DataFrame([list(i) for i in res],columns=columnNames)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+" .读取表完毕: dws_db_prd.dws_newest_layout. 数据概述:")
    print(layout_price)
    res,columnNames  = con.query("select imei ,have_car ,have_house ,is_college_stu , \
                                        case when income_level is null then '高' else income_level end income_level , \
                                        case when app_prefer is null then '暂无' else app_prefer end app_prefer , \
                                        case when resi_district_price is null then '0' else resi_district_price end resi_district_price, \
                                        case when hotel_level_prefer is null then '暂无' else hotel_level_prefer end hotel_level_prefer \
                                from dwb_db.b_dwb_customer_imei_tag where period = '"+tag_period+"'")
    solo_1_imei_tag = pd.DataFrame([list(i) for i in res],columns=columnNames)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+" .读取表完毕: dwb_db.b_dwb_customer_imei_tag. 数据概述:")
    print(solo_1_imei_tag)
    res,columnNames = con.query("select id,ori_id ,ori_table ,imei ,city_id ,county_id ,newest_name ,visit_month ,visit_date ,pv ,source ,idate ,newest_id ,create_date ,create_user ,update_date ,update_user ,current_week \
                        from dwb_db.a_dwb_customer_browse_log where visit_month<= '"+end_date_month+"' and visit_month >= '"+start_date_month+"'")
    browse_log =  pd.DataFrame([list(i) for i in res],columns=columnNames)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+" .读取表完毕: dwb_db.a_dwb_customer_browse_log. 数据概述:")
    print(browse_log)
    look_list = browse_log[['imei','newest_id']].drop_duplicates()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+" .去重表完毕: dwb_db.a_dwb_customer_browse_log. 数据概述(look_list):")
    print(look_list)
    # res,columnNames = con.query("select  ods_id ,ods_table_name ,imei ,newest_id ,visit_month ,dr ,create_time ,update_time\
    #                                from dwb_db.dwb_customer_lookest_list_m_bak where visit_month = '"+start_date_month+"' \
    #                             union \
    #                             select  ods_id ,ods_table_name ,imei ,newest_id ,visit_month ,dr ,create_time ,update_time\
    #                                from dwb_db.dwb_customer_lookest_list_m_bak where visit_month = '"+str(int(start_date_month)+1)+"' \
    #                             union \
    #                             select  ods_id ,ods_table_name ,imei ,newest_id ,visit_month ,dr ,create_time ,update_time\
    #                                from dwb_db.dwb_customer_lookest_list_m_bak where visit_month = '"+str(int(start_date_month)+2)+"' "
    #                             )
    look_list_m = browse_log.groupby(['imei' ,'newest_id' ,'visit_month'])['id'].max().reset_index()
    look_list_m.rename(columns={'id':'ods_id'})
    look_list_m['ods_table_name'] = 'a_dwb_customer_browse_log'
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+" .去重表完毕: dwb_db.a_dwb_customer_browse_log. 数据概述(look_list_m):")
    print(look_list_m)
    con.close()  ##关链接
    solo_2_imei_tag = solo_1_imei_tag[['imei','have_car','have_house','is_college_stu','income_level','app_prefer','resi_district_price','hotel_level_prefer']]    ## 传递数据
    solo_2_imei_tag['resi_district_price'] = solo_2_imei_tag['resi_district_price'].apply(lambda x: re.findall(r'\d+',x )[0])     ## 获取居住价格的第一个数
    solo_2_imei_tag.at[solo_2_imei_tag['resi_district_price'].astype(int) > 100 , 'resi_district_price'] = solo_2_imei_tag['resi_district_price'].astype(int) / 10000 ##将居住小区的价格统一为万单位
    solo_2_imei_tag[['resi_district_price']] = solo_2_imei_tag[['resi_district_price']].astype('int')  ## 转换数据类型
    """
        筛选掉价值低的用户,筛选条件: 筛选掉没车的,没房的,没毕业的,以及一部分常用的app类型
    """
    # solo_imei_tag = solo_2_imei_tag[
    #             (solo_2_imei_tag['have_car'] == 'Y')&(solo_2_imei_tag['have_house'] == '有房')&(solo_2_imei_tag['is_college_stu'] == '否')
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("二次元社区"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("休闲娃娃机"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("卡牌桌游"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("名片管理"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("娱乐时尚"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("婚庆请柬"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("少儿编程"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("心理健康"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("情侣互动"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("智能配件"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("条码二维码"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("汇率换算"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("法律服务"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("流量管理"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("策略解谜"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("美容美妆"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("轻阅读"))
    #             &(~solo_2_imei_tag['app_prefer'].str.contains("音乐识别"))
    #      ] ### 筛选掉没车的,没房的,没毕业的,以及屌丝经常用的app类型
    solo_imei_tag = solo_2_imei_tag[
                (solo_2_imei_tag['have_car'] != 'N')&(solo_2_imei_tag['have_house'] != '无房')&(solo_2_imei_tag['is_college_stu'] != '是')
                &(~solo_2_imei_tag['app_prefer'].str.contains("二次元社区"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("休闲娃娃机"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("卡牌桌游"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("名片管理"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("娱乐时尚"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("婚庆请柬"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("少儿编程"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("心理健康"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("情侣互动"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("智能配件"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("流量管理"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("策略解谜"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("美容美妆"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("轻阅读"))
                &(~solo_2_imei_tag['app_prefer'].str.contains("音乐识别"))
         ] ### 筛选掉没车的,没房的,没毕业的,以及屌丝经常用的app类型
    solo_imei_tag_2 = pd.merge(look_list,solo_imei_tag,how='left',on=['imei'])  ## 关联出楼盘id
    imei_tag_1 = solo_imei_tag_2.loc[(solo_imei_tag_2['hotel_level_prefer'].str.contains("暂无")) | (solo_imei_tag_2['hotel_level_prefer'].str.contains("星级酒店"))]#包含星级酒店或者为空的
    imei_tag_2 = solo_imei_tag_2[(solo_imei_tag_2['resi_district_price']<100) & (solo_imei_tag_2['resi_district_price']!=0)]  #筛选居住小区价格为1W以上的和不为0的
    imei_tag_2['resi_district_price'] = imei_tag_2['resi_district_price'] * 100 #居住小区价格×100平方，估算出房间总价
    imei_tag_2 = pd.merge(imei_tag_2,layout_price,how='inner',on=['newest_id'])   #项目最低总价*50%
    imei_tag_2 = imei_tag_2[imei_tag_2['resi_district_price']>=imei_tag_2['layout_price']*0.5 ] #大于项目最低总价*50%

    newest_cus_tag_3 = imei_tag_2.append(imei_tag_1,ignore_index=False)    #将居住过星级酒店或者未知的人合并到居住小区价格大于项目价格的%50的小区
    imei_tag_4 = imei_tag_2[imei_tag_2['resi_district_price'] <= 0]       #将居住小区价格未知的拼接到一起
    newest_cus_tag_3 = newest_cus_tag_3.append(imei_tag_4,ignore_index=False)
    result = pd.merge(newest_cus_tag_3[['imei']].drop_duplicates(),browse_log,how='inner',on=['imei'])
    # result.to_sql('dwb_cust_high_brow_log',engine,index=False,if_exists='append')
    
    
    
    
    ##加上looklist标识
    result_tmp = newest_cus_tag_3[['imei']].drop_duplicates()
    result_tmp['price_index'] = 1
    result_lookest = pd.merge(look_list_m,result_tmp,how='left',on=['imei'])
    result_lookest.to_sql('dwb_customer_lookest_list_m',engine,index=False,if_exists='append')
    print('\n'+"当前季度跑数开始:     "+oprtion_start_date+'\n'+"开始执行的季度:       "+period +'\n'+"获取数据的开始月份:   "+start_date_month+'\n'+"获取数据的结束月份:   "+end_date_month+"本次程序执行结束时间: "+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+'\n'+"总用时:              "+str((parse(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))-parse(oprtion_start_date)).seconds)+"s"+'\n'+'\n'+'\n'+'\n' )
    

