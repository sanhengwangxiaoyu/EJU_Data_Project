import configparser,os,pymysql,pandas as pd,re,time,numpy as np,re,MySQLdb
from pandas.core.arraylike import OpsMixin
from typing import Tuple
from sqlalchemy import create_engine
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


"""传参"""
period = '2021Q4'
newest_id = '3c2a249ea8fa11ec869c8c8caa44e774'



"""
正式代码
  使用纯sql来更新表
  先更新关注意向迫切增存人数
  再更新城市项目在售待售售罄数量
  再更新区县项目在售待售售罄数量
  再更新城市和区县的总项目数量
"""





#In[]

con = MysqlClient(db_host,database,user,password) #创建数据库连接
## 更新人数
con.updata_one("UPDATE dws_db_prd.dws_newest_city_qua a, ( \
                        SELECT newest_id, intention, orien, urgent, increase \
                            , retained \
                        FROM dwb_db.dwb_newest_customer_info \
                        WHERE period = '"+period+"' \
                            AND newest_id = '"+newest_id+"' \
                    ) b \
                SET a.follow = a.follow + b.intention, a.intention = a.intention + b.orien, a.urgent = a.urgent + b.urgent, a.increase = a.increase + b.increase, a.retained = a.retained + b.retained \
                WHERE a.period = '"+period+"' \
                    AND a.city_id IN ( \
                        SELECT city_id \
                        FROM dws_db_prd.dws_newest_info \
                        WHERE dr = 0 \
                            AND newest_id = '"+newest_id+"' \
                    ) \
                    AND (a.county_id IS NULL \
                        OR a.county_id IN ( \
                            SELECT county_id \
                            FROM dws_db_prd.dws_newest_info \
                            WHERE dr = 0 \
                                AND newest_id = '"+newest_id+"' \
                        ))"
)
##更新城市项目数
con.updata_one("UPDATE dws_db_prd.dws_newest_city_qua a, ( \
                        SELECT count(t1.newest_id) AS num, sales_state, t1.city_id \
                        FROM ( \
                            SELECT newest_id, sales_state, city_id \
                            FROM dws_db_prd.dws_newest_info \
                            WHERE city_id IN ( \
                                    SELECT city_id \
                                    FROM dws_db_prd.dws_newest_info \
                                    WHERE dr = 0 \
                                        AND newest_id = '"+newest_id+"' \
                                ) \
                                AND dr = 0 \
                        ) t1 \
                            INNER JOIN ( \
                                SELECT newest_id \
                                FROM dws_db_prd.dws_newest_period_admit \
                                WHERE dr = 0 \
                                    AND period = '"+period+"' \
                                GROUP BY newest_id \
                            ) t2 \
                            ON t1.newest_id = t2.newest_id \
                        GROUP BY sales_state, t1.city_id \
                    ) b \
                SET a.on_sale = b.num \
                WHERE a.city_id = b.city_id \
                    AND a.period = '"+period+"' \
                    AND b.sales_state = '在售' \
                    AND a.county_id IS NULL"
)
con.updata_one("UPDATE dws_db_prd.dws_newest_city_qua a, ( \
                        SELECT count(t1.newest_id) AS num, sales_state, t1.city_id \
                        FROM ( \
                            SELECT newest_id, sales_state, city_id \
                            FROM dws_db_prd.dws_newest_info \
                            WHERE city_id IN ( \
                                    SELECT city_id \
                                    FROM dws_db_prd.dws_newest_info \
                                    WHERE dr = 0 \
                                        AND newest_id = '"+newest_id+"' \
                                ) \
                                AND dr = 0 \
                        ) t1 \
                            INNER JOIN ( \
                                SELECT newest_id \
                                FROM dws_db_prd.dws_newest_period_admit \
                                WHERE dr = 0 \
                                    AND period = '"+period+"' \
                                GROUP BY newest_id \
                            ) t2 \
                            ON t1.newest_id = t2.newest_id \
                        GROUP BY sales_state, t1.city_id \
                    ) b \
                SET a.for_sale = b.num \
                WHERE a.city_id = b.city_id \
                    AND a.period = '"+period+"' \
                    AND b.sales_state = '待售' \
                    AND a.county_id IS NULL"
)
con.updata_one("UPDATE dws_db_prd.dws_newest_city_qua a, ( \
                        SELECT count(t1.newest_id) AS num, sales_state, t1.city_id \
                        FROM ( \
                            SELECT newest_id, sales_state, city_id \
                            FROM dws_db_prd.dws_newest_info \
                            WHERE city_id IN ( \
                                    SELECT city_id \
                                    FROM dws_db_prd.dws_newest_info \
                                    WHERE dr = 0 \
                                        AND newest_id = '"+newest_id+"' \
                                ) \
                                AND dr = 0 \
                        ) t1 \
                            INNER JOIN ( \
                                SELECT newest_id \
                                FROM dws_db_prd.dws_newest_period_admit \
                                WHERE dr = 0 \
                                    AND period = '"+period+"' \
                                GROUP BY newest_id \
                            ) t2 \
                            ON t1.newest_id = t2.newest_id \
                        GROUP BY sales_state, t1.city_id \
                    ) b \
                SET a.sell_out = b.num \
                WHERE a.city_id = b.city_id \
                    AND a.period = '"+period+"' \
                    AND b.sales_state = '售罄' \
                    AND a.county_id IS NULL"
)
##更新区县项目数
con.updata_one("UPDATE dws_db_prd.dws_newest_city_qua a, ( \
                        SELECT count(t1.newest_id) AS num, sales_state, county_id \
                        FROM ( \
                            SELECT newest_id, sales_state, county_id \
                            FROM dws_db_prd.dws_newest_info \
                            WHERE city_id IN ( \
                                    SELECT city_id \
                                    FROM dws_db_prd.dws_newest_info \
                                    WHERE dr = 0 \
                                        AND newest_id = '"+newest_id+"' \
                                ) \
                                AND county_id IN ( \
                                    SELECT county_id \
                                    FROM dws_db_prd.dws_newest_info \
                                    WHERE dr = 0 \
                                        AND newest_id = '"+newest_id+"' \
                                ) \
                        ) t1 \
                            INNER JOIN ( \
                                SELECT newest_id \
                                FROM dws_db_prd.dws_newest_period_admit \
                                WHERE dr = 0 \
                                    AND period = '"+period+"' \
                                GROUP BY newest_id \
                            ) t2 \
                            ON t1.newest_id = t2.newest_id \
                        GROUP BY sales_state, county_id \
                    ) b \
                SET a.on_sale = b.num \
                WHERE a.county_id = b.county_id \
                    AND a.period = '"+period+"' \
                    AND b.sales_state = '在售'" 
)
con.updata_one("UPDATE dws_db_prd.dws_newest_city_qua a, ( \
                        SELECT count(t1.newest_id) AS num, sales_state, county_id \
                        FROM ( \
                            SELECT newest_id, sales_state, county_id \
                            FROM dws_db_prd.dws_newest_info \
                            WHERE city_id IN ( \
                                    SELECT city_id \
                                    FROM dws_db_prd.dws_newest_info \
                                    WHERE dr = 0 \
                                        AND newest_id = '"+newest_id+"' \
                                ) \
                                AND county_id IN ( \
                                    SELECT county_id \
                                    FROM dws_db_prd.dws_newest_info \
                                    WHERE dr = 0 \
                                        AND newest_id = '"+newest_id+"' \
                                ) \
                        ) t1 \
                            INNER JOIN ( \
                                SELECT newest_id \
                                FROM dws_db_prd.dws_newest_period_admit \
                                WHERE dr = 0 \
                                    AND period = '"+period+"' \
                                GROUP BY newest_id \
                            ) t2 \
                            ON t1.newest_id = t2.newest_id \
                        GROUP BY sales_state, county_id \
                    ) b \
                SET a.for_sale = b.num \
                WHERE a.county_id = b.county_id \
                    AND a.period = '"+period+"' \
                    AND b.sales_state = '待售'" 
)
con.updata_one("UPDATE dws_db_prd.dws_newest_city_qua a, ( \
                        SELECT count(t1.newest_id) AS num, sales_state, county_id \
                        FROM ( \
                            SELECT newest_id, sales_state, county_id \
                            FROM dws_db_prd.dws_newest_info \
                            WHERE city_id IN ( \
                                    SELECT city_id \
                                    FROM dws_db_prd.dws_newest_info \
                                    WHERE dr = 0 \
                                        AND newest_id = '"+newest_id+"' \
                                ) \
                                AND county_id IN ( \
                                    SELECT county_id \
                                    FROM dws_db_prd.dws_newest_info \
                                    WHERE dr = 0 \
                                        AND newest_id = '"+newest_id+"' \
                                ) \
                        ) t1 \
                            INNER JOIN ( \
                                SELECT newest_id \
                                FROM dws_db_prd.dws_newest_period_admit \
                                WHERE dr = 0 \
                                    AND period = '"+period+"' \
                                GROUP BY newest_id \
                            ) t2 \
                            ON t1.newest_id = t2.newest_id \
                        GROUP BY sales_state, county_id \
                    ) b \
                SET a.sell_out = b.num \
                WHERE a.county_id = b.county_id \
                    AND a.period = '"+period+"' \
                    AND b.sales_state = '售罄'" 
)
## 修改区域总量
con.updata_one("UPDATE dws_db_prd.dws_newest_city_qua \
                        SET total_count = for_sale + on_sale + sell_out \
                        WHERE city_id IN ( \
                                SELECT city_id \
                                FROM dws_db_prd.dws_newest_info \
                                WHERE dr = 0 \
                                    AND newest_id = '"+newest_id+"' \
                            ) \
                            AND period = '"+period+"' \
                            AND county_id IS NOT NULL"
)
con.updata_one("UPDATE dws_db_prd.dws_newest_city_qua \
                        SET total_count = for_sale + on_sale + sell_out \
                        WHERE city_id IN ( \
                                SELECT city_id \
                                FROM dws_db_prd.dws_newest_info \
                                WHERE dr = 0 \
                                    AND newest_id = '"+newest_id+"' \
                            ) \
                            AND period = '"+period+"' \
                            AND county_id IS NULL"
)






con.close()


#In[]
# str1 = ("UPDATE dws_db_prd.dws_newest_city_qua a, ( SELECT newest_id, intention, orien, urgent, increase , retained FROM dwb_db.dwb_newest_customer_info WHERE period = '"+period+"' AND newest_id = '"+newest_id+"' ) b SET a.follow = a.follow + b.intention, a.intention = a.intention + b.orien, a.urgent = a.urgent + b.urgent, a.increase = a.increase + b.increase, a.retained = a.retained + b.retained WHERE a.period = '"+period+"' AND a.city_id IN ( SELECT city_id FROM dws_db_prd.dws_newest_info WHERE dr = 0 AND newest_id = '"+newest_id+"' ) AND (a.county_id IS NULL OR a.county_id IN ( SELECT county_id FROM dws_db_prd.dws_newest_info WHERE dr = 0 AND newest_id = '"+newest_id+"' ))"
# )
# str2 = ("UPDATE dws_db_prd.dws_newest_city_qua a, ( SELECT count(t1.newest_id) AS num, sales_state, t1.city_id FROM ( SELECT newest_id, sales_state, city_id FROM dws_db_prd.dws_newest_info WHERE city_id IN ( SELECT city_id FROM dws_db_prd.dws_newest_info WHERE dr = 0 AND newest_id = '"+newest_id+"' ) AND dr = 0 ) t1 INNER JOIN ( SELECT newest_id FROM dws_db_prd.dws_newest_period_admit WHERE dr = 0 AND period = '"+period+"' GROUP BY newest_id ) t2 ON t1.newest_id = t2.newest_id GROUP BY sales_state, t1.city_id ) b SET a.on_sale = b.num WHERE a.city_id = b.city_id AND a.period = '"+period+"' AND b.sales_state = '在售' AND a.county_id IS NULL"
# )
# str3 = ("UPDATE dws_db_prd.dws_newest_city_qua a, ( SELECT count(t1.newest_id) AS num, sales_state, t1.city_id FROM ( SELECT newest_id, sales_state, city_id FROM dws_db_prd.dws_newest_info WHERE city_id IN ( SELECT city_id FROM dws_db_prd.dws_newest_info WHERE dr = 0 AND newest_id = '"+newest_id+"' ) AND dr = 0 ) t1 INNER JOIN ( SELECT newest_id FROM dws_db_prd.dws_newest_period_admit WHERE dr = 0 AND period = '"+period+"' GROUP BY newest_id ) t2 ON t1.newest_id = t2.newest_id GROUP BY sales_state, t1.city_id ) b SET a.on_sale = b.num WHERE a.city_id = b.city_id AND a.period = '"+period+"' AND b.sales_state = '待售' AND a.county_id IS NULL"
# )
# str4 = ("UPDATE dws_db_prd.dws_newest_city_qua a, ( SELECT count(t1.newest_id) AS num, sales_state, t1.city_id FROM ( SELECT newest_id, sales_state, city_id FROM dws_db_prd.dws_newest_info WHERE city_id IN ( SELECT city_id FROM dws_db_prd.dws_newest_info WHERE dr = 0 AND newest_id = '"+newest_id+"' ) AND dr = 0 ) t1 INNER JOIN ( SELECT newest_id FROM dws_db_prd.dws_newest_period_admit WHERE dr = 0 AND period = '"+period+"' GROUP BY newest_id ) t2 ON t1.newest_id = t2.newest_id GROUP BY sales_state, t1.city_id ) b SET a.on_sale = b.num WHERE a.city_id = b.city_id AND a.period = '"+period+"' AND b.sales_state = '售罄' AND a.county_id IS NULL"
# )
# str5 = ("UPDATE dws_db_prd.dws_newest_city_qua a, ( SELECT count(t1.newest_id) AS num, sales_state, county_id FROM ( SELECT newest_id, sales_state, county_id FROM dws_db_prd.dws_newest_info WHERE city_id IN ( SELECT city_id FROM dws_db_prd.dws_newest_info WHERE dr = 0 AND newest_id = '"+newest_id+"' ) AND county_id IN ( SELECT county_id FROM dws_db_prd.dws_newest_info WHERE dr = 0 AND newest_id = '"+newest_id+"' ) ) t1 INNER JOIN ( SELECT newest_id FROM dws_db_prd.dws_newest_period_admit WHERE dr = 0 AND period = '"+period+"' GROUP BY newest_id ) t2 ON t1.newest_id = t2.newest_id GROUP BY sales_state, county_id ) b SET a.on_sale = b.num WHERE a.county_id = b.county_id AND a.period = '"+period+"' AND b.sales_state = '在售'" 
# )
# str6 = ("UPDATE dws_db_prd.dws_newest_city_qua a, ( SELECT count(t1.newest_id) AS num, sales_state, county_id FROM ( SELECT newest_id, sales_state, county_id FROM dws_db_prd.dws_newest_info WHERE city_id IN ( SELECT city_id FROM dws_db_prd.dws_newest_info WHERE dr = 0 AND newest_id = '"+newest_id+"' ) AND county_id IN ( SELECT county_id FROM dws_db_prd.dws_newest_info WHERE dr = 0 AND newest_id = '"+newest_id+"' ) ) t1 INNER JOIN ( SELECT newest_id FROM dws_db_prd.dws_newest_period_admit WHERE dr = 0 AND period = '"+period+"' GROUP BY newest_id ) t2 ON t1.newest_id = t2.newest_id GROUP BY sales_state, county_id ) b SET a.on_sale = b.num WHERE a.county_id = b.county_id AND a.period = '"+period+"' AND b.sales_state = '待售'" 
# )
# str7 = ("UPDATE dws_db_prd.dws_newest_city_qua a, ( SELECT count(t1.newest_id) AS num, sales_state, county_id FROM ( SELECT newest_id, sales_state, county_id FROM dws_db_prd.dws_newest_info WHERE city_id IN ( SELECT city_id FROM dws_db_prd.dws_newest_info WHERE dr = 0 AND newest_id = '"+newest_id+"' ) AND county_id IN ( SELECT county_id FROM dws_db_prd.dws_newest_info WHERE dr = 0 AND newest_id = '"+newest_id+"' ) ) t1 INNER JOIN ( SELECT newest_id FROM dws_db_prd.dws_newest_period_admit WHERE dr = 0 AND period = '"+period+"' GROUP BY newest_id ) t2 ON t1.newest_id = t2.newest_id GROUP BY sales_state, county_id ) b SET a.on_sale = b.num WHERE a.county_id = b.county_id AND a.period = '"+period+"' AND b.sales_state = '售罄'" 
# )
# str8 = ("UPDATE dws_db_prd.dws_newest_city_qua SET total_count = for_sale + on_sale + sell_out WHERE city_id IN ( SELECT city_id FROM dws_db_prd.dws_newest_info WHERE dr = 0 AND newest_id = '"+newest_id+"' ) AND period = '"+period+"' AND county_id IS NOT NULL"
# )
# str9 = ("UPDATE dws_db_prd.dws_newest_city_qua SET total_count = for_sale + on_sale + sell_out WHERE city_id IN ( SELECT city_id FROM dws_db_prd.dws_newest_info WHERE dr = 0 AND newest_id = '"+newest_id+"' ) AND period = '"+period+"' AND county_id IS NULL"
# )
# print(str1+';')

# print(str2+';')

# print(str3+';')

# print(str4+';')
# print(str5+';')

# print(str6+';')
# print(str7+';')

# print(str8+';')
# print(str9+';')
