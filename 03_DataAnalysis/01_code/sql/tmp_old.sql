show processlist 
KILL 76

update `dws_customer_month` set `month`=replace(`month`,'-','');

truncate TABLE dws_newest_customer_qua_tmp
 
 SELECT count(*) FROM dws_newest_property_score
 
 delete from dws_tag_purchase WHERE tag_name='��������'
 
 SELECT * FROM dws_newest_layout_price WHERE TYPE='room'
 
 SELECT *FROM dws_newest_customer_qua
 
 ��/��  ���Ͻ���1���ȵ�
 dws_imei_browse_tag -- ͳ������������  ��Ҫ���������
 dwb_customer_imei_tag -- �жϹ���Ŀ��
 
 select imei,'Ͷ����' type  from dwb_db.dwb_customer_imei_tag where is_college_stu = '��' -- and period = '2020Q4'
and marriage = '�ѻ�' and education = '��' and have_child = '��'
-- �������investment  ������own

SELECT offline_shop_prefer FROM dwb_db.dwb_customer_imei_tag 

dws_imei_browse_tag

SELECT * FROM dwb_db.dwb_customer_imei_tag

SELECT * FROM dws_newest_city_qua 


SELECT *FROM dwb_db.dwb_customer_browse_log WHERE imei='35156509082626'

SELECT city_name,estate_name newest_name,substr(visit_time,1,10) date,customer imei,'2020Q4' period FROM odsdb.cust_browse_log
where idate between '20201001' and '20201231' AND customer ='35156509082626'



SELECT DISTINCT newest_id FROM dws_db.dws_compete_list_qua


CREATE TABLE `dws_newest_customer_qua_tmp` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '����id',
  `period` varchar(255)  COMMENT '����',
  `newest_id` varchar(50) DEFAULT NULL COMMENT '¥��id',
  `intention` int(6)  DEFAULT '0' COMMENT '����ѡ������',
  `urgent` int(6)  DEFAULT '0' COMMENT '����������',
  `investment` int(6) DEFAULT NULL,
  `owner` int(6) DEFAULT NULL,
  `investment_rate` double(10,8) DEFAULT NULL,
  `owner_rate` double(10,8) DEFAULT NULL,
  `city_id` varchar(6) DEFAULT NULL COMMENT '����id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '����id',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='����ѡ���������򷿿ͻ�ͳ��'


SELECT 
imei,-- in_province_travel,out_province_travel,
CASE WHEN in_province_travel IS NOT NULL THEN 1 END in_province,
CASE WHEN out_province_travel IS NOT NULL THEN 1 END out_province,
CASE WHEN (in_province_travel IS NOT NULL) OR (out_province_travel IS NOT NULL)  THEN 1 END 'all'
-- concat(in_province_travel,out_province_travel)
FROM 
dwb_db.dwb_customer_imei_tag 
WHERE in_province_travel IS NOT NULL OR  out_province_travel IS NOT NULL 


SELECT DISTINCT imei,city_id,county_id,newest_id,concat(YEAR(visit_date),'Q',quarter(visit_date)) quarter 
FROM dwb_db.dwb_customer_browse_log 



SELECT * FROM dws_tag_purchase WHERE tag_name='��������'