-- 意向客户统计 dws_newest_city_qua 
-- 以2021第一季度为例

-- 基础数据
insert into dws_db.dws_newest_city_qua(city_id,county_id,period,unit_price) select city_id,county_id,'2021Q1',AVG(unit_price) from dwb_db.dwb_newest_info g GROUP BY city_id,county_id;

--  待售 在售 售罄  total
update dws_db.dws_newest_city_qua t set t.for_sale=IFNULL((select g.sales from(select city_id,county_id,sales_state,count(sales_state) as sales from dwb_db.dwb_newest_info  GROUP BY city_id,county_id,sales_state) g where g.city_id=t.city_id and g.county_id=t.county_id and  g.sales_state='待售' ),0) where t.period='2021Q1';
update dws_db.dws_newest_city_qua t set t.on_sale=IFNULL((select g.sales from(select city_id,county_id,sales_state,count(sales_state) as sales from dwb_db.dwb_newest_info  GROUP BY city_id,county_id,sales_state) g where  g.city_id=t.city_id and g.county_id=t.county_id and  g.sales_state='在售' ),0) where t.period='2021Q1';
update dws_db.dws_newest_city_qua t set t.sell_out=IFNULL((select g.sales from(select city_id,county_id,sales_state,count(sales_state) as sales from dwb_db.dwb_newest_info  GROUP BY city_id,county_id,sales_state) g where  g.city_id=t.city_id and g.county_id=t.county_id and  g.sales_state='售罄' ),0) where t.period='2021Q1';
update dws_db.dws_newest_city_qua t set t.total_count=t.for_sale+t.on_sale+t.sell_out  where t.period='2021Q1';;

-- 关注 意向 紧急 
update dws_db.dws_newest_city_qua t set t.follow=(select count(DISTINCT imei) from dwb_db.dwb_customer_browse_log  where DATE_FORMAT(create_date,'%Y-%m-%d') = '2021-05-27' and city_id=t.city_id and county_id=t.county_id) where t.period='2021Q1';;

UPDATE dws_db.dws_newest_city_qua t
SET t.intention = IFNULL((
	SELECT
		count(*)
	FROM
		(
			SELECT DISTINCT
				city_id,county_id,g.imei
			FROM
				 dwb_db.dwb_customer_browse_log g	
			WHERE DATE_FORMAT(create_date,'%Y-%m-%d') = '2021-05-27'	
			GROUP BY
				g.city_id,county_id,g.imei
			HAVING
				count(g.newest_id) > 3
		) a
		where a.city_id=t.city_id and a.county_id=t.county_id
),0) where t.period='2021Q1';


UPDATE dws_db.dws_newest_city_qua t
SET t.urgent = IFNULL((
	SELECT
		count(*)
	FROM
		(
			SELECT DISTINCT
				city_id,county_id,g.imei
			FROM
				 dwb_db.dwb_customer_browse_log g	
			WHERE DATE_FORMAT(create_date,'%Y-%m-%d') = '2021-05-27'	
			GROUP BY
				g.city_id,county_id,g.imei
			HAVING
				count(g.newest_id) > 10
		) a
		where a.city_id=t.city_id and a.county_id=t.county_id
),0) where t.period='2021Q1';

-- 当季新增 当季留存
-- 将当季和上季的数据分别插入两张临时表
insert into temp_db.dwb_customer_browse_log select * from dwb_db.dwb_customer_browse_log g		WHERE DATE_FORMAT(create_date,'%Y-%m-%d') = '2021-04-16';
insert into temp_db.dwb_customer_browse_log2 select * from dwb_db.dwb_customer_browse_log g		WHERE DATE_FORMAT(create_date,'%Y-%m-%d') = '2021-05-27';	
	
UPDATE dws_db.dws_newest_city_qua t set t.retained = IFNULL((
SELECT
		count(*)
	FROM
		(
			SELECT DISTINCT
				a.imei,
				a.city_id,
				a.county_id
			FROM
				temp_db.dwb_customer_browse_log a
			JOIN temp_db.dwb_customer_browse_log2 b ON a.city_id = b.city_id
			AND a.county_id = b.county_id
			AND a.imei = b.imei
		) c
    where c.city_id=t.city_id and c.county_id=t.county_id
),0) where t.period='2021Q1';

UPDATE dws_db.dws_newest_city_qua t set t.increase = t.follow-t.retained where t.period='2021Q1';

