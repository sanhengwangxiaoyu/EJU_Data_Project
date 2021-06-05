select city_name ,sum(ifnull(room_sum ,0)) from temp_db.tmp_city_newest_deal tcnd group by city_name ;

select city_id ,count(1) from dwb_db.dwb_customer_browse_log dcbl group by city_id ;

select city_id ,city_name from dws_db.dim_geography dg where grade in ('3') ;


select b.*,a.* from (select city_id ,count(1) pn from dwb_db.dwb_customer_browse_log dcbl group by city_id) a left join (select city_id ,city_name from dws_db.dim_geography dg where grade in ('3')) b on a.city_id=b.city_id 


select city_name ,sum(ifnull(room_sum ,0)) from temp_db.tmp_city_newest_deal tcnd group by city_name ;

select
	b.city_name,
	a.pn
from
	(
	select
		city_id ,
		count(1) pn
	from
		dwb_db.dwb_customer_browse_log dcbl
	group by
		city_id) a
left join (
	select
		city_id ,
		city_name
	from
		dws_db.dim_geography dg
	where
		grade in ('3')) b on
	a.city_id = b.city_id;
	
	
select
	t1.city_name,
	ROUND(t1.s/t2.pn*100,1)
from
	(
	select
		city_name ,
		sum(ifnull(room_sum , 0)) s
	from
		temp_db.tmp_city_newest_deal tcnd
	group by
		city_name ) t1
left join (
	select
		b.city_name,
		a.pn
	from
		(
		select
			city_id ,
			count(1) pn
		from
			dwb_db.dwb_customer_browse_log dcbl
		group by
			city_id) a
	left join (
		select
			city_id ,
			city_name
		from
			dws_db.dim_geography dg
		where
			grade in ('3')) b on
		a.city_id = b.city_id ) t2 on
	concat(t1.city_name,'市') = t2.city_name;

	

select * from tmp_city_newest_deal tcnd where city_name in ('杭州')



select
	sum(t.room_sum),
	t.city_name
from
	(
	select
		distinct issue_code,
		room_sum,
		city_name
	from
		temp_db.tmp_city_newest_deal tcnd
	) t group  by t.city_name ;
	
	
	
SELECT
	t1.city_name,
	ROUND(t2.pn / t1.s*100,
	2)
FROM
	(
	SELECT
		sum(t.room_sum) s,
		t.city_name
	FROM
		(
		SELECT
			DISTINCT issue_code,
			room_sum,
			city_name
		FROM
			temp_db.tmp_city_newest_deal tcnd ) t
	GROUP BY
		t.city_name ) t1
LEFT JOIN (
	SELECT
		b.city_name,
		a.pn
	FROM
		(select city_id ,count(1) pn FROM dwb_db.dwb_customer_browse_log dcbl where substr(visit_date, 1, 7) in ('2021-01', '2021-02', '2021-03') 
		        GROUP BY city_id) a
	LEFT join
	    ( SELECT city_id , city_name FROM dws_db.dim_geography dg WHERE grade IN ('3')) b
	  ON a.city_id = b.city_id 
	 ) t2 
  ON concat(t1.city_name, '市') = t2.city_name;
	
 
 
SELECT t.city_name,sum(t.room_sum) s  FROM ( SELECT DISTINCT issue_code, room_sum, city_name FROM temp_db.tmp_city_newest_deal tcnd where city_name not in ('惠州','重庆','佛山','贵阳','武汉','唐山','温州','海口','无锡','福州','合肥','中山','长沙','南宁','青岛','淄博')) t GROUP BY t.city_name
 
union all 
 
select '惠州',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('惠州') and room_sale_state in ('可售') and room_sum<500 )t

union all 

select '重庆',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('重庆') and room_sum not in ('3566'))t 

union all 

select '贵阳',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('贵阳') and room_sale_state in ('可售') AND room_sum < 150)t

union all 

select '武汉',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('武汉') and room_sale_state in ('未销（预）售') AND room_sum <150)t

union all 

select '唐山',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('唐山') and room_sale_state in ('未售') and room_sum <100)t

union all 

select '温州',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('温州') and room_sale_state in ('在售') and room_sum <100)t

union all 

select '海口',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('海口') and room_sale_state in ('可售') and room_sum <300)t


union all 

select '无锡',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('无锡') and room_sale_state in ('待售'))t


union all 

select '福州',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('福州') and room_sale_state in ('可售') and room_sum <550)t

union all 

select '合肥',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('合肥') and room_sale_state in ('可售'))t

union all 

select '中山',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('中山') and room_sale_state in ('可售') and room_sum <150)t

union all 

select '长沙',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('长沙') and room_sale_state in ('可售') and room_sum <150)t


union all 

SELECT '南宁',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('南宁') and room_sale_state in ('未出售') and room_sum <200)t

union all 

select '青岛',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('青岛') and room_sale_state in ('可售') and room_sum <300)t

union all 

select '淄博',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('淄博') and room_sale_state in ('可售') and room_sum <100)t;

union all 

select '厦门',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('厦门') and room_sale_state in ('可售') and room_sum <600)t;


select '嘉兴',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('嘉兴') and room_sale_state in ('已备案') and room_sum <300)t;

SELECT
	t1.city_name,
	ROUND(t2.pn / t1.s*100, 2)
FROM
	(
	SELECT
		t.city_name,
		sum(t.room_sum) s
	FROM
		(
		SELECT
			DISTINCT issue_code,
			room_sum,
			city_name
		FROM
			temp_db.tmp_city_newest_deal tcnd
		WHERE
			city_name NOT IN ('南通','丽水','宝鸡', '厦门', '嘉兴', '惠州', '重庆', '佛山', '贵阳', '武汉', '唐山', '温州', '海口', '无锡', '福州', '合肥', '中山', '长沙', '南宁', '青岛', '淄博')) t
	GROUP BY
		t.city_name
UNION ALL
	SELECT
	
		'惠州' city_name,
		sum(t.room_sum) s
	FROM
		(
		SELECT
			DISTINCT issue_code d,
			room_sum
		FROM
			temp_db.tmp_city_newest_deal tcnd
		WHERE
			city_name IN ('惠州')
				AND room_sale_state IN ('可售')
					AND room_sum<150 )t
UNION ALL
	SELECT
		'重庆' city_name,
		sum(t.room_sum) s
	FROM
		(
		SELECT
			DISTINCT issue_code d,
			room_sum
		FROM
			temp_db.tmp_city_newest_deal tcnd
		WHERE
			city_name IN ('重庆')
				AND room_sum NOT IN ('3566'))t
UNION ALL
	SELECT
		'贵阳' city_name,
		sum(t.room_sum) s
	FROM
		(
		SELECT
			DISTINCT issue_code d,
			room_sum
		FROM
			temp_db.tmp_city_newest_deal tcnd
		WHERE
			city_name IN ('贵阳')
				AND room_sale_state IN ('可售')
					AND room_sum < 120)t
UNION ALL
	SELECT
		'武汉' city_name,
		sum(t.room_sum) s
	FROM
		(
		SELECT
			DISTINCT issue_code d,
			room_sum
		FROM
			temp_db.tmp_city_newest_deal tcnd
		WHERE
			city_name IN ('武汉')
				AND room_sale_state IN ('未销（预）售')
					AND room_sum < 180)t
UNION ALL
SELECT
	'唐山' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('唐山')
			AND room_sale_state IN ('未售')
				AND room_sum <100)t
UNION ALL
SELECT
	'温州' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('温州')
			AND room_sale_state IN ('在售')
				AND room_sum <130)t
UNION ALL
SELECT
	'海口' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('海口')
			AND room_sale_state IN ('可售')
				AND room_sum <300)t
UNION ALL
SELECT
	'无锡' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('无锡')
			AND room_sale_state IN ('待售')
				AND room_sum <2000)t
UNION ALL
SELECT
	'福州' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('福州')
			AND room_sale_state IN ('可售')
				AND room_sum <550)t
UNION ALL
SELECT
	'合肥' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('合肥')
			AND room_sale_state IN ('可售')
				AND room_sum <800 )t
UNION ALL
SELECT
	'中山' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('中山')
			AND room_sale_state IN ('可售')
				AND room_sum <100)t
UNION ALL
SELECT
	'长沙' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('长沙')
			AND room_sale_state IN ('可售')
				AND room_sum <150)t
UNION ALL
SELECT
	'南宁' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('南宁')
			AND room_sale_state IN ('未出售')
				AND room_sum <300)t
UNION ALL
SELECT
	'青岛' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('青岛')
			AND room_sale_state IN ('可售')
				AND room_sum <210)t
UNION ALL
SELECT
	'淄博'city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('淄博')
			AND room_sale_state IN ('可售')
				AND room_sum <100)t
UNION ALL
SELECT
	'佛山' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('佛山')
			AND room_sale_state IN ('可售')
				AND room_sum <150)t
UNION ALL
SELECT
	'厦门' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('厦门')
			AND room_sale_state IN ('可售')
				AND room_sum <600)t
UNION ALL
SELECT
	'嘉兴' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('嘉兴')
			AND room_sale_state IN ('已备案')
				AND room_sum <300)t
UNION ALL
SELECT
	'宝鸡' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('宝鸡')
			AND room_sum <240)t
UNION ALL
SELECT
	'丽水' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('丽水')
			AND room_sum <100)t			
			
UNION ALL
SELECT
	'南通' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('南通')
		  AND room_sale_state IN ('可售'))t	
			
			
) t1
LEFT JOIN (
	SELECT
		b.city_name,
		a.pn
	FROM
		(
		SELECT
			city_id ,
			count(1) pn
		FROM
			dwb_db.dwb_customer_browse_log dcbl
		WHERE
			substr(visit_date, 1, 7) IN ('2021-01', '2021-02', '2021-03')
		GROUP BY
			city_id) a
	LEFT JOIN (
		SELECT
			city_id ,
			city_name
		FROM
			dws_db.dim_geography dg
		WHERE
			grade IN ('3')) b ON
		a.city_id = b.city_id ) t2 ON
	concat(t1.city_name, '市') = t2.city_name;