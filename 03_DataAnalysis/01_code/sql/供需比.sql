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
	concat(t1.city_name,'��') = t2.city_name;

	

select * from tmp_city_newest_deal tcnd where city_name in ('����')



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
  ON concat(t1.city_name, '��') = t2.city_name;
	
 
 
SELECT t.city_name,sum(t.room_sum) s  FROM ( SELECT DISTINCT issue_code, room_sum, city_name FROM temp_db.tmp_city_newest_deal tcnd where city_name not in ('����','����','��ɽ','����','�人','��ɽ','����','����','����','����','�Ϸ�','��ɽ','��ɳ','����','�ൺ','�Ͳ�')) t GROUP BY t.city_name
 
union all 
 
select '����',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('����') and room_sale_state in ('����') and room_sum<500 )t

union all 

select '����',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('����') and room_sum not in ('3566'))t 

union all 

select '����',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('����') and room_sale_state in ('����') AND room_sum < 150)t

union all 

select '�人',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('�人') and room_sale_state in ('δ����Ԥ����') AND room_sum <150)t

union all 

select '��ɽ',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('��ɽ') and room_sale_state in ('δ��') and room_sum <100)t

union all 

select '����',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('����') and room_sale_state in ('����') and room_sum <100)t

union all 

select '����',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('����') and room_sale_state in ('����') and room_sum <300)t


union all 

select '����',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('����') and room_sale_state in ('����'))t


union all 

select '����',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('����') and room_sale_state in ('����') and room_sum <550)t

union all 

select '�Ϸ�',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('�Ϸ�') and room_sale_state in ('����'))t

union all 

select '��ɽ',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('��ɽ') and room_sale_state in ('����') and room_sum <150)t

union all 

select '��ɳ',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('��ɳ') and room_sale_state in ('����') and room_sum <150)t


union all 

SELECT '����',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('����') and room_sale_state in ('δ����') and room_sum <200)t

union all 

select '�ൺ',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('�ൺ') and room_sale_state in ('����') and room_sum <300)t

union all 

select '�Ͳ�',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('�Ͳ�') and room_sale_state in ('����') and room_sum <100)t;

union all 

select '����',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('����') and room_sale_state in ('����') and room_sum <600)t;


select '����',sum(t.room_sum) from (
select distinct issue_code d,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('����') and room_sale_state in ('�ѱ���') and room_sum <300)t;

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
			city_name NOT IN ('��ͨ','��ˮ','����', '����', '����', '����', '����', '��ɽ', '����', '�人', '��ɽ', '����', '����', '����', '����', '�Ϸ�', '��ɽ', '��ɳ', '����', '�ൺ', '�Ͳ�')) t
	GROUP BY
		t.city_name
UNION ALL
	SELECT
	
		'����' city_name,
		sum(t.room_sum) s
	FROM
		(
		SELECT
			DISTINCT issue_code d,
			room_sum
		FROM
			temp_db.tmp_city_newest_deal tcnd
		WHERE
			city_name IN ('����')
				AND room_sale_state IN ('����')
					AND room_sum<150 )t
UNION ALL
	SELECT
		'����' city_name,
		sum(t.room_sum) s
	FROM
		(
		SELECT
			DISTINCT issue_code d,
			room_sum
		FROM
			temp_db.tmp_city_newest_deal tcnd
		WHERE
			city_name IN ('����')
				AND room_sum NOT IN ('3566'))t
UNION ALL
	SELECT
		'����' city_name,
		sum(t.room_sum) s
	FROM
		(
		SELECT
			DISTINCT issue_code d,
			room_sum
		FROM
			temp_db.tmp_city_newest_deal tcnd
		WHERE
			city_name IN ('����')
				AND room_sale_state IN ('����')
					AND room_sum < 120)t
UNION ALL
	SELECT
		'�人' city_name,
		sum(t.room_sum) s
	FROM
		(
		SELECT
			DISTINCT issue_code d,
			room_sum
		FROM
			temp_db.tmp_city_newest_deal tcnd
		WHERE
			city_name IN ('�人')
				AND room_sale_state IN ('δ����Ԥ����')
					AND room_sum < 180)t
UNION ALL
SELECT
	'��ɽ' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('��ɽ')
			AND room_sale_state IN ('δ��')
				AND room_sum <100)t
UNION ALL
SELECT
	'����' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('����')
			AND room_sale_state IN ('����')
				AND room_sum <130)t
UNION ALL
SELECT
	'����' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('����')
			AND room_sale_state IN ('����')
				AND room_sum <300)t
UNION ALL
SELECT
	'����' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('����')
			AND room_sale_state IN ('����')
				AND room_sum <2000)t
UNION ALL
SELECT
	'����' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('����')
			AND room_sale_state IN ('����')
				AND room_sum <550)t
UNION ALL
SELECT
	'�Ϸ�' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('�Ϸ�')
			AND room_sale_state IN ('����')
				AND room_sum <800 )t
UNION ALL
SELECT
	'��ɽ' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('��ɽ')
			AND room_sale_state IN ('����')
				AND room_sum <100)t
UNION ALL
SELECT
	'��ɳ' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('��ɳ')
			AND room_sale_state IN ('����')
				AND room_sum <150)t
UNION ALL
SELECT
	'����' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('����')
			AND room_sale_state IN ('δ����')
				AND room_sum <300)t
UNION ALL
SELECT
	'�ൺ' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('�ൺ')
			AND room_sale_state IN ('����')
				AND room_sum <210)t
UNION ALL
SELECT
	'�Ͳ�'city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('�Ͳ�')
			AND room_sale_state IN ('����')
				AND room_sum <100)t
UNION ALL
SELECT
	'��ɽ' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('��ɽ')
			AND room_sale_state IN ('����')
				AND room_sum <150)t
UNION ALL
SELECT
	'����' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('����')
			AND room_sale_state IN ('����')
				AND room_sum <600)t
UNION ALL
SELECT
	'����' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('����')
			AND room_sale_state IN ('�ѱ���')
				AND room_sum <300)t
UNION ALL
SELECT
	'����' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('����')
			AND room_sum <240)t
UNION ALL
SELECT
	'��ˮ' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('��ˮ')
			AND room_sum <100)t			
			
UNION ALL
SELECT
	'��ͨ' city_name,
	sum(t.room_sum) s
FROM
	(
	SELECT
		DISTINCT issue_code d,
		room_sum
	FROM
		temp_db.tmp_city_newest_deal tcnd
	WHERE
		city_name IN ('��ͨ')
		  AND room_sale_state IN ('����'))t	
			
			
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
	concat(t1.city_name, '��') = t2.city_name;