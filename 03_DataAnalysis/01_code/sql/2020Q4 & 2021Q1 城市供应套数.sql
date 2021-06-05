--2020Q4 & 2021Q1 城市供应套数
--  2020Q4 : 2020年第四季度
--  2021Q1 : 2021年第一季度


	SELECT
	    '2021Q1',
		t.city_name,
		sum(t.room_sum) s
	FROM
		(
		SELECT
			DISTINCT issue_code,
			room_sum,
			city_name
		FROM
			temp_db.city_newest_deal_huizhou tcnd 
		WHERE substr(issue_date, 1, 7) IN ('2021-01', '2021-02', '2021-03') ) t
	GROUP BY 
		t.city_name
UNION all
	SELECT
	    '2020Q4',
		t.city_name,
		sum(t.room_sum) s
	FROM
		(
		SELECT
			DISTINCT issue_code,
			room_sum,
			city_name
		FROM
			temp_db.city_newest_deal_huizhou tcnd 
		WHERE substr(issue_date, 1, 7) IN ('2020-10', '2020-11', '2020-12')) t
	GROUP BY
		t.city_name;