select city_name from tmp_city_newest_deal group by city_name ;

select count(1) from tmp_city_newest_deal tcnd where tcnd.city_name in ('None');

select * from tmp_city_newest_deal tcnd where tcnd.city_name in ('None');


select * from tmp_city_newest_deal tcnd where city_name in ('');
select count(1) from tmp_city_newest_deal tcnd where city_name in ('');

select
	*
from
	tmp_city_newest_deal tcnd
where
	city_name in ('')
	and url not in ('None',
	'',
	null)
	and city_name not in ('None',
	'',
	null)
	and floor_name not in ('None',
	'',
	null)
	and address not in ('None',
	'',
	null)
	and business not in ('None',
	'',
	null)
	and issue_code not in ('None',
	'',
	null)
	and issue_date not in ('None',
	'',
	null)
	and open_date not in ('None',
	'',
	null)
	and issue_area not in ('None',
	'',
	null)
	and sale_state not in ('None',
	'',
	null)
	and building_code not in ('None',
	'',
	null)
	and room_sum not in ('None',
	'',
	null)
	and area not in ('None',
	'',
	null)
	and simulation_price not in ('None',
	'',
	null)
	and sale_telephone not in ('None',
	'',
	null)
	and sale_address not in ('None',
	'',
	null)
	and room_code not in ('None',
	'',
	null)
	and room_sale_area not in ('None',
	'',
	null)
	and room_sale_state;

select count(1) from tmp_city_newest_deal tcnd;


delete from tmp_city_newest_deal where city_name = 'None';
delete from tmp_city_newest_deal where city_name = '';

