
select * from  dim_geography  where isnull( city_name ) limit 10;


select * from  dws_customer_visit_reality_rate where isnull( city_rate_cre ) limit 10;


select * from  dws_imei_browse_tag where isnull( urgent ) limit 10;
select * from  dws_imei_browse_tag where isnull( cre ) limit 10;


select * from  dws_newest_alias where isnull( create_user ) limit 10;
select * from  dws_newest_alias where isnull( update_user ) limit 10;


select * from  dws_newest_city_qua where isnull( county_id ) limit 10;


select * from  dws_newest_county_qua where isnull( county_id ) limit 10;


select * from  dws_newest_developer_info where isnull( create_user ) limit 10;
select * from  dws_newest_developer_info where isnull( update_user ) limit 10;



select * from  dws_newest_info where isnull( county_id ) limit 10;
select * from  dws_newest_info where isnull( property_id ) limit 10;
select * from  dws_newest_info where isnull( building_num ) limit 10;
select * from  dws_newest_info where isnull( park_num ) limit 10;
select * from  dws_newest_info where isnull( park_rate ) limit 10;
select * from  dws_newest_info where isnull( opening_date ) limit 10;
select * from  dws_newest_info where isnull( create_user ) limit 10;
select * from  dws_newest_info where isnull( update_user ) limit 10;


select * from  dws_newest_layout where isnull( create_user ) limit 10;
select * from  dws_newest_layout where isnull( update_user ) limit 10;


select * from  dws_newest_offer_rate where isnull( offer ) limit 10;
select * from  dws_newest_offer_rate where isnull( rate ) limit 10;


select * from 	dws_newest_planinfo	where isnull(	park_rate	) limit 10;
select * from 	dws_newest_planinfo	where isnull(	park_num	) limit 10;
select * from 	dws_newest_planinfo	where isnull(	building_num	) limit 10;


select * from 	dws_newest_planinfo_20210511	where isnull(	newest_id	) limit 10;
select * from 	dws_newest_planinfo_20210511	where isnull(	land_area	) limit 10;
select * from 	dws_newest_planinfo_20210511	where isnull(	building_area	) limit 10;
select * from 	dws_newest_planinfo_20210511	where isnull(	volume_rate	) limit 10;
select * from 	dws_newest_planinfo_20210511	where isnull(	household_num	) limit 10;
select * from 	dws_newest_planinfo_20210511	where isnull(	park_rate	) limit 10;
select * from 	dws_newest_planinfo_20210511	where isnull(	park_num	) limit 10;
select * from 	dws_newest_planinfo_20210511	where isnull(	right_term	) limit 10;
select * from 	dws_newest_planinfo_20210511	where isnull(	green_rate	) limit 10;
select * from 	dws_newest_planinfo_20210511	where isnull(	decoration	) limit 10;
select * from 	dws_newest_planinfo_20210511	where isnull(	building_type	) limit 10;
select * from 	dws_newest_planinfo_20210511	where isnull(	floor_num	) limit 10;
select * from 	dws_newest_planinfo_20210511	where isnull(	building_num	) limit 10;



select * from 	dws_newest_provide_sche	where isnull(	date	) limit 10;
select * from 	dws_newest_provide_sche	where isnull(	period	) limit 10;


select * from 	dws_newest_saleinfo	where isnull(	recent_opening_time	) or	recent_opening_time	in ('') limit 10;
select * from 	dws_newest_saleinfo	where isnull(	recent_delivery_time	) or	recent_delivery_time	in ('') limit 10;
select * from 	dws_newest_saleinfo	where isnull(	issue_number	) or	issue_number	in ('') limit 10;


select * from 	dws_newest_supply	where isnull(	city_id	) or	city_id	in ('') limit 10;


select * from 	dws_newestvisitorfromcity_quarter	where isnull(	resi_city	) or	resi_city	in ('') limit 10;


select * from 	dws_newestvisitorfromcounty_quarter	where isnull(	resi_county	) or	resi_county	in ('') limit 10;

	
select * from 	dws_newestvisitorfromprovince_quarter	where isnull(	resi_province	) or	resi_province	in ('') limit 10;	


select * from 	dws_tag_basic	where isnull(	newest_id	) or	newest_id	in ('') limit 10;


select * from 	dws_tag_lifestyle_traffic	where isnull(	workday_value_num	) or	workday_value_num	in ('') limit 10;	
select * from 	dws_tag_lifestyle_traffic	where isnull(	workday_total_num	) or	workday_total_num	in ('') limit 10;	
select * from 	dws_tag_lifestyle_traffic	where isnull(	holiday_value_num	) or	holiday_value_num	in ('') limit 10;	
select * from 	dws_tag_lifestyle_traffic	where isnull(	holiday_total_num	) or	holiday_total_num	in ('') limit 10;	
select * from 	dws_tag_lifestyle_traffic	where isnull(	workday_ratio	) or	workday_ratio	in ('') limit 10;	
select * from 	dws_tag_lifestyle_traffic	where isnull(	holiday_ratio	) or	holiday_ratio	in ('') limit 10;	


select * from 	dws_tag_purchase	where isnull(	newest_id	) or	newest_id	in ('') limit 10;	



select * from 	dws_tag_purchase_mobile	where isnull(	newest_id	) or	newest_id	in ('') limit 10;



select * from 	dws_tag_purchase_poi	where isnull(	tag_value	) or	tag_value	in ('') limit 10;	


select * from 	dws_tag_purchase_prefer2	where isnull(	newest_id	) or	newest_id	in ('') limit 10;	


select * from 	dws_tag_resident_area	where isnull(	percentage	) or	percentage	in ('') limit 10;	









