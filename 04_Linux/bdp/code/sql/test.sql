use dwb_db;
show tables;
select "================================================================";
select "================================================================";
select "================================================================";
## 青岛
select t.newest_id,t.newest_name,t.city_id city1,t.county_id county1,t.region_name region1,t.issue_code,tt.county_id,tt.county_name,tt.issue_s from 
  (select a.newest_id,a.newest_name,a.city_id,a.county_id,a.region_name,b.county_name,a.address,max(a.issue_code) issue_code,0,now() create_date ,now() update_date from 
    (select t2.newest_id,t1.newest_name,t1.city_id,t1.county_id,t2.issue_code,t1.address,t3.region_name from 
      (select newest_id,newest_name,city_id,county_id,address from dws_db_prd.dws_newest_info where city_id = '370200' and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id) group by newest_id,city_id,county_id,address,newest_name ) t1
    inner join
      (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code group by newest_id ,issue_code) t2
    on t1.newest_id = t2.newest_id
    inner join 
      (select city_id,region_id,city_name,region_name from dws_db_prd.dim_geography where grade='4' group by city_id,region_id,city_name,region_name) t3
    on t1.city_id = t3.city_id and t1.county_id=t3.region_id) a
  left join 
    (select t2.newest_id,max(issue_code) code,t3.city_id,t3.city_name,t3.county_id,t3.county_name from 
      (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where dr = 0 and city_id = '370200'  group by newest_id ,city_id ,county_id ) t1
    left join 
      (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code where dr = 0 group by newest_id ,issue_code) t2
    on t1.newest_id = t2.newest_id
    left join 
      (select * from dwb_db.dim_issue where city_id = '370200' and dr = 0) t3
    on t1.city_id = t3.city_id and t1.county_id = t3.county_id
    where INSTR(t2.issue_code, t3.issue_s) group by t2.newest_id,t3.city_name,t3.city_id,t3.county_id,t3.county_name) b  
  on a.newest_id = b.newest_id where b.newest_id is null  and a.issue_code not like '%青%'
  group by a.newest_id,a.newest_name,a.city_id,a.county_id,a.address,b.county_name,a.region_name) t
left join 
    (select city_id,city_name,county_id,county_name,issue_s from dwb_db.dim_issue where dr = 0 and city_id='370200' and county_id is not null) tt
on t.city_id = tt.city_id where INSTR(t.issue_code, tt.issue_s);
