select substr(issue_code,1,6) from odsdb.city_newest_deal where city_name = '�Ϻ�' group by substr(issue_code,1,6) ;
union all
select substr(issue_code,1,4) from dws_db_prd.dws_newest_issue_code where city_name = '�Ϻ���' group by substr(issue_code,1,4) ;


select * from odsdb.city_newest_deal where city_name = '�Ϻ�' and substr(issue_code,1,4) = '01(2';