select distinct a.*,b.newest_id
from 
(
select 
a.newest_name,a.city_old,a.city_new,a.region_name_new,a.address,a.gd_lng_new,a.gd_lat_new,b.city_id
from dim_housing_bd_gd_compare a
left join dim_geography b on b.city_name=a.city_new and b.grade=3
-- left join dim_geography c on c.region_name=a.region_name_new and c.city_id=b.city_id and c.grade=4
-- left join dim_geography d on d.city_name=a.city_old and d.grade=3
-- where c.region_id is not null 
) a
left join dws_newest_info b on a.newest_name=b.newest_name and b.city_id=a.city_id and b.dr=0
where b.newest_id is not null ;


delete from dws_db_prd.dws_tag_purchase_poi where newest_id in (select distinct b.newest_id from (select a.newest_name,a.city_old,a.city_new,a.region_name_new,a.address,a.gd_lng_new,a.gd_lat_new,b.city_id from dim_housing_bd_gd_compare a left join dim_geography b on b.city_name=a.city_new and b.grade=3 ) a left join dws_newest_info b on a.newest_name=b.newest_name and b.city_id=a.city_id and b.dr=0 where b.newest_id is not null)


delete from dws_db_prd.dws_tag_purchase_poi where newest_id in (select newest_id from temp_db.tmp_newest_in_city_error where dr = 0 and newest_id not in ('7dae2029d10cfa1a6b24fd0060ecc79a','84be99387c460a2b850fc79919c62158','53234bfe2375747ca4e53da30ebf671d','d1b4c797211ffe5ca12890bfaf5104b5','331daaa40ed5d03efedd17a2c5f861e4','cd83c7cbc9c6e84d549929dc4e0d28d5','5a85bd3178d40eeb16a1a05970fd058f','b60a33ce77c8a91710df9b0ac1a4c2bc','c5469d16b150bdd87dcd3003834f0b46','dedcec9cd7bce87082806070b0cdea72','3c7fd0ec1628524d2a0949c5598a1afc','b0ade1b6f389c0c4e91d1b285bb7c804','55c58a9195dedcfb325c4121c0fae5a4','5f5581e6b2b0a3e3d095abe3835690fc','7a42a72d4b6c3091cf340890158472b1','8896b66014148af9915e632aa45b0a83','8dc361a06dcd7d1e92a2314fac06b0e9','665ccf58b75432e513f9febd8b571473','16d2dda8bdae4a99cbaa18e5b5d1d15e','18e88bddb3d5e7185284cc6e755a581c','fe434ab9eac2afb8205b39a1f453a817','0d69530ce541ada658f6766ab2a6f8e4','a1e7206ec14317977bd86a4e3ff6f068','f0749f5a41b2369989dca779bff1fd54','873fa36f4b2800def2f08927dc9f5523','a5328926a6370c26ead474c38700a5cd','4e99d8e8b5c6c9e238e57431fd9bde84','d72b5abc155c03f376cfb36a510dcb36','1e25b358785aea629b1d3103546caa27','0af473d22ebdac2c9abeae6b6f276930','cd518390fadf8a05afc2ae2ecc47420f','9f3631b112a83efec293fd2bcb94d126','9d009b2a363d17ebcb03eac972aefc57','25c1705574bc2268d8435a681b8bce59','cd27da2967e34d977ddbdf5de2a0cd47','af8bc6f61bc35578ce852090a69dfd7f','3dc69ed9cc129164c39a40fc75be0ce7','eebeeec5205a145035b9589ce106fa4b','abde2394b37a0e5e7fa47827bf61c1cc','a9be5388107833177d4fb49052654751','bc9691e02a48fe49c2d0cacfad6d7b61'));



