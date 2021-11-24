db.setSlaveOk();
db.web_pages_a2wqjt21634186115834.find( {"create_time": {$lt: ISODate("2021-11-24 00:00:00") } }, {"resource_list.name":1 , "mark_user":1 , "mark_page":1 ,"_id":0 }).forEach( function(cap){
	print(cap.mark_user, "\t" , cap.mark_page, "\t", cap.resource_list.name)
} )
