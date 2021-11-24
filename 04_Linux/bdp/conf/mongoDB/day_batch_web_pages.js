db.setSlaveOk();
var c = db.web_pages_a2wqjt21634186115834.find( {"create_time": {$regex:"2021-11-24"} }).pretty()
while(c.hasNext()) {
    printjson(c.next());
}
