目录 ：
    脚本：
         amap_search_poi.py
         amap_search_poi_V3.py
    key使用日志：
          keylog.txt
    要爬取的楼盘需求：
          search_gps.txt
          search_gps_v3.txt
    爬取的结果：
          latlng_rs.txt
          latlng_rs_v3.txt

作用：
          脚本 【amap_search_poi.py】 和 【amap_search_poi_V3.py】 都是从 【要爬取的楼盘需求】 中获
    取楼盘名称，楼盘id，楼盘经纬度，楼盘地址。然后将爬取的poi信息和楼盘需求的字段全部写入到【爬取
    的结果】 当中去。其中此次执行时，key的使用次数记录在【keylog.txt】当中。其中所有文件名字带有V3
    的是一波，不带有V3的是另外一波，两者的区别就是调用的poi接口的版本不一样。

注意事项：
           1、脚本会记录此次程序key的使用次数，所以建议当需要连续执行两天的时候，第二天重新运行一遍
           2、两个脚本的key的使用是重复使用的，而且都是存放在keylog中。所以你会看到重复的key出现在
     使用日志中。    
           3、 由于两个脚本使用的poi接口版本不一样，所以导致相同楼盘会出现不同的poi。所以当全部爬完
    之后，如果没有poi的可以交换之后再爬一遍
           4、 之所以使用两个poi的版本，就是最大程度的重复利用手里的key

待优化：
         1、 keylog分开
         2、 要爬取的楼盘需求从数据库表中读取
         3、 爬取的结果存放到数据库表里
         4、 连续运行第二天时，程序自动清空key的使用次数
    