目录:
    ods_city_issue_code.py    各个城市预售证爬取
    ods_gd_points_web.py    GoningData生产数据入库

作用:
    从外部数据源获取数据,通过简单处理加载到数据库的第一层ods中.

注意事项:
    1、 ods_city_issue_code.py 网址会变,内容会变,需要每天注意
    2、 ods_gd_points_web.py 先通过DataX将生产数据从mongDB中导出成文件,再通过此代码入库

代码完成情况:
    202202: ods_city_issue_code.py 代码完成(三亚 ,唐山 ,南通部分区县 ,保定部分区县)
    