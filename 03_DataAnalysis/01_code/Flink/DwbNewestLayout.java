package com.eju.bigdata.task;

import com.alibaba.fastjson.JSON;
import com.eju.bigdata.sqlclient.ShopSinkClient;
import com.eju.bigdata.util.DigitalUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.scala.row;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * ori_newest_info_archive_main 表数据清洗
 *
 * @author Xionghaijun
 * @since Created in 2021/4/16 下午3:35
 */
public class DwbNewestLayout {

    public static void main(String[] args) throws Exception {
       /* String tableName = "odsdb.ori_newest_info_main";
        Long totalCount = (Long) ShopSinkClient.executeSql("select count(*) from " + tableName).get(0).get(0);
//        Long totalCount = (Long) ShopSinkClient.executeSql("select count(*) from " + tableName + " where uuid in(\"02345a7bbac8d6f061d0c3234b2ea674\",\"225beb858e1712be0f750f89007154f9\",\"45e200deb651a8d04b5c9f0e8804248b\",\"4b1149fbae8cca05f9318143613d91dd\",\"4c2a2bd3a28ff2dcba4362cd9d32676c\",\"520837cb74c862eced86336181c38e66\",\"5d335cc4f55431c70fd52f58503a0571\",\"6c33fdbdc76a7a3739441a6b5cef66e1\",\"6f032e995629c4ee84b9bbff4c892ae9\",\"87221f80b8d8ecca4f263a23abd1ad20\",\"8d6ab9ff7074d6f8ba0400ceceb36412\",\"9b3f206656ba96d3182763e7a14be4b1\",\"a78d729566162d90b0df10a6967cc0a8\",\"b239f002d92d4ebaec994ba64fc772e7\",\"c1323f1575179339d866ff503fcdcf1c\",\"c56dde50084f8fcec3752e3c544c3be0\",\"e4fd592febeb94751bec6aa72f0c2e6e\",\"f17250f3a44639b0fbe7a4471b36d3e4\",\"ff698edd1288fab42dc5497fd9d595bc\",\"23f955cdf9ab41bad8b08cadfd38d843\")").get(0).get(0);
        if (totalCount == null) {
            return;
        }
//        Long totalCount = 400000L;

        long pageSize = 50000L;
        long page;
        if (totalCount % pageSize > 0) {
            page = totalCount / pageSize + 1;
        } else {
            page = totalCount / pageSize;
        }

        long l0 = System.currentTimeMillis();
        for (int i = 1; i <= page; i++) {
            System.out.println(FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(l0) + " ==== 第 "+ i + " 批数据开始插入 ====");
            long offset = (i - 1) * pageSize;

            long l1 = System.currentTimeMillis();
            updateDwbNewestLayout(tableName, offset, pageSize);
            long l2 = System.currentTimeMillis();
            System.out.println(FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(l2) + " 第 "+ i + " 批插入目标表消耗时间：" + (l2 -l1));
        }
        long l3 = System.currentTimeMillis();
        System.out.println(FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(l3) + " 总消耗时间：" + (l3 -l0));*/
      //  updateDwbNewestLayout("",0,0);

        String jdata = "{'series': [{'data': [3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}], 'xAxis': ['信阳市-商城县-商城李集加油中心-115.546504,31.889243', '上海市-闵行区-上海虹桥商务区建虹路加油站-121.317371,31.187091', '信阳市-浉河区-加油站-114.157786,31.990530', '南阳市-新野县-加油站(新都路)-112.429466,32.390624', '孝感市-云梦县-中国石化(黄河加油站)-113.729457,31.052147', '孝感市-应城市-中国石化(碾屋加油站)-113.567851,30.935280', '孝感市-应城市-加油站(陈家湾)-113.584877,30.925114', '武汉市-江夏区-服务区-光谷石油-114.414835,30.464903', '武汉市-江岸区-中国石油(机电园路店)-114.331621,30.663211', '武汉市-洪山区-中国石化加油站-114.309923,30.503900', '武汉市-洪山区-乔木湾加油站-114.289760,30.497932', '武汉市-洪山区-南湖二加油站-114.353726,30.510447', '武汉市-硚口区-广东大隆武汉古田一路CNG加气站-114.185298,30.607734', '武汉市-蔡甸区-江城大道油气合建加油站-114.191443,30.481867', '珠海市-香洲区-加华加油站-113.589841,22.243101', '省直辖县级行政单位-天门市-加油站-113.171232,30.634432', '荆州市-洪湖市-中国石化(洪湖汊河加油站)-113.477331,29.993434', '鄂州市-鄂城区-中国石化(湖北鄂州石油分公司黄龙服务区加油南站)-114.967714,30.333865', '随州市-曾都区-中国石油(云山竹语西)-113.374772,31.680143', '黄冈市-蕲春县-中国石化(吴庄加油站)-115.445833,30.219309']}";

        JgGroupTag jgGroupTag = JSON.parseObject(jdata, JgGroupTag.class);
        String[] strings = jgGroupTag.getxAxis();
        int[] datas = jgGroupTag.getSeries()[0].getData();

        System.out.println(StringUtils.join(strings, "^"));
       /* for(int i=0;i<strings.length;i++) {
            System.out.println(strings[i]);
        }*/
      /*  String xAxisString = Arrays.toString(strings);
        System.out.println(xAxisString);
        System.out.println(strings.length+"--"+datas.length);
        if(strings.length!=datas.length){
            String[] split = xAxisString.split(",");
            StringBuilder result = new StringBuilder();
            for(int i=0;i<split.length;i++){
                result.append(split[i]);
                if(i%2==0){
                    result.append("/");
                }else if(i!=split.length-1){
                    result.append(",");
                }
            }
            xAxisString= result.toString();
            System.out.println(xAxisString);*/

    }

    /**
     * 从odsdb.ori_newest_info_archive_main 清洗到 dwb_db.ori_newest_info_archive_main 表
     * @param offset 起始位置
     * @param pageSize 每页大小
     * @throws Exception 异常
     */
    private static void updateDwbNewestLayout(String tableName, long offset, long pageSize) throws Exception {
        String url = "jdbc:mysql://172.28.36.77:3306/odsdb?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
        String username = "mysql";
        String password = "egSQ7HhxajHZjvdX";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //查询 odsdb.ori_newest_info_archive_main 表
        RowTypeInfo dwbNewestLayoutRowType = new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO);
        //String jgSql = "select id,uuid,layout from " + tableName + " limit " + offset + ", " + pageSize;
        //String jgSql  = "select DISTINCT b.id,b.uuid,b.layout from temp_db.dwd_floor_name_2 t LEFT JOIN odsdb.ori_newest_info_base b  on t.newest_id=b.uuid  where t.flag='1'";
        String jgSql  = "select id,uuid,layout from odsdb.ori_newest_info_base where id >276205 and remark is null ";
        //  String jgSql = "select id,uuid,layout from " + tableName + " where uuid in(\"02345a7bbac8d6f061d0c3234b2ea674\",\"225beb858e1712be0f750f89007154f9\",\"45e200deb651a8d04b5c9f0e8804248b\",\"4b1149fbae8cca05f9318143613d91dd\",\"4c2a2bd3a28ff2dcba4362cd9d32676c\",\"520837cb74c862eced86336181c38e66\",\"5d335cc4f55431c70fd52f58503a0571\",\"6c33fdbdc76a7a3739441a6b5cef66e1\",\"6f032e995629c4ee84b9bbff4c892ae9\",\"87221f80b8d8ecca4f263a23abd1ad20\",\"8d6ab9ff7074d6f8ba0400ceceb36412\",\"9b3f206656ba96d3182763e7a14be4b1\",\"a78d729566162d90b0df10a6967cc0a8\",\"b239f002d92d4ebaec994ba64fc772e7\",\"c1323f1575179339d866ff503fcdcf1c\",\"c56dde50084f8fcec3752e3c544c3be0\",\"e4fd592febeb94751bec6aa72f0c2e6e\",\"f17250f3a44639b0fbe7a4471b36d3e4\",\"ff698edd1288fab42dc5497fd9d595bc\",\"23f955cdf9ab41bad8b08cadfd38d843\")";
        DataSet<Row> dwbNewestLayoutDataSet = env.createInput(
                JDBCInputFormat.buildJDBCInputFormat()
                        .setDrivername("com.mysql.cj.jdbc.Driver")
                        .setDBUrl(url)
                        .setUsername(username)
                        .setPassword(password)
                        .setQuery(jgSql)
                        .setRowTypeInfo(dwbNewestLayoutRowType)
                        .finish()
        );
//        System.out.println("源表数据量：" + dwbNewestLayoutDataSet.count());

        DataSet<Row> dwGroupTagDataSet = dwbNewestLayoutDataSet.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public void flatMap(Row row, Collector<Row> collector) throws Exception {
                Object layout = row.getField(2);
                if (layout != null && !"".equals(layout.toString())) {
                    Row r = new Row(9);
                    r.setField(0, row.getField(0));     //layout_id
                    r.setField(1, row.getField(1));     //newest_id
                    String[] houseTypes = layout.toString().split("\\^", -1);
                    if (houseTypes.length == 0) {
                        return;
                    }
                    for (String houseType : houseTypes) {
                        String[] details = houseType.split("\\|", -1);
                        if (details.length == 0) {
                            continue;
                        }
                        if (details.length != 4) {
                            System.out.println("layout户型长度不正确,id =" + row.getField(0));
                            continue;
                        }

                        String detail1 = details[0];
                        String detail2 = details[1];
                        String detail3 = details[3];
                        int index1 = detail1.indexOf("室");
                        int room = index1 == -1 ? 0 : Integer.parseInt(detail1.substring(0, index1));
                        int index2 = detail1.indexOf("厅");
                        int hall = index2 == -1 ? 0 : index1 == -1 ? 0 : Integer.parseInt(detail1.substring(index1 + 1, index2));
                        if (index2 == -1) {
                            index2 = index1;
                        }
                        int index3 = detail1.indexOf("厨");
                        if (index3 == -1) {
                            index3 = index2;
                        }
                        int index4 = detail1.indexOf("卫");
                        int bathroom = index4 == -1 ? 0 : index3 == -1 ? 0 : Integer.parseInt(detail1.substring(index3 + 1, index4));
                        r.setField(2, room);                                //room
                        r.setField(3, hall);                                //hall
                        r.setField(4, bathroom);                            //bathroom
                        r.setField(5, DigitalUtil.getDigital(detail3));     //layout_area
                        r.setField(6, detail3);                             //layout_area_str
                        r.setField(7, DigitalUtil.getDigital(detail2));     //layout_price
                        r.setField(8, detail2);                             //layout_price_str
                        collector.collect(r);
                    }

                }
            }
        });
//        System.out.println("插入 dim_group_tag_order 表数据量：" + dwGroupTagDataSet.count());

        //对newest_id,room,hall,bathroom,layout_area,layout_price字段去重
        dwGroupTagDataSet = dwGroupTagDataSet.distinct(r -> r.getField(1).toString()
                                                            .concat(r.getField(2).toString())
                                                            .concat(r.getField(3).toString())
                                                            .concat(r.getField(4).toString())
                                                            .concat(r.getField(5).toString())
                                                            .concat(r.getField(7).toString()));
//        System.out.println("去重后插入 dim_group_tag_order 表数据量：" + dwGroupTagDataSet.count());


        //插入 dwb_db.dwb_newest_layout 表
        String dimGroupTagDataSql = "insert into dwb_db.dwb_newest_layout(layout_id,newest_id,room,hall,bathroom,layout_area,layout_area_str,layout_price,layout_price_str) values(?,?,?,?,?,?,?,?,?)";
        dwGroupTagDataSet.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl(url)
                .setUsername(username)
                .setPassword(password)
                .setQuery(dimGroupTagDataSql)
                .finish()
        );

        env.execute(tableName + "户型表清洗任务");
    }

}
