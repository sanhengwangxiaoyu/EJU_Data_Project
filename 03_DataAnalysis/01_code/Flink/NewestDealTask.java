package com.eju.bigdata.task.newest.tempdb;

import com.eju.bigdata.entity.imei.UpdateFieldDTO;
import com.eju.bigdata.entity.newest.OriNewestInfoSite;
import com.eju.bigdata.sqlclient.CustBrowseLogSqlClient;
import com.eju.bigdata.task.newest.RegexNewest;
import com.eju.bigdata.util.Property;
import me.xdrop.fuzzywuzzy.FuzzySearch;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

import static com.eju.bigdata.sqlclient.NewestMysqlClient.getDataSetOdsdb;
import static com.eju.bigdata.sqlclient.NewestMysqlClient.getDataSetTempdb;
import static com.eju.bigdata.task.newest.FieldClean202104.makeOriNewestInfoSite;
import static com.eju.bigdata.task.newest.RegexNewest.*;
import static com.eju.bigdata.task.newest.RegexNewest.getSaleStatus;
import static com.eju.bigdata.util.StringTool.isEmpty;
import static com.eju.bigdata.util.StringTool.isNotBlank;


public class NewestDealTask {


    /**
     * 注：修改JDBCUtils.url为mysql.url.temp_db
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        /*
        update tmp_city_newest_deal set floor_name_clean = floor_name ;
        select count(*) from tmp_city_newest_deal where floor_name != '' and floor_name regexp '(\\\\|"|\'|`|·|\\.|丨|,|;|，|；|、|\\/|\\|)' ;#4065468
        update tmp_city_newest_deal set issue_date_clean = issue_date ;
        select count(*) from tmp_city_newest_deal where issue_date != '' and issue_date not regexp '([0-9]{4}\-[0-9]{1,2}\-[0-9]{1,2})' ; #11442270
        select count(distinct floor_name_clean) from tmp_city_newest_deal
        */
        //批量处理
        /*int totalCount = 11442270;
        int pageSize = 50000;
        int pages = Integer.valueOf((totalCount-1)/pageSize) + 1;
        System.out.println(String.format("pages = %d", pages));
        for (int pageNum = 0; pageNum < pages; pageNum++) {
            System.out.println(String.format("########################surplus pages = %d", pages-pageNum));
            //对单字段进行清洗
            singleFeildData((pageNum * pageSize), pageSize);
            //break;
        }*/
        cleanFloorName2();
    }

    public static void singleFeildData(Integer startIndex, Integer pageSize) throws Exception {
        Long beginTimeMillis = System.currentTimeMillis();
        String tableName = "tmp_city_newest_deal";
        String fieldName = "issue_date_clean";//floor_name_clean issue_date_clean

        RowTypeInfo rowTypeInfo = new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        StringBuilder querySql = new StringBuilder("select id, ");
        querySql.append(fieldName).append(" from ").append(tableName);
        String fieldRegex = getFieldRegex(fieldName);
        if (fieldRegex != null) {
            //querySql.append(" where floor_name").append(" != '' ").append("and floor_name ").append(fieldRegex);
            querySql.append(" where issue_date").append(" != '' ").append("and issue_date ").append(fieldRegex);
        }
        querySql.append(String.format(" limit %d, %d ", startIndex, pageSize)).toString();
        //System.out.println(querySql.toString());
        DataSet<Row> dataSet = getDataSetTempdb(rowTypeInfo, querySql.toString());
        List<UpdateFieldDTO> dataList = new ArrayList<>(1023);
        try {
            for (Row row : dataSet.collect()) {
                String content = String.valueOf(row.getField(1));
                String fieldValue = getFieldValue(content, fieldName);
                if (!content.equals(fieldValue)) {
                    dataList.add(new UpdateFieldDTO(Long.parseLong(row.getField(0).toString()), fieldValue));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (dataList.size() > 0) {
            CustBrowseLogSqlClient.updateFieldBatch(tableName, fieldName, dataList);
            System.out.println("###dataList.size()="+dataList.size());
        }
        System.out.println("### singleFeildData() finish!");
        Long endTimeMillis = System.currentTimeMillis();
        System.out.println(String.format("####################################用时:%d分钟!\n", (endTimeMillis-beginTimeMillis)/1000/60));
    }

    public static void cleanFloorName() throws Exception {
        Long beginTimeMillis = System.currentTimeMillis();
        RowTypeInfo rowTypeInfo = new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        String cols = "id, gd_city, alias, newest_name";
        StringBuffer targetSql = new StringBuffer("select ").append(cols).append(" from ori_newest_info_main").append(" where remark is null ");
        DataSet targetDataSet = getDataSetOdsdb(rowTypeInfo, targetSql.toString());
        List<Row> targetRows = targetDataSet.collect();
        System.out.println("###查询完成 targetRows.size()="+targetRows.size());

        cols = "id, gd_city, floor_name_clean";
        rowTypeInfo = new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        StringBuffer originSql = new StringBuffer("select ").append(cols).append(" from tmp_city_newest_deal");
        DataSet orginDataSet = getDataSetTempdb(rowTypeInfo, originSql.toString());
        List<Row> originRows = orginDataSet.collect();
        System.out.println("###查询完成 originRows.size()="+originRows.size());

        //与城市相关的用【gd_city】分组来做比对去重操作
        Map<String, List<Row>> tarCityGroup = targetRows.stream().collect(Collectors.groupingBy(row -> row.getField(1).toString()));
        Map<String, List<Row>> oriCityGroup = originRows.stream().collect(Collectors.groupingBy(row -> row.getField(1).toString()));
        //以源表为基准
        oriCityGroup.keySet().forEach(key -> {
            System.out.println("===> "+key);
            List<Row> originList = oriCityGroup.get(key);
            List<Row> targetList = tarCityGroup.get(key);
            List<UpdateFieldDTO> dataList = new ArrayList<>(1023);
            originList.stream().forEach(ori -> {
                String floor_name_clean = ori.getField(2).toString();
                if(CollectionUtils.isEmpty(targetList)) {
                    dataList.add(new UpdateFieldDTO(Long.parseLong(ori.getField(0).toString()), floor_name_clean));
                    return;
                } else {
                    //轮循比较
                    targetList.stream().forEach(tar -> {
                        String tAlias = tar.getField(2).toString();
                        //Set<String> tarAlias = Arrays.asList(tAlias.split(",")).stream().filter(s -> isNotBlank(s.trim())).collect(Collectors.toSet());
                        if (String.format(",%s,", tAlias).contains(String.format(",%s,", floor_name_clean))) {
                            //取标准名
                            dataList.add(new UpdateFieldDTO(Long.parseLong(ori.getField(0).toString()), tar.getField(3).toString()));
                        }
                    });
                }

            });
            if (dataList.size() > 0) {
                try {
                    CustBrowseLogSqlClient.updateFieldBatch("tmp_city_newest_deal", "clean_floor_name", dataList);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("###dataList.size()="+dataList.size());
            }
        });


        System.out.println("### cleanFloorName() finish!");
        Long endTimeMillis = System.currentTimeMillis();
        System.out.println(String.format("####################################用时:%d分钟!\n", (endTimeMillis-beginTimeMillis)/1000/60));

    }

    public static void cleanFloorName2() throws Exception {
        // System.currentTimeMillis()相当于是毫秒为单位.使用System.currentTimeMillis()去代替new Date()，效率上会高一点
        Long beginTimeMillis = System.currentTimeMillis();
        // 设置列数据类型
        RowTypeInfo rowTypeInfo = new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        // 设置列名称
        String cols = "id, gd_city, CONCAT(',',alias,',') alias, newest_name";
        // 从ori_newest_info_main中查询cols的字段，并remark为空，platorm按照自定义排序
        StringBuffer targetSql = new StringBuffer("select ").append(cols).append(" from ori_newest_info_main").append(" where remark is null order by FIELD(platform , '贝壳', '房天下', '吉屋', '居里', '搜狐'), id ");
        // 常见dataset对象
        DataSet targetDataSet = getDataSetOdsdb(rowTypeInfo, targetSql.toString());
        // 遍历数据
        List<Row> targetRows = targetDataSet.collect();
        // 打印数据行数
        System.out.println("###查询完成 targetRows.size()="+targetRows.size());
        // 与城市相关的用【gd_city】分组来做比对去重操作
        Map<String, List<Row>> tarCityGroup = targetRows.stream().collect(Collectors.groupingBy(row -> row.getField(1).toString()));

        /*RowTypeInfo crowTypeInfo = new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO);
        DataSet cityDataSet = getDataSetTempdb(crowTypeInfo, "select distinct gd_city from tmp_city_newest_deal where floor_name_clean = clean_floor_name");
        List<String> cityList = cityDataSet.map((MapFunction<Row, String>) row -> {
            return row.getField(0).toString();
        }).collect();*/
        
        List<String> cityList = Arrays.asList("三亚,东莞,南京,哈尔滨,宁波,广州,徐州,惠州,扬州,沈阳,深圳,珠海,长春".split(","));
        System.out.println("###查询完成 cityList.size()="+cityList.size()+"================================>"+cityList.stream().collect(Collectors.joining(",")));

        RowTypeInfo orowTypeInfo = new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        //以目标表为基准
        tarCityGroup.keySet().forEach(key -> {
            List<Row> targetList = tarCityGroup.get(key);
            System.out.println(key+"================================> "+targetList.size());
            if(!cityList.contains(key)) {
                //System.out.println(key+"================================>Null");
                return;
            }
            List<String> citys = cityList.stream().filter(obj -> !obj.equals(key)).collect(Collectors.toList());
            System.out.println(citys.size()+"================================> "+citys.stream().collect(Collectors.joining(",")));

            StringBuffer originSql = new StringBuffer("select id, CONCAT(',',floor_name_clean,',') floor_name_clean, clean_floor_name")
                    .append(" from tmp_city_newest_deal where floor_name_clean = clean_floor_name and gd_city = '").append(key).append("'");
            DataSet orginDataSet = getDataSetTempdb(orowTypeInfo, originSql.toString());
            List<Row> originList = null;
            try {
                originList = orginDataSet.collect();
            } catch (Exception e) {
                e.printStackTrace();
            }

            System.out.println("###查询完成 originList.size()="+originList.size());

            List<UpdateFieldDTO> dataList = new ArrayList<>(1024);
            originList.stream().forEach(ori -> {
                Long id = Long.parseLong(ori.getField(0).toString());
                String floor_name_clean = ori.getField(1).toString();
                String clean_floor_name = ori.getField(2).toString();
                //轮循比较
                targetList.stream().forEach(tar -> {
                    String alias = tar.getField(2).toString();
                    String newest_name = tar.getField(3).toString();
                    //Set<String> tarAlias = Arrays.asList(tAlias.split(",")).stream().filter(s -> isNotBlank(s.trim())).collect(Collectors.toSet());
                    if (!newest_name.equalsIgnoreCase(clean_floor_name) && alias.contains(floor_name_clean)) {
                        //取标准名
                        dataList.add(new UpdateFieldDTO(id, newest_name));
                        return;
                    }
                });
            });
            if (dataList.size() > 0) {
                try {
                    CustBrowseLogSqlClient.updateFieldBatch("tmp_city_newest_deal", "clean_floor_name", dataList);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("###dataList.size()="+dataList.size());
            }
        });

        System.out.println("### cleanFloorName2() finish!");
        Long endTimeMillis = System.currentTimeMillis();
        System.out.println(String.format("####################################用时:%d分钟!\n", (endTimeMillis-beginTimeMillis)/1000/60));
    }

    public static void cleanFloorName22() throws Exception {
        Long beginTimeMillis = System.currentTimeMillis();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        RowTypeInfo rowTypeInfo = new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        String cols = "id, gd_city, alias, newest_name";
        StringBuffer targetSql = new StringBuffer("select ").append(cols).append(" from ori_newest_info_main").append(" where remark is null order by FIELD(platform , '贝壳', '房天下', '吉屋', '居里', '搜狐'), id ");
        DataSet targetDataSet = getDataSetOdsdb(rowTypeInfo, targetSql.toString());
        List<Row> targetRows = targetDataSet.collect();
        System.out.println("###查询完成 targetRows.size()="+targetRows.size());
        //与城市相关的用【gd_city】分组来做比对去重操作
        Map<String, List<Row>> tarCityGroup = targetRows.stream().collect(Collectors.groupingBy(row -> row.getField(1).toString()));

        RowTypeInfo orowTypeInfo = new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        //以目标表为基准
        tarCityGroup.keySet().forEach(key -> {
            List<Row> targetList = tarCityGroup.get(key);
            DataSet<Row> tarDataSet = env.fromCollection(targetList);
            System.out.println(key+"================================> "+targetList.size());

            StringBuffer originSql = new StringBuffer("select id, CONCAT(',',floor_name_clean,',') floor_name_clean, clean_floor_name")
            .append(" from tmp_city_newest_deal where floor_name_clean = clean_floor_name and gd_city = '").append(key).append("'");
            DataSet<Row> orginDataSet = getDataSetTempdb(orowTypeInfo, originSql.toString());
            DataSet<Row> updateDataSet = tarDataSet.join(orginDataSet).where(0).equalTo(0)
                    .with((JoinFunction<Row, Row, Row>) (tar, ori) -> {
                        Row row = new Row(2);
                        if (ori != null && tar != null) {
                            String alias = tar.getField(2).toString();
                            String newest_name = tar.getField(3).toString();
                            String floor_name_clean = ori.getField(1).toString();
                            String clean_floor_name = ori.getField(2).toString();
                            if (!newest_name.equalsIgnoreCase(clean_floor_name) && alias.contains(floor_name_clean)) {
                                //取标准名
                                row.setField(0, ori.getField(0));       //id
                                row.setField(1, tar.getField(3));       //newest_name
                            }
                        }
                        return row;
                    });
            List<Row> updateList = null;
            try {
                updateList = updateDataSet.collect();
            } catch (Exception e) {
                e.printStackTrace();
            }
            if(CollectionUtils.isEmpty(updateList)) {
                System.out.println(key+"================================>Null");
                return;
            }
            System.out.println("###查询完成 updateList.size()="+updateList.size());

            List<UpdateFieldDTO> dataList = new ArrayList<>(1024);
            updateList.stream().forEach(row -> {
                dataList.add(new UpdateFieldDTO(Long.parseLong(row.getField(0).toString()), row.getField(1).toString()));
            });
            if (dataList.size() > 0) {
                try {
                    CustBrowseLogSqlClient.updateFieldBatch("tmp_city_newest_deal", "clean_floor_name", dataList);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("###dataList.size()="+dataList.size());
            }
        });

        System.out.println("### cleanFloorName2() finish!");
        Long endTimeMillis = System.currentTimeMillis();
        System.out.println(String.format("####################################用时:%d分钟!\n", (endTimeMillis-beginTimeMillis)/1000/60));
    }

    public static void cleanFloorName3() throws Exception {
        Long beginTimeMillis = System.currentTimeMillis();
        RowTypeInfo rowTypeInfo = new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        StringBuffer targetSql = new StringBuffer("select id, gd_city, alias, newest_name from ori_newest_info_main where remark is null ");
        DataSet<Row> targetDataSet = getDataSetOdsdb(rowTypeInfo, targetSql.toString());
        System.out.println("###查询完成 targetDataSet.count()="+targetDataSet.count());

        rowTypeInfo = new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        StringBuffer originSql = new StringBuffer("select id, gd_city, floor_name_clean from tmp_city_newest_deal where floor_name_clean = clean_floor_name");
        DataSet<Row> orginDataSet = getDataSetTempdb(rowTypeInfo, originSql.toString());
        //System.out.println("###查询完成 orginDataSet.count()="+orginDataSet.count());

        DataSet<Row> updateDataSet = targetDataSet.coGroup(orginDataSet)
                .where(tar -> tar.getField(1).toString()).equalTo(ori -> ori.getField(1).toString())
                .with((CoGroupFunction<Row, Row, Row>) (itar, iori, collector) -> {
            if(itar.iterator().hasNext() && iori.iterator().hasNext()) {
                List<Row> tarList = new ArrayList<>();
                List<Row> oriList = new ArrayList<>();
                for (Row r : itar) {
                    tarList.add(r);
                }
                for (Row r : iori) {
                    oriList.add(r);
                }
                Row row = new Row(2);
                for (Row ori : oriList) {
                    row.setField(0, ori.getField(0));      //id
                    Long id = Long.parseLong(ori.getField(0).toString());
                    String floor_name_clean = ori.getField(2).toString();

                    for (Row tar : tarList) {
                        String alais = tar.getField(2).toString();
                        if (String.format(",%s,", alais).contains(String.format(",%s,", floor_name_clean))) {
                            String newestName = tar.getField(3).toString();
                            row.setField(1, newestName);      //newest_name
                            collector.collect(row);
                            return;
                        }
                    }
                }
            }

        });
        System.out.println("待更新的数据量：" + updateDataSet.count());

        System.out.println("### cleanFloorName3() finish!");
        Long endTimeMillis = System.currentTimeMillis();
        System.out.println(String.format("####################################用时:%d分钟!\n", (endTimeMillis-beginTimeMillis)/1000/60));
    }

    private static String getFieldRegex(String fieldName) {
        String fieldRegex = null;
        switch (fieldName) {
            case "floor_name_clean" :
                fieldRegex = String.format(" regexp '(%s|·|\\\\.|丨|,|;|，|；|、|\\\\/|\\\\|)'", special_symbol_sql);//|,|;|，|；|、|\\/|\\|
                break;
            case "issue_date_clean" :
                fieldRegex = " not regexp '([0-9]{4}\\-[0-9]{1,2}\\-[0-9]{1,2})'";
                break;
        }
        return fieldRegex;
    }

    private static String getFieldValue(String content, String fieldName) {
        String fieldValue = findRegexReplaceNull(content, special_symbol);
        switch (fieldName) {
            case "floor_name_clean" :
                fieldValue = getFloorName(fieldValue);
                break; //验收SQL:
            case "issue_date_clean" :
                fieldValue = getIssueDate(fieldValue,"-");
                break; //验收SQL:

        }

        return fieldValue;
    }

    /**
     *
     * @param content
     * @return
     */
    private static String getFloorName(String content) {
        //content = findRegexReplaceNull(content, "[·•\\.\\丨\\-\\(\\)]");
        content = content.replaceAll("[·\\.]", "•").trim();
        content = content.replaceAll(alias_regex, "&").trim();
        content = RegexNewest.dealQiShu(content, "|V期|1.期");

        return content;
    }

    /**
     *
     * @param content
     * @param fmt
     * @return
     */
    private static String getIssueDate(String content, String fmt) {
        content = matchRegexSetNull(content,"(^-$)|暂无信息|暂无资料");
        content = findRegexReplaceNull(content,"0:00:00|,.+");
        return content.replaceAll("[./年月]", fmt).replaceAll("日", "");
    }

}
