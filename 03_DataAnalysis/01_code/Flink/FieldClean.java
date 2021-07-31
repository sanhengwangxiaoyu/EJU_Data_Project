package com.eju.bigdata.task.newest.tempdb;

import com.eju.bigdata.entity.imei.UpdateFieldDTO;
import com.eju.bigdata.entity.newest.GeographyDTO;
import com.eju.bigdata.entity.newest.OriNewestInfoSite;
import com.eju.bigdata.sqlclient.NewestMysqlClient;
import com.eju.bigdata.task.newest.RegexNewest;
import com.eju.bigdata.util.BeanTool;
import com.eju.bigdata.util.PropertyTypeUtil;
import com.eju.bigdata.util.StringTool;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.*;
import java.util.stream.Collectors;

import static com.eju.bigdata.sqlclient.NewestMysqlClient.*;
import static com.eju.bigdata.task.newest.RegexNewest.*;
import static com.eju.bigdata.util.StringTool.isNotBlank;

/***
 * temp_db库数据字段清洗
 */
public class FieldClean {

    private static final Map<String, Integer> regionDict = new HashMap<>();
    private static final Map<String, GeographyDTO> cityDict = null;//makeCityDict();//makeShortCityDict();

    public static void main(String[] args) throws Exception {
        String fromTable = "tmp_city_newest_deal";//ori_newest_info_archive_main

        //resetFeildData(fromTable);
        singleFeildData(fromTable);
        //manyFeildData(fromTable);
    }

    /**
     * 单字段清洗
     select distinct newest_name  from odsdb.ori_newest_info_202104_clean where remark is null and newest_name != '' and newest_name regexp '(·|•|\\.|丨|期)';
     * @throws Exception
     */
    public static void singleFeildData(String tableName) throws Exception {
        String fieldName = "floor_name_clean";//property_type newest_name alias unit_price recent_opening_time recent_delivery_time property_fee green_rate volume_rate building_type household_num right_term property_comp gd_city address sale_status issue_number sale_phone sale_address
        RowTypeInfo rowTypeInfo = new RowTypeInfo(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        String cols = "id,".concat(fieldName);
        StringBuffer querySql = new StringBuffer("select ").append(cols).append(" from ").append(tableName).append(" where remark is null and ").append(fieldName).append(" != '' ");//limit 1000
        String fieldRegex = getFieldRegex(fieldName);
        if (fieldRegex != null) {
            querySql.append("and ").append(fieldName).append(fieldRegex);
        }
        System.out.println(querySql.toString());
        //列名的数组索引值映射
        Map<String, Integer> colMap = BeanTool.getColMap(cols.replaceAll(" ", ""));

        DataSet<Row> dataSet = getDataSetOdsdb(rowTypeInfo, querySql.toString());
        List<UpdateFieldDTO> dataList = new ArrayList<>(1023);
        for (Row row : dataSet.collect()) {
            String content = String.valueOf(row.getField(colMap.get(fieldName)));
            String fieldValue = getFieldValue(content, fieldName);
            if (!content.equals(fieldValue) || "property_type".equals(fieldName)) {
                dataList.add(new UpdateFieldDTO(Long.parseLong(row.getField(colMap.get("id")).toString()), fieldValue));
            }
        }
        System.out.println("###查询完成，准备更新..." + dataList.size());
        if (dataList.size() > 0) {
            if ("property_type".equals(fieldName)) {
                fieldName = "property_sub";
                dataList = dataList.stream().filter(o -> StringTool.isNotEmpty(o.getFieldValue())).collect(Collectors.toList());
            }
            NewestMysqlClient.updateFieldBatch(tableName, fieldName, dataList);
        }
        System.out.println("### singleFeildData() finish:" + dataList.size());
    }

    private static String getFieldRegex(String fieldName) {
        String fieldRegex = null;
        switch (fieldName) {
            case "newest_name" :
                fieldRegex = String.format(" regexp '(%s|·|\\\\.|丨|,|;|，|；|、|\\\\/|\\\\|)'", special_symbol_sql);//|,|;|，|；|、|\\/|\\|
                break;
            case "alias" :
                fieldRegex = String.format(" regexp '(%s|别名：|别名:|楼盘别名|^-$|^无$|^\\[|\\]$)'", special_symbol_sql);
                break;
            case "building_type" :
                fieldRegex = String.format(" regexp '(%s|未知|暂无|资料|数据|信息|暂无|0)'", special_symbol_sql);
                break;
            case "sale_address" :
                fieldRegex = String.format(" regexp '(%s|未知|尚未|暂无|尚无)'", special_symbol_sql);
                break;
            case "unit_price" :
            case "land_area" :
            case "building_area" :
            case "household_num" :
            case "property_fee" :
            case "park_num" :
            case "right_term" :
                fieldRegex = " not regexp '^[0-9]+(.?)[0-9]*$'";
                break;
            case "recent_opening_time" :
            case "recent_delivery_time" :
            case "issue_date" :
                fieldRegex = " not regexp '([0-9]{4}\\-[0-9]{1,2}\\-[0-9]{1,2})'";
                break;
            case "park_rate" :
            case "sale_phone" :
                fieldRegex = " not regexp '[0-9]+(.?)[0-9]*'";
                break;
            //case "issue_number" :
            //case "green_rate" :
            //case "volume_rate" :
            //case "property_comp" :
            //case "sale_status" :
        }
        return fieldRegex;
    }

    private static String getFieldValue(String content, String fieldName) {
        String fieldValue = findRegexReplaceNull(content, special_symbol);
        switch (fieldName) {
            case "newest_name" :
                fieldValue = getNewestName(fieldValue);
                break; //特殊字符统一替换成【-】，特殊字符包含【·，•，.，|】，验收SQL: select distinct newest_name  from odsdb.ori_newest_info_202104_clean where remark is null and newest_name != '' and newest_name regexp '(·|•|\\.|丨|期)';
            case "alias" :
                fieldValue = getAlias(fieldValue);
                break; //如字段内容包含有效楼盘名称（eg：格式为-别名：XXX楼盘），则去除前缀【别名：】，验收SQL: select distinct alias  from odsdb.ori_newest_info_202104_clean where remark is null and alias != '' and alias regexp '(别名：|别名:|楼盘别名|^-$|^无$|^\[|\]$)';
            case "unit_price" :
                fieldValue = getUnitPrice(fieldValue);
                break;//保留有效单价，验收SQL: select distinct unit_price  from odsdb.ori_newest_info_202104_clean where remark is null and unit_price != '' and unit_price not regexp '^[0-9]+(.?)[0-9]*$';
            case "recent_opening_time" :
                //保留有效最新开盘日期，验收SQL: select distinct recent_opening_time from odsdb.ori_newest_info_202104_clean where remark is null and recent_opening_time != '' and recent_opening_time not regexp '([0-9]{4}\\-[0-9]{1,2}\\-[0-9]{1,2})';
            case "recent_delivery_time" :
                fieldValue = getRecentDeliveryTime(fieldValue, "-");
                break;//保留有效最新开盘日期，验收SQL: select distinct recent_delivery_time from odsdb.ori_newest_info_202104_clean where remark is null and recent_delivery_time != '' and recent_delivery_time not regexp '([0-9]{4}\\-[0-9]{1,2}\\-[0-9]{1,2})';
            case "issue_date" :
                fieldValue = getIssueDate(fieldValue,"-");
                break;//保留有效预证信息，验收SQL: select distinct issue_date from odsdb.ori_newest_info_202104_clean where remark is null and issue_date != '' and issue_date not regexp '[-]';
            case "land_area" :
                //保留有效土地面积，验收SQL: select distinct land_area  from odsdb.ori_newest_info_202104_clean where remark is null and land_area != '' and land_area not regexp '^[0-9]+(.?)[0-9]*$';
            case "building_area" :
                fieldValue = getLandArea(fieldValue);
                break;//保留有效建筑面积，验收SQL: select distinct building_area  from odsdb.ori_newest_info_202104_clean where remark is null and building_area != '' and building_area not regexp '^[0-9]+(.?)[0-9]*$';
            case "household_num" :
                fieldValue = getHouseholdNum(fieldValue);
                break;//保留有效户数，验收SQL: select distinct household_num  from odsdb.ori_newest_info_202104_clean where remark is null and household_num != '' and household_num not regexp '^[0-9]+(.?)[0-9]*$';
            case "property_fee" :
                fieldValue = getPropertyFee(fieldValue);
                break;//保留有效物业费，验收SQL: select distinct property_fee  from odsdb.ori_newest_info_202104_clean where remark is null and property_fee != '' and property_fee not regexp '^[0-9]+(.?)[0-9]*$';
            case "park_num" :
                fieldValue = getParkNum(fieldValue);
                break;//保留有效车位数量，验收SQL: select distinct park_num  from odsdb.ori_newest_info_202104_clean where remark is null and park_num != '' and park_num not regexp '^[0-9]+(.?)[0-9]*$';
            case "right_term" :
                fieldValue = getRightTerm(fieldValue);
                break;//保留有效产权年限，验收SQL: select distinct right_term  from odsdb.ori_newest_info_202104_clean where remark is null and right_term != '' and right_term not regexp '^[0-9]+(.?)[0-9]*$'
            case "park_rate" :
                fieldValue = findRegexSetNull(fieldValue, regex_int);
                break;//select distinct park_rate  from odsdb.ori_newest_info_202104_clean where remark is null and park_rate != '' and park_rate not regexp '[0-9]+(.?)[0-9]*'
            case "issue_number" :
                fieldValue = getIssueNumber(fieldValue);
                break;//保留有效预售证号，验收SQL: select distinct issue_number  from odsdb.ori_newest_info_202104_clean where remark is null and issue_number != '' and issue_number regexp '（|）|暂无|暂未|预售许可证|预备字|(^\\|$)';
            case "green_rate" :
                fieldValue = getGreenRate(fieldValue);
                break;//保留有效绿化率，验收SQL: select distinct green_rate from odsdb.ori_newest_info_202104_clean where remark is null and green_rate != '' and green_rate not regexp '^[0-9]+(.?)[0-9]*$';
            case "volume_rate" :
                fieldValue = getVolumeRate(fieldValue);
                break;//保留有效容积率，验收SQL: select distinct cast(volume_rate as decimal(10,2))  from odsdb.ori_newest_info_202104_clean where remark is null and volume_rate != '' order by cast(volume_rate as decimal(10,2)) desc;
            case "building_type" :
                fieldValue = findRegexReplaceNull(fieldValue, "未知|暂无|资料|数据|信息|0");
                break;//保留有效建筑类型，验收SQL: select distinct building_type  from odsdb.ori_newest_info_202104_clean where remark is null and building_type != '' and  building_type regexp '(待定|暂无|未知|^0$)';
            case "property_comp" :
                fieldValue = getPropertyComp(fieldValue);
                break;//保留有效物业公司，验收SQL: select distinct property_comp  from odsdb.ori_newest_info_202104_clean where remark is null and property_comp != '' and property_comp  regexp '(^[0-9]+(.?)[0-9]*$)|暂无信息|暂无资料|待定|(^·$)|(^-$)'
            case "sale_status" :
                fieldValue = getSaleStatus(fieldValue);
                break;
            case "address" :
                //fieldValue = findRegexReplaceNull(fieldValue, "'");
                break;
            case "sale_phone" :
                fieldValue = findRegexReplaceNull(fieldValue, "暂无数据");
                break;//select distinct sale_phone from ori_newest_info_202104_clean where remark is null and sale_phone != '' and sale_phone not regexp '[0-9]+(.?)[0-9]*';
            case "sale_address" :
                fieldValue = findRegexReplaceNull(fieldValue, "售楼处暂无开放，敬请期待！|售楼处暂无|尚无售楼处|暂无相关信息|暂无信息|暂无数据|暂无资料|暂无售楼处|项目暂无售楼处|暂无展厅|暂无公开|尚未公开|尚未开放|尚未开发|售楼处尚未开放|（接待时间.*）|^暂无$");
                break;//select distinct sale_address from ori_newest_info_202104_clean where remark is null and sale_address != '' and sale_address regexp '(未知|尚未|暂无|尚无)';

        }

        return fieldValue;
    }

    /**
     * 先完善地理信息表：
     * ALTER TABLE dw_v2.dim_geography ADD city_short varchar(32) NULL COMMENT '城市简称' AFTER `city_name`;
     * ALTER TABLE dw_v2.dim_geography ADD region_short varchar(32) NULL COMMENT '区县简称' AFTER `region_name`;
     * update dw_v2.dim_geography g set g.city_short = ifnull((select distinct c.city_name from dw.dim_city c where c.full_name = g.city_name), g.city_name) where g.city_name is not null and g.city_name != '';
     * update dw_v2.dim_geography g set g.region_short = ifnull((select distinct c.short_name from odsdb.county_dict c where c.city_name = g.city_name and c.county_name = g.region_name), g.region_name)
     * where g.region_name is not null and g.region_name != '';
     * @return
     */
    private static Map<String, GeographyDTO> makeShortCityDict() {
        RowTypeInfo rowTypeInfo = new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        String querySql = "select distinct city_id, city_name, city_short, region_id, region_name from dim_geography where city_id is not null";

        DataSet<Row> dataSet = getDataSetDwv2(rowTypeInfo, querySql);

        Map<String, GeographyDTO> cityShortDict = new HashMap<>();
        try {
            Integer city_id;
            String city_name;
            String city_short;
            Integer region_id;
            String region_name;
            String key;
            for (Row row : dataSet.collect()) {
                city_id = Integer.parseInt(row.getField(0).toString().trim());
                city_name = String.valueOf(row.getField(1)).trim();
                city_short = String.valueOf(row.getField(2)).trim();
                region_id = row.getField(3)==null ? 0 : Integer.parseInt(row.getField(3).toString().trim());
                region_name = String.valueOf(row.getField(4)).trim();
                cityShortDict.putIfAbsent(city_short, new GeographyDTO(city_id, city_name, region_id, region_name));
                key = getRegionKey(city_short, region_name);
                regionDict.put(key, region_id);
                //System.out.println("key:"+key+",value:"+regionDict.get(key));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cityShortDict;
    }
    private static Map<String, GeographyDTO> makeCityDict() {
        RowTypeInfo rowTypeInfo = new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        String querySql = "select distinct city_id, city_name, city_short, region_id, region_name from dim_geography where city_id is not null";

        DataSet<Row> dataSet = getDataSetDwv2(rowTypeInfo, querySql);

        Map<String, GeographyDTO> cityDict = new HashMap<>();
        try {
            Integer city_id;
            String city_name;
            String city_short;
            Integer region_id;
            String region_name;
            String key;
            for (Row row : dataSet.collect()) {
                city_id = Integer.parseInt(row.getField(0).toString().trim());
                city_name = String.valueOf(row.getField(1)).trim();
                region_id = row.getField(3)==null ? 0 : Integer.parseInt(row.getField(3).toString().trim());
                region_name = String.valueOf(row.getField(4)).trim();
                cityDict.putIfAbsent(city_name, new GeographyDTO(city_id, city_name, region_id, region_name));
                key = getRegionKey(city_name, region_name);
                regionDict.put(key, region_id);
                //System.out.println("key:"+key+",value:"+regionDict.get(key));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cityDict;
    }

    private static String getRegionKey(String city_name, String region_name) {
        return city_name.concat("#").concat(region_name);
    }

    private static String getGeo(String fieldName, String content, String city_name, String region_name) {
        if (StringTool.isNotEmpty(city_name)) {
            switch (fieldName) {
                case "gd_city":
                    content = (cityDict.get(city_name)==null ? content : cityDict.get(city_name).getCity_name());
                    break;
                case "city_id":
                    content = (cityDict.get(city_name)==null ? content : cityDict.get(city_name).getCity_id().toString());
                    break;
                case "county_id":
                    String key = getRegionKey(city_name, region_name);
                    //System.out.println("key:"+key+",value:"+regionDict.get(key));
                    content = (regionDict.get(key)==null ? content : regionDict.get(key).toString());
                    break;
            }
        }
        return content;
    }

    /**
     SELECT distinct newest_name from odsdb.ori_newest_info_202104_clean where newest_name regexp '期';
     *
     * @param content
     * @return
     */
    private static String getNewestName(String content) {
        //content = content.replaceAll("[·•\\.]", "-").trim();
        content = content.replaceAll("[·\\.]", "•").trim();
        content = content.replaceAll(alias_regex, "&").trim();
        content = RegexNewest.dealQiShu(content, "|V期|1.期");

        return content;
    }

    /**
     SELECT distinct alias from odsdb.ori_newest_info_202104_clean where alias regexp '期';
     *
     * @param content
     * @return
     */
    private static String getAlias(String content) {
        content = findRegexReplaceNull(content, "别名：|别名:|楼盘别名|^-$|^无$|^\\[|\\]$");
        /*String aliasRegex = "[,;，；丨、\\/\\|]";
        StringBuffer sb = new StringBuffer();
        Set<String> aliases = Arrays.asList(content.split(aliasRegex)).stream().filter(s -> isNotBlank(s)).collect(Collectors.toSet());
        if (aliases.size() > 0) {
            for (String alias : aliases) {
                sb.append(",").append(RegexNewest.dealQiShu(alias, "|首期.*|5A期"));
            }
            content = sb.deleteCharAt(0).toString();
        }*/

        return content;
    }



}

