package com.statistics

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.java.io.jdbc.{JDBCInputFormat, JDBCOutputFormat}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

import scala.collection.mutable.ArrayBuffer

/**
  * 1. 清空临时表
  * delete from qyf_tmp.t1 ;
  * delete from qyf_tmp.t2 ;
  * 2.这里跑的是临时数据，跑完之后，需要做聚合，将数据写到结果表
  *
  * insert into  dws_db.dws_tag_purchase_prefer
  * (period,newest_id,city_id,tag_value,value1,value2,value3,value4,tag_name,value5)
  * select if(t1.period=1,'2021Q1','2020Q4')as period ,t1.newest_id ,t1.city_id ,t1.tag_value ,value1,value2,value3 ,value4 ,tag_name ,(value1 /value2 )/(value3/value4) as rate
  * from t1
  * join t2
  * on t1.period =t2.period and t1.city_id =t2.city_id and t1.tag_value = t2.tag_value;
  *
  */
object AppPreferFirstType {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment


    val sql1 =
      """
        |select imei,app_prefer from  dwb_db.dwb_customer_imei_tag
      """.stripMargin

    val sql2 =
      """
        |select type1,type2 from dwb_db.app_type at2

      """.stripMargin

    val sql3 =
      """
        |select imei,newest_id,quarter(visit_date) as quarters,city_id
        |from dwb_db.dwb_customer_browse_log dcbl

      """.stripMargin
    val sql4 =
      """
        |select newest_id,city_id as city from dwb_db.dwb_newest_info
      """.stripMargin

    val datasource1: DataSet[(String, String)] = jdbcRead(env,sql1,new RowTypeInfo(//tag
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    )).flatMap(line =>{
      val arr = ArrayBuffer[(String,String)]()
       if(line.getField(1)!=null ){
         val types =line.getField(1).toString.split(",")
        for(tp <- types){
          arr.append((line.getField(0).toString,tp))
        }
      }
      arr
    })

    val dataSource2: DataSet[Row] = jdbcRead(env,sql2,new RowTypeInfo(//app_type
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    ))

    val result1: DataSet[(String, String, String)] = datasource1.leftOuterJoin(dataSource2).where(1).equalTo(1).apply(
      (l,f) =>{
        if(f == null){
          (l._1,
            l._2,
            ""
          )
        }else{
          (l._1,//imei
            l._2,//二类标签
            f.getField(0).toString//一类标签
            )
        }
      })
    val datasource3: DataSet[Row] = jdbcRead(env,sql3,new RowTypeInfo(//log
      BasicTypeInfo.STRING_TYPE_INFO,//imei
      BasicTypeInfo.STRING_TYPE_INFO,//newest_id
      BasicTypeInfo.LONG_TYPE_INFO,//quarters
      BasicTypeInfo.STRING_TYPE_INFO//city_id
    ))

    val datasource4: DataSet[Row] = jdbcRead(env,sql4,new RowTypeInfo(//dwb_newest_info
      BasicTypeInfo.STRING_TYPE_INFO,//newest_id
      BasicTypeInfo.STRING_TYPE_INFO//,city_id
    ))

    val resultss: DataSet[(String, String, Int, String)] = datasource3.leftOuterJoin(datasource4).where(1).equalTo(0).apply(
      (l,f) =>{
        if(f != null){
          (
          l.getField(0).toString,////imei
          l.getField(1).toString,////newest_id
          l.getField(2).toString.toInt,////quarters
          f.getField(1).toString,////city_id
          )

        }else{
          (
            l.getField(0).toString,////imei
            l.getField(1).toString,////newest_id
            l.getField(2).toString.toInt,////quarters
            "",////city_id
          )
        }
      }
    )

    val result2= resultss.leftOuterJoin(result1).where(0).equalTo(0).apply(
      (l, f) => {
      if (f == null) {
        (l._1, //imei
          l._3, //quarters
          l._2, //newest_id
          l._4, //city_id
          "", //二类标签
          "", //一类标签
          1
        )
      } else {
        (l._1,//imei
          l._3,//quarters
          l._2,//newest_id
          l._4, //city_id
          f._2, //二类标签
          f._3,//一类标签
          1
        )
      }
    }).filter(line =>{line._5 !="" && line._6 !=""})
    //计算value1,按季度、城市、楼盘、一级标签分组
    val groupByQuartersAndCityAndNewestIDAndType2 = result2.groupBy(1,2,3,5).sum(6).map(line =>{
      (line._2,//quarter
        line._3,//newest_id
        line._4,//city_id
        line._6,//一类标签名
        line._7)//value1
    }).groupBy(0,1,2).sortGroup(4,Order.DESCENDING).first(10)
    //计算value2，按季度、城市、楼盘分组
    val groupByQuartersAndCityAndNewestID: DataSet[(Int, String, String, Int)] =
      groupByQuartersAndCityAndNewestIDAndType2.groupBy(0,2,1).aggregate(Aggregations.SUM,4).map(line =>{
      (line._1,//quarter
        line._2,//newest_id
      line._3,//city_id
      line._5)//value2
    })
    //计算value3，按季度、城市、一级标签分组，取
    val groupByQuartersAndCityDAndType2 = result2.groupBy(1,3,5).sum(6).map(line =>{
      (line._2,//quarter
        line._4,//city_id
        line._6,//一类标签
        line._7)//value3
    }).groupBy(0,1).sortGroup(3, Order.DESCENDING).first(10)
    //计算value4，按季度分组,取前十城市的imei个数
    val imeiTotal = groupByQuartersAndCityDAndType2.groupBy(0,1).sum(3).map(line =>{
      (line._1,line._2,line._4)
    })

    val result3 =groupByQuartersAndCityAndNewestIDAndType2.leftOuterJoin(groupByQuartersAndCityAndNewestID)
      .where(0,1,2).equalTo(0,1,2).apply(
      (l,f) =>{
        if(f != null){
          (
            l._1,//quarters
            l._2,//newest_id
            l._3,//city_id
            l._4,//一类标签
            l._5,//value1
            f._4//value2
          )
        }else{
          (l._1,//quarters
          l._2,//newest_id
          l._3,//city_id
          l._4,//一类标签
          l._5,//一类标签中 每类 的imei数量
          0)//每个楼盘中一类标签总数据量
        }
      }
    )
        .map(line =>{
          val row =new Row(6)
          row.setField(0,line._1)//quarters
          row.setField(1,line._2)//newest_id
          row.setField(2,line._3)//city_id
          row.setField(3,line._4)//一类标签
          row.setField(4,line._5)//value1
          row.setField(5,line._6)//value2
          row
        })
    val insertSQL1 =
      """
        |insert into  qyf_tmp.t1
        | (period,newest_id,city_id,tag_value,value1,value2)
        |values (?,?,?,?,?,?)
      """.stripMargin

    jdbcWrite(result3,insertSQL1)

    val result4 = groupByQuartersAndCityDAndType2.leftOuterJoin(imeiTotal).where(0,1).equalTo(0,1).apply(
      (l,f) =>{
        if(f != null){
          (
            l._1,//quarter
            l._2,//city_id
            l._3,//一类标签
            l._4,//value3
            f._3//value4

          )
        }else{
          (
            l._1,//quarter
            l._2,//city_id
            l._3,//一类标签
            l._4,//value3
            0//value4
          )
        }
      }
    ).map(line =>{
      val row = new Row(6)
      row.setField(0,line._1)//quarter
      row.setField(1,line._2)//city_id
      row.setField(2,line._3)//一类标签
      row.setField(3,line._4)//value3
      row.setField(4,line._5)//value4
      row.setField(5,"APP一级偏好")//tag_name
      row
    })


    val insertSQL2 =
      """
        | insert into  qyf_tmp.t2
        |(period,city_id,tag_value,value3,value4,tag_name)
        |values (?,?,?,?,?,?)
      """.stripMargin
    jdbcWrite(result4,insertSQL2)



    env.execute("AppPreferFirstType")

  }
  def jdbcRead(env: ExecutionEnvironment, sql: String, rti: RowTypeInfo) = {

    val inputMysql: DataSet[Row] = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
      //    .指定驱动名称
      .setDrivername("com.mysql.jdbc.Driver")
      //      url
      .setDBUrl("jdbc:mysql://172.28.36.77:3306/qyf_tmp?useUnicode=true&characterEncoding=utf8")
      .setUsername("mysql")
      .setPassword("egSQ7HhxajHZjvdX")
      .setQuery(sql)
      .setRowTypeInfo(rti)
      .finish()
    )
    inputMysql
  }

  def jdbcWrite(write: DataSet[Row], insertSQL: String) {

    write.output(JDBCOutputFormat.buildJDBCOutputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://172.28.36.77:3306/qyf_tmp?useUnicode=true&characterEncoding=utf8")
      .setUsername("mysql")
      .setPassword("egSQ7HhxajHZjvdX")
      .setQuery(insertSQL)
        .setBatchInterval(10000)
      .finish())

  }

}



