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
  * 业务逻辑，
  *   0. 首先要清空临时表
  *     truncate table qyf_tmp.t4;
  *     truncate table qyf_tmp.t5;
  *   1. 先按一类标签统计top10
  *
  *   2. 然后基于一类标签的前十个计算每个一类标签的前十个二类标签
  *
  *   3. 通过SQL计算rate
  *   insert into qyf_tmp.t5
  *   select period,city_id,newest_id,type1,type1CNT ,type2 ,type2CNT ,type2CNT / type1CNT  as rates from qyf_tmp.t4 where type1 !='';
  *   4.如何入到最终结果表
  *   insert into dws_db.dws_tag_purchase_prefer2
  *   (period,city_id,newest_id,      value1 ,value3 ,tag_value,value2,value4,tag_name)
  *   select if(period=1,'2021Q1',if(period =4,'2020Q4',period))as period,city_id,newest_id,type1,type1CNT ,type2 ,   type2CNT ,rate,'APP偏好类型二级分类占比TOP10' from qyf_tmp.t5 order by
  *   period ,city_id ,newest_id ,type1 ,type2 ;
  *
  * 5.修改周期字段
  * UPDATE dws_db.dws_tag_purchase_prefer2 SET period='2020Q4' WHERE period='4';
  */
object AppPreferSecondType {
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

    val datasource1 = jdbcRead(env, sql1, new RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    )).flatMap(line => {
      val arr = ArrayBuffer[(String, String)]()
      if (line.getField(1) != null) {
        val types = line.getField(1).toString.split(",")
        for (tp <- types) {
          arr.append((line.getField(0).toString, tp))
        }
      }
      arr
    })//tag

    val dataSource2 = jdbcRead(env, sql2, new RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    ))//type

    val result1: DataSet[(String, String, String)] = datasource1.leftOuterJoin(dataSource2).where(1).equalTo(1).apply(
      (l, f) => {
        if (f == null) {
          (l._1,
            l._2,
            ""
          )
        } else {
          (l._1, //imei
            l._2, //二类标签
            f.getField(0).toString //一类标签
          )
        }
      })
    val datasource3 = jdbcRead(env, sql3, new RowTypeInfo(//log
      BasicTypeInfo.STRING_TYPE_INFO, //imei
      BasicTypeInfo.STRING_TYPE_INFO, //newest_id
      BasicTypeInfo.LONG_TYPE_INFO, //quarters
      BasicTypeInfo.STRING_TYPE_INFO //city_id
    ))

    val datasource4: DataSet[(String, String)] = jdbcRead(env,sql4,new RowTypeInfo(//dwb_newest_info
      BasicTypeInfo.STRING_TYPE_INFO,//newest_id
      BasicTypeInfo.STRING_TYPE_INFO//,city_id
    )).map(row =>{(row.getField(0).toString,
      row.getField(1).toString)})
    val resultss: DataSet[(String, String, Int, String)] = datasource3.leftOuterJoin(datasource4).where(1).equalTo(0).apply(
      (l,f) =>{
        if(f != null){
          (
            l.getField(0).toString,////imei
            l.getField(1).toString,////newest_id
            l.getField(2).toString.toInt,////quarters
            f._2.toString,////city_id
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
          (l._1, //imei
            l._3, //quarters
            l._2, //newest_id
            l._4, //city_id
            f._3, //一类标签
            f._2, //二类标签
            1
          )
        }
      })
    val result3: DataSet[Row] = result2.groupBy(1, 3, 2, 4).sum(6).map(line => {
      (
        line._2, //quarters
        line._4, //city_id
        line._3, //newest_id
        line._5, //一类标签
        line._6, //二类标签
        line._7, //一类imei数量
      )
    }).groupBy(0, 1, 2, 3).sortGroup(5, Order.DESCENDING).first(10)
      .leftOuterJoin(result2).where(0,1,2,3).equalTo(1,3,2,4).apply(
      (l,f) =>{
        if(f != null){
          (
            l._1, //quarters
            l._2,//city_id
            l._3,//newest_id
            l._4,//一类标签
            l._6,//一类标签数量
            f._6,//二类标签
            f._7,// 单位 1
          )

        }else{
          (
            l._1, //quarters
            l._2,//city_id
            l._3,//newest_id
            l._4,//一类标签
            l._6,//一类标签数量
            "",//二类标签
            0,// 单位 1
          )
        }
      }
    )

    .groupBy(0,1,2,3,5).sum(6).map(line =>{
      (
        line._1, //quarters
        line._2, //city_id
        line._3, //newest_id
        line._4, //一类标签
        line._5,//一类标签数量
        line._6, //二类标签
        line._7,//二类标签数量
        0
      )
    }).groupBy(0, 1, 2, 3, 5).sortGroup(6, Order.DESCENDING).first(10)
      .map(line =>{
      val row = new Row(8)

      row.setField(0,line._1)//quarters
      row.setField(1,line._2)//city_id
      row.setField(2,line._3)//newest_id
      row.setField(3,line._4)//一类标签
      row.setField(4,line._5)//一类标签数量
      row.setField(5,line._6)//二类标签
      row.setField(6,line._7)//二类标签数量
      row.setField(7,line._8)// rate
      row
    })

//    result3.print()


    val insertSQL =
      """
        |insert into qyf_tmp.t4 values(?,?,?,?,?,?,?,?)
      """.stripMargin

    jdbcWrite(result3,insertSQL)

    env.execute("AppPreferSecondType")
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
    ).setParallelism(1)
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
