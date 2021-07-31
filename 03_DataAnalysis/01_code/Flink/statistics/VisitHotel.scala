package com.statistics

import com.conf.ConfigurationManager
import com.constants.Constant
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.{JDBCInputFormat, JDBCOutputFormat}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

import scala.collection.mutable.ListBuffer

/**
  * 执行完代码后，执行下面SQL
  *
  * 0. 首先要清空临时表
  * delete from qyf_tmp.t6;
  * 1.
  * insert into qyf_tmp.t6
  * select t1.quarters,t1.city_id,t1.newest_id,types,visit_times,total_times ,visit_times/total_times from (
  * select * from qyf_tmp.dws_visithoteltyperate_quarter where types != '')t1
  * left join (
  * select t.quarters,t.city_id,t.newest_id,sum(t.visit_times) as total_times
  * from (select * from qyf_tmp.dws_visithoteltyperate_quarter where types != '')t
  * group by quarters,city_id,newest_id )t2
  * on t1.quarters =t2.quarters and t1.city_id =t2.city_id and t1.newest_id = t2.newest_id;
  *
  * 2.
  * insert into dws_db.dws_visithoteltyperate_quarter
  * (city_id,quarters,newest_id,types,rate)
  * select city_id ,if(quarters=1,'2021Q1',if(quarters =4,'2020Q4',quarters))as quarters,newest_id,types,rate from qyf_tmp.t6;
  */
object VisitHotel {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val sql =
      """
        |select imei,hotel_level_prefer
        |from dwb_db.dwb_customer_imei_tag
        |where hotel_level_prefer is not null
      """.stripMargin
    val sql2 =
      """
        |select hotel_level_prefer,front from qyf_tmp.mapping
      """.stripMargin

    val sql3 =
      """
        |select imei,newest_id,quarter(visit_date) as quarters,city_id
        |from dwb_db.dwb_customer_browse_log dcbl
      """.stripMargin

    val dataSource = jdbcRead(env,sql,new RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    ))
    val dataSource2 = jdbcRead(env,sql2,new RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    )).map(row =>{(row.getField(0).toString,row.getField(1).toString)})


    val result = dataSource.flatMap(row =>{
      val list = ListBuffer[(String,String,Int)]()
      val arr: Array[String] = row.getField(1).toString.split(",")
      for(i <- arr){
        list.append((row.getField(0).toString,i,1))
      }
      list
    })
      .leftOuterJoin(dataSource2)
      .where(1)
      .equalTo(0)
      .apply((l, f) => {
        if (f == null){
          (
            l._1,//imei
            "",//front
            l._3//countTimes
          )
        }else{
          (
            l._1,//imei
            f._2,//front
            l._3//countTimes
          )
        }
      })
    val datasource3 = jdbcRead(env, sql3, new RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO, //imei
      BasicTypeInfo.STRING_TYPE_INFO, //newest_id
      BasicTypeInfo.LONG_TYPE_INFO, //quarters
      BasicTypeInfo.STRING_TYPE_INFO //city_id
    )).map(row =>{
      (row.getField(0).toString,//imei
        row.getField(1).toString,//newest_id
      row.getField(2).toString.toInt,//quarters
      row.getField(3).toString)//city_id
    })

    val result2: DataSet[Row] = datasource3.leftOuterJoin(result).where(0).equalTo(0)
      .apply((l,f)=>{
        if(f != null){
          (
            l._4,
            l._3,
            l._2,
            f._2,
            1
          )
        }else{
          (
            l._4,//city_id
            l._3,//quarters
            l._2,//newest_id
            "",//标准名称
            1
          )
        }
      }).groupBy(0,1,2,3).sum(4).map(line =>{
      val row = new Row(5)
      row.setField(0,line._1)
      row.setField(1,line._2)
      row.setField(2,line._3)
      row.setField(3,line._4)
      row.setField(4,line._5)
      row
    })



    val sql4=
      """
        |insert into qyf_tmp.dws_visithoteltyperate_quarter
        |(city_id,quarters,newest_id,types,visit_times) values(?,?,?,?,?)
      """.stripMargin
    jdbcWrite(result2,sql4)

    env.execute("write to mysql")

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
