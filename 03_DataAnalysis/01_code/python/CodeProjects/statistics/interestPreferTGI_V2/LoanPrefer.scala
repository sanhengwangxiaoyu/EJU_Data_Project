package com.statistics.interestPreferTGI_V2

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.{JDBCInputFormat, JDBCOutputFormat}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object LoanPrefer {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment


    val querySQL1 =
      """
        |SELECT imei,loan_prefer,concat_ws ('',social_prefer,read_prefer,game_prefer,live_prefer,
        |loan_prefer,bank_prefer,invest_prefer,habit,consume_prefer) as totals
        |FROM dwb_db.dwb_customer_imei_tag where ori_table='origin_estate.ori_jiguang_personal_tag_i'

      """.stripMargin

    val querySQL2 =
      """
        |select imei,newest_id,quarter(visit_date) as quarters,city_id
        |from dwb_db.dwb_customer_browse_log

      """.stripMargin

    val sql4 =
      """
        |select newest_id,city_id as city from dwb_db.dwb_newest_info

      """.stripMargin

    val tagDataSource = jdbcRead(env, querySQL1, new RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO, //imei
      BasicTypeInfo.STRING_TYPE_INFO, //social_prefer
      BasicTypeInfo.STRING_TYPE_INFO, //totals
    ))

    val logDataSource = jdbcRead(env, querySQL2, new RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO, //imei
      BasicTypeInfo.STRING_TYPE_INFO, //newest_id
      BasicTypeInfo.LONG_TYPE_INFO, //quarters
      BasicTypeInfo.STRING_TYPE_INFO //city_id
    ))

    val datasource4: DataSet[Row] = jdbcRead(env,sql4,new RowTypeInfo(//dwb_newest_info
      BasicTypeInfo.STRING_TYPE_INFO,//newest_id
      BasicTypeInfo.STRING_TYPE_INFO//,city_id
    ))

    val resultss: DataSet[(String, String, Int, String)] = logDataSource.leftOuterJoin(datasource4).where(1).equalTo(0).apply(
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

    val joinData = resultss.join(tagDataSource).where(0).equalTo(0)
      .apply(
        (l, f) => {

          (l._1, //imei
            l._3, //quarters
            l._2, //newest_id
            l._4, //city_id, //city_id
            if (f.getField(1) !=null &&f.getField(2) !="")"理财偏好" else "", //social_prefer
            if (f.getField(2) == null) "" else f.getField(2).toString, //totals
            1
          )
        }
      )
    //按季度、城市、楼盘 分组
    val value1 = joinData.filter(line =>{ line._5 !=""}).groupBy(1, 3, 2).sum(6).map(line => {
      (line._2,//quarters
        line._3,//newest_id
        line._4,//city_id
        line._5,//social_prefer
        line._7)//social_prefer的个数 value1
    })



    //按季度、城市、楼盘 分组
    val value2 = joinData.filter(line =>{line._6 != "" }).groupBy(1, 3, 2).sum(6).map(line => {
      (line._2,//quarters
        line._3,//newest_id
        line._4,//city_id
        line._6,//totals
        line._7)//totals个数 value2
    })


    //按季度、城市 分组
    val value3 = joinData.filter(line =>{line._5 !=null}).groupBy(1,3).sum(6).map(line => {
      (line._2,//quarters
        line._4,//city_id
        line._7//value3
      )
    })
    //按季度、城市 分组
    val value4 = joinData.filter(line =>{line._6 != "" }).groupBy(1,3).sum(6).map(line => {
      (line._2,//quarters
        line._4,//city_id
        line._6,//totals
        "兴趣偏好TGITOP10",
        line._7)//totals 个数
    })


    val result3 =value1.leftOuterJoin(value2)
      .where(0,1,2).equalTo(0,1,2).apply(
      (l,f) =>{
        if(f != null){
          (
            l._1,//quarters
            l._2,//newest_id
            l._3,//city_id
            l._4,//social_prefer
            l._5,//value1
            f._5//value2
          )
        }else{
          (l._1,//quarters
            l._2,//newest_id
            l._3,//city_id
            l._4,//social_prefer
            l._5,//value1
            0)//value2
        }
      }
    )
      .map(line =>{
        val row =new Row(6)
        row.setField(0,line._1)//quarters
        row.setField(1,line._2)//newest_id
        row.setField(2,line._3)//city_id
        row.setField(3,line._4)//social_prefer
        row.setField(4,line._5)//value1
        row.setField(5,line._6)//value2
        row
      })
    val insertSQL1 =
      """
        |insert into  qyf_tmp.t11
        | (period,newest_id,city_id,tag_value,value1,value2)
        |values (?,?,?,?,?,?)
      """.stripMargin

    jdbcWrite(result3,insertSQL1)

    val result4 = value3.leftOuterJoin(value4).where(0,1).equalTo(0,1).apply(
      (l,f) =>{
        if(f != null){
          (
            l._1,//quarter
            l._2,//city_id
            l._3,//value3
            f._4,//兴趣偏好TGITOP10
            f._5//value4

          )
        }else{
          (
            (
              l._1,//quarter
              l._2,//city_id
              l._3,//value3
              "兴趣偏好TGITOP10",//兴趣偏好TGITOP10
              0//value4

            )
            )
        }
      }
    ).map(line =>{
      val row = new Row(6)
      row.setField(0,line._1)//quarter
      row.setField(1,line._2)//city_id
      row.setField(2,line._3)//value3
      row.setField(3,line._4)//兴趣偏好TGITOP10
      row.setField(4,line._5)//value4
      row.setField(5,"理财偏好")
      row
    })


    val insertSQL2 =
      """
        | insert into  qyf_tmp.t22
        |(period,city_id,value3,tag_name,value4,tag_value)
        |values (?,?,?,?,?,?)
      """.stripMargin
    jdbcWrite(result4,insertSQL2)








    env.execute("SocialPerfer")


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
