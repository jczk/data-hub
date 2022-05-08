package org.wuwangfu.com.spark.didi

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

import java.util.Calendar


/**
 * 滴滴海口市出行运营数据分析，使用sparkSQL操作数据，加载hudi表数据，按照业务需求统计
 *
 */
object DidiAnalysisSpark {

  //hudi表中的属性
  val tableName = "tbl_bus_shenzhen"
  val tablePath = "/hudi/spark/tbl_bus_shenzhen"

  /**
   *加载hudi表数据，封装到DataFrame中
   *
   * @param spark
   * @param tablePath
   * @return
   */
  def readFromHudi(spark: SparkSession, tablePath: String):DataFrame = {

    val busDF = spark.read.format("hudi").load(tablePath)
    //选择字段
    busDF.select(
      "product_id","type","traffic_type","pre_total_fee","start_dest_distance","departure_time"
    )
  }

  /**
   * 订单类型统计：字段product_id
   *
   * @param hudiDF
   */
  def reportProduct(hudiDF: DataFrame):Unit = {

    //按照产品线ID分组统计
    val reportDF = hudiDF.groupBy("product_id").count()
    //自定义UDF函数，转换名称
    val to_name = udf(
      (productId: Int) => {
            productId match {
              case 1 => "滴滴专车"
              case 2 => "滴滴企业专车"
              case 3 => "滴滴快车"
              case 4 => "滴滴企业快车"
            }
      }
    )
    //转换名称
    val resultDF = reportDF.select(
      to_name(col("product_id")).as("order_type"),
      col("count").as("total")
    )

    resultDF.printSchema()
    resultDF.show(10,truncate = false)

  }


  /**
   * 订单时效性，字段：type
   *
   * @param hudiDF
   */
  def reportType(hudiDF: DataFrame):Unit = {

    //按照时效性ID分组统计
    val reportDF = hudiDF.groupBy("type").count()

    //自定义UDF函数，转换名称
    val to_name = udf(
      (realtimeType: Int) => {
        realtimeType match {
          case 0 => "实时"
          case 1 => "预约"
        }
      }
    )

    //转换名称
    val resultDF = reportDF.select(
      to_name(col("type")).as("order_realtime"),
      col("count").as("total")
    )

    resultDF.printSchema()
    resultDF.show(10,truncate = false)
  }

  /**
   * 交通类型统计，字段：traffic_type
   *
   * @param hudiDF
   * @return
   */
  def reportTraffic(hudiDF: DataFrame):Unit = {
    //按照交通类型ID分组统计
    val reportDF = hudiDF.groupBy("traffic_type").count()

    //自定义UDF函数，转换名称
    val to_name = udf(
      (tracfficType: Int) => {
        tracfficType match {
          case 0 => "普通散客"
          case 1 => "企业时租"
          case 2 => "企业接机套餐"
          case 3 => "企业送机套餐"
          case 4 => "拼车"
          case 5 => "接机"
          case 6 => "送机"
          case 302 => "跨城拼车"
          case _ => "未知"
        }
      }
    )

    //转换名称
    val resultDF = reportDF.select(
      to_name(col("traffic_type")).as("traffic_type"),
      col("count").as("total")
    )

    resultDF.printSchema()
    resultDF.show(10,truncate = false)

  }

  /**
   * 订单价格统计，先将订单价格划分阶段，再统计各个阶段数目，字段：pre_total_fee
   *
   * @param hudiDF
   * @return
   */
  def reportPrice(hudiDF: DataFrame):Unit = {

    val resultDF = hudiDF.agg(
      //价格 0~15
      sum(
        when(col("pre_total_fee").between(0, 15), 1).otherwise(0)
      ).as("0_15"),
      //价格 16~30
      sum(
        when(col("pre_total_fee").between(16, 30), 1).otherwise(0)
      ).as("16_30"),
      //价格 31~50
      sum(
        when(col("pre_total_fee").between(31, 50), 1).otherwise(0)
      ).as("31_50"),
      //价格 51~100
      sum(
        when(col("pre_total_fee").between(51, 100), 1).otherwise(0)
      ).as("51_100"),
      //价格 100以上
      sum(
        when(col("pre_total_fee").gt(100), 1).otherwise(0)
      ).as("100+")
    )

    resultDF.printSchema()
    resultDF.show(10,truncate = false)

  }

  /**
   * 订单距离统计，先将订单距离划分为不同区间，再统计各区间数目，字段：start_dest_distance
   *
   * @param hudiDF
   */
  def reportDistance(hudiDF: DataFrame):Unit = {

    val resultDF = hudiDF.agg(
      //距离：0-10km
      sum(
        when(col("start_dest_distance").between(0, 10000), 1).otherwise(0)
      ).as("0_10"),
      //距离：10-20km
      sum(
        when(col("start_dest_distance").between(10001, 20000), 1).otherwise(0)
      ).as("10_20"),
      //距离：20-30km
      sum(
        when(col("start_dest_distance").between(20001, 30000), 1).otherwise(0)
      ).as("20_30"),
      //距离：30-50km
      sum(
        when(col("start_dest_distance").between(30001, 50000), 1).otherwise(0)
      ).as("30_50"),
      //距离：50km+
      sum(
        when(col("start_dest_distance").gt(50001), 1).otherwise(0)
      ).as("50+")
    )

    resultDF.printSchema()
    resultDF.show(10,truncate = false)

  }

  /**
   * 订单星期分组统计，先将日期转换为星期，再对星期分组统计，字段：departure_time
   *
   * @param hudiDF
   */
  def reportWeek(hudiDF: DataFrame):Unit = {

    //自定义UDF函数，转换日期为星期
    val to_week = udf(
      (dateStr: String) => {
        val format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
        val calendar = Calendar.getInstance()

        val date = format.parse(dateStr)
        calendar.setTime(date)

        val dayWeek = calendar.get(Calendar.DAY_OF_WEEK) match {
          case 1 => "星期日"
          case 2 => "星期一"
          case 3 => "星期二"
          case 4 => "星期三"
          case 5 => "星期四"
          case 6 => "星期五"
          case 7 => "星期六"
        }
        //返回星期即可
        dayWeek
      }
    )

    //使用UDF函数，对数据进行处理
    val resultDF = hudiDF.select(
      to_week(col("departure_time")).as("week")
    )
      .groupBy("week")
      .count()
      .select(col("week"), col("count").as("total"))

    resultDF.printSchema()
    resultDF.show(10,truncate = false)

  }

  def main(args: Array[String]): Unit = {

    //构建sparksession实例对象
    val spark = SparkUtils.createSparkSession(this.getClass, partitions = 4)

    //加载hudi表的数据，指定字段
    val hudiDF = readFromHudi(spark, tablePath)

    //由于数据被多次使用，建议缓存
    hudiDF.persist(StorageLevel.MEMORY_AND_DISK)

    //按照业务指标进行统计分析
    //指标一：订单类型统计
    reportProduct(hudiDF)

    //指标二：订单时效统计
    reportType(hudiDF)

    //指标三：交通类型统计
    reportTraffic(hudiDF)

    //指标四：订单价格统计
    reportPrice(hudiDF)

    //指标五：订单距离统计
    reportDistance(hudiDF)

    //指标六：日期类型——星期，进行统计
    reportWeek(hudiDF)

    //当数据不再使用时，释放缓存
    hudiDF.unpersist()

    //关闭资源
    spark.stop()

  }

}
