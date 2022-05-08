package org.wuwangfu.com.spark.stream

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.wuwangfu.com.spark.didi.SparkUtils

/**
 * 基于StructuredStreaming结构化流实时从kafka消费数据，经过ETL转换后，存储至hudi表
 *
 */
object DidiStructureSpark {

  /**
   * 指定kafka topic名称，实时消费数据
   *
   * @param spark
   * @param topicName
   * @return
   */
  def readFromKafka(spark: SparkSession, topicName: String):DataFrame = {

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node03:9092")
      .option("subscribe", topicName)
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", 100000)
      .option("failOnDataLoss", "false")
      .load()
    //      .selectExpr("CAST (value AS STRING)")

  }


  /**
   * 对kafka获取数据，进行转换操作，获取所有字段的值，转换为String，以便保存至hudi表中
   *
   * @param streamDF
   * @return
   */
  def process(streamDF: DataFrame):DataFrame = {

    streamDF
      //选择字段
      .selectExpr(
        "CAST(key as STRING) order_id",
        "CAST(value as STRING) as message",
        "topic","partition","offset","timestamp"
      )
    //解析message数据，提取字段值
      .withColumn("user_id",get_json_object(col("message"),"$.userId"))
      .withColumn("order_time",get_json_object(col("message"),"$.orderTime"))
      .withColumn("ip",get_json_object(col("message"),"$.ip"))
      .withColumn("order_money",get_json_object(col("message"),"$.orderMoney"))
      .withColumn("order_status",get_json_object(col("message"),"$.orderStatus"))
    //删除message字段
      .drop(col("message"))
    //转换订单日期时间格式为long类型，作为hudi表中合并数据字段
      .withColumn("ts",to_timestamp(col("order_time"),"yyyy-MM-dd HH:mm:ss.SSS"))
    //订单日期时间提取分区日志：yyyyMMdd
      .withColumn("day",substring(col("order_time"),0,10))

  }
  /**
   * 将流式数据DataFrame保存到hudi表中
   *
   * @param streamDF
   */
  def saveToHudi(streamDF: DataFrame):Unit = {
    streamDF.writeStream
      .outputMode(OutputMode.Append())
      .queryName("query-hudi-streaming")
      .foreachBatch((batchDF:Dataset[Row],batchId:Long)=>{
        println(s"--------- BatchId:${batchId} start---------")
        import org.apache.hudi.DataSourceWriteOptions._
        import org.apache.hudi.config.HoodieWriteConfig._
        import org.apache.hudi.keygen.constant.KeyGeneratorOptions._

        batchDF.write
          .mode(SaveMode.Append)
          .format("hudi")
          .option("hoodie.insert.shuffle.parallelism","2")
          .option("hoodie.upsert.shuffle.parallelism","2")
          //hudi表的属性值设置
          .option(RECORDKEY_FIELD.key(),"order_id")
          .option(PRECOMBINE_FIELD.key(),"ts")
          .option(PARTITIONPATH_FIELD.key(),"day")
          .option(TBL_NAME.key(),"tbl_hudi_order")
          .option(TABLE_TYPE.key(),"MERGE_ON_READ")
          //分区值对应目录格式，与hive分区策略一致
          .option(HIVE_STYLE_PARTITIONING_ENABLE.key(),"true")
          .save("/hudi/spark/tbl_hudi_order")

      })
      .option("checkpointLocation","/tmp/checkpoint/")
      .start()
  }

  def structureKafkaConsole(spark: SparkSession, topicName: String) = {

    import spark.implicits._

    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node03:9092")
      .option("subscribe", topicName)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("maxOffsetsPerTrigger", "100000")
      .option("failOnDataLoss","false")
      .load()

    val filterStream = kafkaStreamDF
      // 获取value字段的值，转换为String类型
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      // 转换为Dataset类型
      .as[(String,String)]
      .filter(line => null != line && line._1.trim.length > 0)

    val query = filterStream.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("numRows", "10")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
    query.stop()
  }


  def main(args: Array[String]): Unit = {

    //构建Sparksession实例对象
    val spark = SparkUtils.createSparkSession(this.getClass)

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    spark.sparkContext.setLogLevel("ERROR")

    //从kafka实时消费数据
//    val kafkaStreamDF = readFromKafka(spark, "orders")

    structureKafkaConsole(spark,"orders")

    //提取数据，转换数据类型
//    val streamDF = process(kafkaStreamDF)

    //保存数据至hudi表中，MOR类型表，读取表数据合并文件
//    saveToHudi(streamDF)

    //流式应用启动后，等待终止
    spark.streams.active.foreach(query => println(s"${query.name} is Running ......"))
    spark.streams.awaitAnyTermination()

  }

}
