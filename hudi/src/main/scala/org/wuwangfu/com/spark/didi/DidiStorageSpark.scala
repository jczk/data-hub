package org.wuwangfu.com.spark.didi

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 滴滴海口市出行运营数据分析，使用sparkSQL操作数据，先读取CSV文件，保存至hudi表
 *
 * 1、构建sparksession实例对象
 * 2、加载本地CSV文件格式滴滴出行数据
 * 3、滴滴出行数据etl处理
 * 4、保存转换后数据至hudi表
 * 5、应用结束关闭资源
 *
 */
object DidiStorageSpark {

  //数据路径
  val dataPath = "file:///F:/workspace/data-hub/data/bus_shenzhen_2920003803116.csv"
  //hudi表中的属性
  val tableName = "tbl_bus_shenzhen"
  val tablePath = "/hudi/spark/tbl_bus_shenzhen"

  /**
   * 读取Csv数据，封装到DataFrame
   *
   * @param spark
   * @param path
   */
  def readCsvFile(spark: SparkSession, path: String):DataFrame = {

    spark.read
    //设置分割符
      .option("sep",",")
    //文件首行为列名称
      .option("header","true")
    //依据数值自动推断数据类型
      .option("inferSchema","true")
    //指定文件路径
      .csv(path)

  }

  /**
   * 对数据进行ETL转换操作，指定ts和partitionpath列
   *
   * @param busDF
   * @return
   */
  def process(busDF: DataFrame):DataFrame = {

    busDF
      //添加字段，指定hudi表分区字段，三级分区-》yyyy-MM-dd
      .withColumn("partitionpath",
        concat_ws("-",col("year"),col("month"),col("day")))
      //删除列
      .drop("year","month","day")
      //添加timestamp列，作为hudi表记录数据合并时字段，使用发车时间
      .withColumn("ts",
        unix_timestamp(col("departure_time"),"yyyy-MM-dd HH:mm:ss"))

  }

  /**
   *将数据集写入hudi表，表的类型为Cow，属于批量保存数据，写少读多
   *
   * @param etlDF
   * @param tableName
   * @param tablePath
   * @return
   */
  def saveToHudi(dataframe: DataFrame, tableName: String, tablePath: String) = {

    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._

    //保存数据
    dataframe.write
      .mode(SaveMode.Overwrite)
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism","2")
      .option("hoodie.upsert.shuffle.parallelism","2")
      .option(RECORDKEY_FIELD.key(),"order_id")
      .option(PRECOMBINE_FIELD.key(),"ts")
      .option(PARTITIONPATH_FIELD.key(),"partitionpath")
      .option(TBL_NAME.key(),tableName)
      .save(tablePath)
  }

  def main(args: Array[String]): Unit = {
    //1、构建sparksession实例对象
    val spark = SparkUtils.createSparkSession(this.getClass)

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    spark.sparkContext.setLogLevel("WARN")

    //2、加载本地CSV文件格式滴滴出行数据
    val busDF = readCsvFile(spark, dataPath)
    //打印测试
    busDF.printSchema()
    busDF.show(10,truncate = false)

    //3、滴滴出行数据etl处理
    val etlDF = process(busDF)

    //4、保存转换后数据至hudi表
    saveToHudi(etlDF,tableName,tablePath)

    //5、应用结束关闭资源
    spark.stop()

  }

}
