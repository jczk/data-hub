package org.wuwangfu.com.spark

import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME, QUERY_TYPE, QUERY_TYPE_INCREMENTAL_OPT_VAL}
import org.apache.hudi.QuickstartUtils.DataGenerator
import org.apache.spark.sql.{SaveMode, SparkSession}



/**
 * 下载源码命令
 *  mvn dependency:resolve -Dclassifier=sources
 *
 */
object HudiOperate {


  /**
   * 官方案例：模拟产生数据，插入hudi表，表的类型cow
   *
   * @param spark
   * @param tableName
   * @param tablePath
   */
  def insertData(spark: SparkSession, tableName: String, tablePath: String): Unit = {

    import spark.implicits._

    val tableName = "hudi_trips_cow"
    val basePath =  "hdfs://node01:9000/hudi/spark/"


    import org.apache.hudi.QuickstartUtils._
    //模拟乘车数据
    val dataGen = new DataGenerator()
    val inserts = convertToStringList(dataGen.generateInserts(100))

    import scala.collection.JavaConverters._
    val insertDF = spark.read.json(
      spark.sparkContext.parallelize(inserts.asScala, 2).toDS()
    )

//    insertDF.printSchema()
//    insertDF.show(10,truncate = false)

    //插入数据到hudi表
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._

    insertDF.write
      .format("hudi")
      . options(getQuickstartWriteConfigs)
      .option("hoodie.insert.shuffle.parallelism","2")
      .option("hoodie.upsert.shuffle.parallelism","2")
      //hudi表的属性值
      //预合并
      .option(PRECOMBINE_FIELD.key(), "ts")
      //主键
      .option(RECORDKEY_FIELD.key(), "uuid")
      //分区
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      //表名
      .option(TBL_NAME.key(),tableName)
      .mode(SaveMode.Append)
      .save(tablePath)
  }

  /**
   * 使用Snapshot query快照方式查询表数据
   *
   * @param spark
   * @param path
   */
  def queryData(spark: SparkSession, path: String): Unit = {

    import spark.implicits._

    val tripsDF = spark.read.format("hudi").load(path)

//    tripsDF.printSchema()
//    tripsDF.show(10,truncate = false)

    //查询费用大于20，小于50的乘车数据
    tripsDF
      .filter($"fare" >= 20 && $"fare" <= 50)
      .select($"driver",$"rider",$"fare",$"begin_lat",$"begin_lon",$"partitionpath",$"_hoodie_commit_time")
      .orderBy($"fare".desc,$"_hoodie_commit_time".desc)
      .show(20,truncate = false)

  }

  /**
   * 根据时间查询
   *
   * @param spark
   * @param path
   */
  def queryDataByTime(spark: SparkSession, path: String):Unit = {

    import org.apache.spark.sql.functions._

    //方式一：指定字符串，按照日期时间过滤获取数据
    val timeDF = spark.read
      .format("hudi")
      .option("as.of.instant", "20220405111356850")
      .load(path)
      .sort(col("_hoodie_commit_time").desc)

    timeDF.printSchema()
    timeDF.show(5,truncate = false)

    //方式二：指定字符串，按照年月日过滤获取数据
    val dataDF = spark.read
      .format("hudi")
      .option("as.of.instant", "2022-04-05 11:13:56.850")
      .load(path)
      .sort(col("_hoodie_commit_time").desc)

    dataDF.printSchema()
    dataDF.show(5,truncate = false)

  }

  def insertData(spark: SparkSession,  table:String, path: String,generator: DataGenerator):Unit={
    import spark.implicits._

    //模拟乘车数据
    import org.apache.hudi.QuickstartUtils._
    val inserts = convertToStringList(generator.generateInserts(100))

    import scala.collection.JavaConverters._
    val insertDF = spark.read.json(
      spark.sparkContext.parallelize(inserts.asScala, 2).toDS()
    )

    insertDF.printSchema()
    insertDF.show(10,truncate = false)

    //插入数据到hudi表
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._
    insertDF.write
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism","2")
      .option("hoodie.upsert.shuffle.parallelism","2")
      .option(PRECOMBINE_FIELD.key(),"ts")
      .option(RECORDKEY_FIELD.key(),"uuid")
      .option(PARTITIONPATH_FIELD.key(),"partitionpath")
      .option(TBL_NAME.key(),table)
      .mode(SaveMode.Overwrite)
      .save(path)
  }


  def updateData(spark: SparkSession, table: String, path: String, generator: DataGenerator): Unit = {

    import spark.implicits._

    //模拟乘车数据
    import org.apache.hudi.QuickstartUtils._
    val updates = convertToStringList(generator.generateUpdates(100))

    import scala.collection.JavaConverters._
    val updateDF = spark.read.json(
      spark.sparkContext.parallelize(updates.asScala, 2).toDS()
    )

    //插入数据到hudi表
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._
    updateDF.write
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism","2")
      .option("hoodie.upsert.shuffle.parallelism","2")
      .option(PRECOMBINE_FIELD.key(),"ts")
      .option(RECORDKEY_FIELD.key(),"uuid")
      .option(PARTITIONPATH_FIELD.key(),"partitionpath")
      .option(TBL_NAME.key(),table)
      .mode(SaveMode.Append)
      .save(path)
  }


  /**
   * 采用增量方式查询数据
   *
   * @param spark
   * @param path
   * @return
   */
  def incrementalQueryData(spark: SparkSession, path: String) = {

    import spark.implicits._

    //加载hudi表数据，获取commit time时间，作为增量查询数据阈值
    spark.read
      .format("hudi")
      .load(path)
      .createOrReplaceTempView("view_temp_hudi_trips")

    val commits = spark.sql(
      """
        |select
        |distinct(_hoodie_commit_time) as commit_time
        |from
        | view_temp_hudi_trips
        |order by
        | commit_time desc
        |""".stripMargin
    )
      .map(row => row.getString(0))
      .take(50)

    val beginTime = commits(commits.length - 1)
    println(s"begin_time = ${beginTime}")

    //设置hudi数据commit时间阈值，进行增量数据查询
    val tripsIncrementDF = spark.read
      .format("hudi")
      //设置查询数据模式为：increment，增量读取
      .option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL)
      //设置增量读取数据开始时间
      .option(BEGIN_INSTANTTIME.key(), beginTime)
      .load(path)

    //将增量查询数据注册为临时视图，查询费用大于20的数据
    tripsIncrementDF.createOrReplaceTempView("hudi_trips_incremental")
    spark.sql(
      """
        |select
        | `_hoodie_commit_time`,fare,begin_lon,begin_lat,ts
        |from
        | hudi_trips_incremental
        |where
        | fare > 20.0
        |""".stripMargin
    )
      .show(10,truncate = false)

  }

  /**
   * 删除数据，依据主键UUID，如果是分区表，指定分区路径
   *
   * @param spark
   * @param table
   * @param path
   */
  def deleteData(spark: SparkSession, table: String, path: String):Unit = {

    import spark.implicits._

    //加载hudi表数据，获取条目数
    val tripsDF = spark.read.format("hudi").load(path)
    println(s"Row Count = ${tripsDF.count()}")

    //模拟要删除的数据，从hudi中加载数据，获取几条数据，转换为要删除数据集合
    val dataFrame = tripsDF.limit(2).select($"uuid", $"partitionpath")
    import org.apache.hudi.QuickstartUtils._

    val generator = new DataGenerator()
    val deletes = generator.generateDeletes(dataFrame.collectAsList())

    import scala.collection.JavaConverters._
    val deleteDF = spark.read.json(spark.sparkContext.parallelize(deletes.asScala, 2).toDS())

    //保存数据到hudi表中，设置操作类型：DELETE
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._
    deleteDF.write
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism","2")
      .option("hoodie.upsert.shuffle.parallelism","2")
    //设置数据操作类型为delete，默认为upsert
      .option(OPERATION.key(),"delete")
      .option(PRECOMBINE_FIELD.key(),"ts")
      .option(RECORDKEY_FIELD.key(),"uuid")
      .option(PARTITIONPATH_FIELD.key(),"partitionpath")
      .option(TBL_NAME.key(),table)
      .mode(SaveMode.Append)
      .save(path)

    //再次加载hudi表数据，统计条目数，查看是否减少2条数据
    val hudiDF = spark.read.format("hudi").load(path)
    println(s"Delete After Count = ${hudiDF.count()}")

  }

  def main(args: Array[String]): Unit = {

    //创建sparksession实例对象，设置属性
    val spark =
      SparkSession.builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .master("local[2]")
        //设置序列化kyro
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    spark.sparkContext.setLogLevel("WARN")

    //定义变量：表名、保存路径
    val tableName = "tbl_trips_cow"
    val insertPath = "hdfs://node01:9000/hudi/spark/tbl_trips_cow"
    val queryPath = "hdfs://node01:9000/hudi/spark/hudi_trips_cow"

    //构建数据生成器，模拟产生业务数据
    import org.apache.hudi.QuickstartUtils._

    //任务一：模拟数据，插入hudi表，采用cow模式
//    insertData(spark,tableName,insertPath)

    //任务二：快照方式查询（Snapshot Query）数据，采用DSL方式
//    queryData(spark,insertPath)
//    queryDataByTime(spark,insertPath)

//    任务三：更新数据
//    val dataGen = new DataGenerator()
//    insertData(spark,tableName,insertPath,dataGen)
//    updateData(spark,tableName,insertPath,dataGen)

//    任务四：采用增量方式查询数据，采用SQL方式
//    incrementalQueryData(spark,insertPath)

    //任务五：删除数据
    deleteData(spark,tableName,insertPath)


    //关闭资源
    spark.stop()
  }

}
