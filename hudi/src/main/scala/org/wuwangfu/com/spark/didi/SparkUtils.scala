package org.wuwangfu.com.spark.didi

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * sparkSQL操作数据工具类
 *
 */
object SparkUtils {

  /**
   * 构建sparkSession实例对象时，默认情况下本地模式运行
   * @param clazz
   * @param master
   * @param partitions
   * @return
   */
  def createSparkSession(clazz:Class[_],master:String="local[2]",partitions:Int=2):SparkSession={

    SparkSession.builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .master(master)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions",partitions)
      .getOrCreate()
  }

  def printToConsole(dataFrame: DataFrame) = {

    dataFrame.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
//      .format("console")//往控制台写
//      .outputMode("complete")//每次将所有的数据写出
//      .trigger(Trigger.ProcessingTime(0))//触发时间间隔,0表示尽可能的快
//      .option("checkpointLocation","./ckp")//设置checkpoint目录,socket不支持数据恢复,所以第二次启动会报错,需要注掉
      .start()//开启
      .awaitTermination()//等待停止
  }


  def main(args: Array[String]): Unit = {

    val spark = createSparkSession(this.getClass)
    println(spark)

    Thread.sleep(50000)

    spark.stop()
  }

}
