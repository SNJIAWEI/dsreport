package com.donson.report

import java.util.Properties

import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingSQL {

  def main(args: Array[String]) {
    LoggerLevel.setStreamingLogLevels(Level.WARN)
    if (args.length != 4) {
      System.err.println("Usage: StreamingSQL <dbUrl> <tableName> <user> <password> <filePath>")
      System.exit(1);
    }

    //    val Array(dbUrl, dbUser, dbPassword, tableName) = args
    //
    // 配置数据库连接参数
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "root")
    connectionProperties.setProperty("password", "123")
    connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    val Array(zkQuorum, group, topics, numThreads, outurl) = args
    val sparkConf = new SparkConf().setAppName("StreamingSQL").setMaster ("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(30)) /* duringtime  min */
    /*ssc.checkpoint("hdfs://master:9000/ck6")*/
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    lines.foreachRDD { rdd =>
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      val schema = Utils.getSchemaInfo

      val rowRDD = Utils.getRowRDD(rdd)
      val rddDataFrame = sqlContext.createDataFrame(rowRDD, schema)
      rddDataFrame.write.parquet("hdfs://192.168.1.220:9000/donson/streaming/people" + Utils.formateFileName + ".parquet")

      val parquetFile = sqlContext.read.parquet("hdfs://192.168.1.220:9000/donson/streaming/people" + Utils.formateFileName + ".parquet")
      parquetFile.registerTempTable("adloginfo")

      // 查询结果并存储到数据库中
      val results = sqlContext.sql("SELECT AdvertisersID,ADOrderID,ADCreativeID,ReqDate,ReqHour,sum(IsShow),sum(IsClick),sum(IsTakeBid),sum(IsSuccessBid) " +
        "FROM adloginfo " +
        "group by AdvertisersID,ADOrderID,ADCreativeID,ReqDate,ReqHour")
      // results.collect().foreach(println)
      results.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.129:3306/test", "donson1", connectionProperties)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
