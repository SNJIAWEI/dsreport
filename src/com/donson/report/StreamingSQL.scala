package com.donson.report

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingSQL {


  def main(args: Array[String]) {
    LoggerLevel.setStreamingLogLevels()
   /* if (args.length != 4) {
      System.err.println("Usage: StreamingSQL <dbUrl> <tableName> <user> <password> <filePath>")
      System.exit(1);
    }*/

//    val Array(dbUrl, dbUser, dbPassword, tableName) = args
//
    // 配置数据库连接参数
    val connectionProperties :Properties = new Properties()
    connectionProperties.setProperty("user","root")
    connectionProperties.setProperty("password","123")
    connectionProperties.setProperty("driver","com.mysql.jdbc.Driver")


//    val sparkConf = new SparkConf().setAppName("StreamingSQL").setMaster("local[4]")
//    val sc = new SparkContext(sparkConf)

//    val ssc = new StreamingContext(sc,Seconds(2))
//    val lines = ssc.textFileStream("file://")
    val Array(zkQuorum, group, topics, numThreads,outurl) = args
    val sparkConf = new SparkConf().setAppName("DonsonKafkaTest")setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(30)) /* duringtime  min */
    /*ssc.checkpoint("hdfs://master:9000/ck6")*/
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    /*val wordCounts = lines.flatMap(_.split(" "))*/
//    lines.foreachRDD(rdd=>rdd.foreach(println))
//    lines.print()/**/
    lines.filter(_.startsWith(",")).foreachRDD{ rdd=>
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      val schema = Utils.getSchema

      val rowRDD = Utils.getRowRDD(rdd)
      val rddDataFrame = sqlContext.createDataFrame(rowRDD, schema)
     val filename:String =fun
rddDataFrame.write.parquet("hdfs://192.168.1.220:9000/donson/streaming/people"+filename+".parquet")

      val parquetFile = sqlContext.read.parquet("hdfs://192.168.1.220:9000/donson/streaming/people"+filename+".parquet")
      parquetFile.registerTempTable("people")

      // 查询结果并存储到数据库中
      val results = sqlContext.sql("SELECT * FROM people")
      results.collect().foreach(println)
      results.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.129:3306/test", "donson1", connectionProperties)

    }

    ssc.start()
    ssc.awaitTermination()
  }




  def fun:String ={
    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val filename:String = dateFormat.format(new Date())
    filename
  }

}
