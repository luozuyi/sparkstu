package com.ido

import org.apache.spark.sql.SparkSession

object StructuredStreamingKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("stream")
      .master("local[2]")
      .getOrCreate()
    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.2.180:9092,192.168.2.181:9092")
      .option("subscribe", "testflume")
      .load()
    import spark.implicits._
    val lines = df.selectExpr("CAST(value AS STRING)")
      .as[(String)]
    val words = lines.flatMap(_.split(" "))
    val wordcounts =words.groupBy("value").count()

    val query = wordcounts.writeStream
                .outputMode("complete")
                .format("console")
                .start()
    query.awaitTermination()

  }
}
