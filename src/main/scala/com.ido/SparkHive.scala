package com.ido

import org.apache.spark.sql.SparkSession

object SparkHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkHive")
      .master("local[2]")
      .getOrCreate()
    val ds = spark
      .read
      .textFile("D:/sud.txt")

    import spark.implicits._
    val lines = ds.flatMap(_.split(" ")).map((_,1)).show()
    case class Book(bookName:String,bookCount:Int)
  }
}
