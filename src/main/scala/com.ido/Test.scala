package com.ido

import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    println("xxxxxxx")
    var spark = SparkSession
      .builder()
      .appName("sss")
      .master("local[2]")
      .getOrCreate()
    var file = "D:/jetshop-err.txt"
    var rs =spark.sparkContext
      .textFile(file)
      .flatMap(x => x.split(" "))
      .map(x => (x,1))
      .reduceByKey((x,y) => x+y)
      .collect().toList
    for (elem <- rs) {
      println(elem)
    }
  }
}
