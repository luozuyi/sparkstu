package com.ido

import org.apache.spark.sql.SparkSession

object Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("user.name", "root")
    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[2]")
      //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://192.168.2.180:3306/hive?createDatabaseIfNotExist=true")
      .config("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver")
      .config("javax.jdo.option.ConnectionUserName", "root")
      .config("javax.jdo.option.ConnectionPassword", "CB@951357tech")
      .enableHiveSupport()
      .getOrCreate()

      spark.sql("show databases").show()
      spark.sql("select * from userdb.t_user").show()
  }
}
