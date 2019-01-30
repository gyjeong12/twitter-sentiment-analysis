package com.twitter.stream.spark

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import javax.xml.transform.Source
import java.util.Properties
import org.apache.spark.sql.streaming.Trigger

import org.apache.spark.streaming._

object App {

  def main(args: Array[String]) {
    println("Hello World!")

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //    Class.forName("com.mysql.cj.jdbc.Driver");
    //
    //    val prop = new Properties()
    //    prop.setProperty("user", "root")
    //    prop.setProperty("password", "pass")
    //    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder()
      .appName("App")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testKafka")
      .option("startingOffsets", "earliest")
      .load()
      //.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS STRING)")
      .selectExpr("CAST(value AS STRING)")

    df.printSchema()

    df.writeStream
      .queryName("TEST_KAFKA")
      .outputMode("append")
      .format("memory")
      .start()

    // Create MySQL Properties Object
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "pass")
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")


    var i = 0

    while (i < 5) {
      Thread.sleep(5000)
      println("Saving to table!")
      val df2 = spark.sql("select * from TEST_KAFKA")
      df2.show()
      df2.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/mysql", "TEST_KAFKA", prop)
      i += 1
    }

  }

}