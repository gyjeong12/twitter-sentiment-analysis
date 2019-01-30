package com.twitter.stream.spark1

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import shapeless.ops.nat.GT.>

import scala.io.Codec
import scala.io.Source
import java.nio.charset.CodingErrorAction
//import javax.xml.transform.Source
import java.util.Properties
import org.apache.spark.sql.streaming.Trigger

import org.apache.spark.streaming._

import scala.collection.mutable

import org.apache.spark.sql.functions.udf

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object TwitterAnalysis {

  val posWords= scala.io.Source.fromFile("D:\\SparkScala\\TwitterAnalysis\\data\\pos-words.txt").getLines()
  val negWords = scala.io.Source.fromFile("D:\\SparkScala\\TwitterAnalysis\\data\\neg-words.txt").getLines()
  val stopWords = scala.io.Source.fromFile("D:\\SparkScala\\TwitterAnalysis\\data\\stop-words.txt").getLines()

  val posWordsArr = mutable.MutableList("")
  val negateWordsArr = mutable.MutableList("")

  for (posWord <- posWords)
    posWordsArr += (posWord)

  for (negWord <- negWords) {
    negateWordsArr += (negWord)
  }

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
      .option("subscribe", "seahawksTopic")
      .option("startingOffsets", "earliest")
      .load()
      //.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS STRING)")
      .selectExpr("CAST(value AS STRING)")

    //lazy val sentiments = df.map((e: String)=> findTweetSentiment(e.toLowerCase()))

    //df.printSchema()

    df.writeStream
      .queryName("TEST_KAFKA")
      .outputMode("append")
      .format("memory")
      .start()

    Thread.sleep(3000)
    println("Retrieving tweets from memory")
    val dfTweets = spark.sql("select * from TEST_KAFKA")
    println("Printing tweets")
    //dfTweets.p
    dfTweets.printSchema()
    println(dfTweets.count())
    val dfTemp = dfTweets.map(tweet => tweet.getString(0))
    //lazy val listSentiment = dfTemp.map(tweet => findTweetSentiment(tweet))
    //listSentiment.foreach(result => println("The result is " + result))
    //dfSentiment.foreach(_=>println())
    //sentiment.foreach(_=>println())
    // Create MySQL Properties Object
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "pass")
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    Thread.sleep(3000)

    /*
     Creating an UDF

      If you can't complete your task with the built-in functions, you may consider defining an UDF
      (User Defined Function). They are useful when you can process each item of a column independently and
      you expect to produce a new column with the same number of rows as the original one (not an aggregated column).
      This approach is quite simple: first, you define a simple function, then you register it as an UDF,
      then you use it. Example:

      def myFunc: (String => String) = { s => s.toLowerCase }

      import org.apache.spark.sql.functions.udf
      val myUDF = udf(myFunc)

      val newDF = df.withColumn("newCol", myUDF(df("oldCol")))
    */

    // Define a regular Scala function
    val funcTweet: (String => String) = findTweetSentiment(_)

    val textUDF = udf(funcTweet)
    //val finalDF = dfTemp.withColumn("sentiment", textUDF(dfTemp.col("value")))
    val finalDF = dfTemp.withColumn("sentiment", textUDF(dfTemp("value")))
    finalDF.show()

    //dfSentiment.foreach(_=>println())

    //val seq = Seq((dfTemp, listSentiment.toDF("sentiment")))
    //dfTemp.printSchema()
    //listSentiment.toDF("sentiment").printSchema()
    //seq.foreach(_=>println())
    //val newDF = seq.toDF("Tweet","Sentiment")
    //newDF.show()

   // val sentimentCol = listSentiment.toDF("sentiment")
    // val dfFinal = dfTemp.withColumn("sentiment", sentimentCol)

    //dfFinal.foreach(_=>println())*/


    var i = 0

    while (i < 5) {
      Thread.sleep(5000)
      println("Saving to table!")
      val df2 = spark.sql("select * from TEST_KAFKA")
      finalDF.show()
      finalDF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/mysql", "SEAHAWKS", prop)
      i += 1
    }

  }

    def findTweetSentiment(tweet: String): String = {
      var count = 0
      for (w <- tweet.split(" ")) {
        for (positiveW <- posWordsArr) {
          if (w != "" && positiveW.toString.toLowerCase() == w) {
            count = count + 1
          }
        }

        for (negativeW <- negateWordsArr) {
          if (w != "" && negativeW.toString.toLowerCase() == w) {
            count = count - 1
          }
        }
      }
      if (count > 0) {
        return "positivie"
      }
      else if (count < 0) {
        return "negative"
      }
      else
        return "neutral"
    }
}
