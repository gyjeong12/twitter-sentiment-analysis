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

    package com.twitter.stream.spark1
    //import javax.xml.transform.Source
    import java.text.SimpleDateFormat
    import java.util.Date

    import org.apache.spark.sql.SaveMode
    import org.json.JSONObject
    import org.jsoup.Jsoup
    //import javax.xml.transform.Source
    import java.util.Properties

    import org.apache.log4j.{Level, Logger}
    import org.apache.spark.sql.SparkSession

    import scala.collection.mutable

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

        val df = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "seahawksKafkaTopic")
          .option("startingOffsets", "earliest")
          .load()
          //.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS STRING)")
          .selectExpr("CAST(value AS STRING)")

        //lazy val sentiments = df.map((e: String)=> findTweetSentiment(e.toLowerCase()))

        df.printSchema()

        val query = df.writeStream
          .queryName("TEST_KAFKA")
          .outputMode("append")
          .format("memory")
          .start()

        Thread.sleep(3000)
        println("Retrieving tweets from memory")
        val dfTweets = spark.sql("select * from TEST_KAFKA")
        println("Printing tweets")
        dfTweets.show()
        dfTweets.printSchema()
        println(dfTweets.count())

  // New Add
        val dfJSON = dfTweets.select("value").rdd.map {
          case Row(string_val: String) => (string_val, getJsonObject(string_val))
        }.toDF("value", "json")

        val dfTweetInfo = dfJSON.select("json").rdd.map {
          case Row(json: JSONObject) => (json.getDateFromMilliseoond(tweet.getLong("createdAt"))), json.findTweetSentiment(tweet.getString("text")), json.getLocation(tweet.getJSONObject("user")))
        }.toDF("createdAt", "text", "location")


        //Collect raw json strings into a List[String]
        //Drop null values
        /*
        val dfNoNan = dfTweets.na.drop()
        dfNoNan.show(20)
        val rawStrings = dfNoNan.select("value").collect().map(_.getString(0)).toList
        */

        //dfTweets.na.drop()

        /*
        rawStrings.toList.foreach(_=>println())
        //Map that List[String] to a List[JSONObject]
        val jsonObjects = rawStrings.map(tweet=> getJsonObject(tweet))
        jsonObjects.foreach(_=>println())
        */

        //Create a map for each value you want from each key...
        //val createdAt = jsonObjects.map(tweet => tweet.getString("accessLevel"))
        //createdAt.toDF.show()

        //You have to use the right type method or else it will return an error. getString for strings, getDouble for doubles... et cetera
        /*
        val datetime = jsonObjects.map(tweet => getDateFromMilliseoond(tweet.getLong("createdAt")))
        val text = jsonObjects.map(tweet => tweet.getString("text"))
        val sentiment = jsonObjects.map(tweet => findTweetSentiment(tweet.getString("text")))
        val lang = jsonObjects.map(tweet => tweet.getString("lang"))
        val sourcedata = jsonObjects.map(tweet => getSourceFromHTML(tweet.getString("source")))
        val location = jsonObjects.map(tweet => getLocation(tweet.getJSONObject("user")))
        val retweet = jsonObjects.map(tweet => tweet.getInt("retweetCount"))
        */

        //val idStr = jsonObjects.map(tweet => tweet.getString("id_str"))

        //Turn these lists to Dataframes and grab only the Col
        //val createdAtCol = createdAt.toDF("createdAt").col("createdAt")

        import spark.implicits._
        println("********LOADING TEXT*********")
        /*
        text.toDF.show()
        val datetimeDF = datetime.toDF("createdAt")
        val textDF = text.toDF("text")//.col("text")
        val langDF = lang.toDF("lang")//.col("lang")
        val sentimentDF = sentiment.toDF("sentiment")
        val sourceDF = sourcedata.toDF("source")
        val locationDF = location.toDF("location")
        val retweetDF = retweet.toDF("retweetCount")
        */
        //... more columns
        //val idStrCol = idStr.toDF("idStr").col("idStr")

        //Finally make a new dataframe by taking an existing one, adding new columns for each Col we create separately
        //And then drop the original column(s) found in dfTweets...

        query.awaitTermination(2000)

        //Figure out a way to use sql to resolve this... I will also do my investigations. We'll meet tomorrow to resolve this, but we're almost there.
        //val dfTweetInfo = textDF.join(langDF, "full_outer").join(datetimeDF, "full_outer").join(sourceDF, "full_outer").join(locationDF, "full_outer").join(retweetDF, "full_outer").join(sentimentDF, "full_outer")

        dfTweetInfo.show()
        /*val dfTweetInfo= dfTweets
          .withColumn("text", textCol)
          .withColumn("lang", langCol)
          //.withColumn("idStr", idStrCol)
          .drop("value")*/

        //dfTweetInfo.show(20)
        /*
        val data = """{"one":"1000"}"""
        val data = """{"body":{"method":"string", "events":"string", "clients":"string", "parameter":"string", "channel":"string",
                       "metadata":{
                            "meta1":"string",
                            "meta2":"string",
                            "meta3":"string"
                        } },
                       "timestamp":"string"}"""

        val mapper = new ObjectMapper
        mapper.registerModule(DefaultScalaModule)
        //val df1 = dfTweets.select("value").map(r => r.getString(0)).collect.toString()
        val df1 = dfTweets.map(r => r.getString(0)).toJSON
        val jsonStr = df1.toString()
        //val jsonStr = df1.toJSON.toString()
        println(jsonStr)

        //println(df1)
        //val map = mapper.readValue(data, classOf[Map[String,String]])
        val map = mapper.readValue(jsonStr, classOf[Map[String,String]])
        println("Map test : "+map)
        //println("Map Value :"+map("one"))

        //val dfTemp = dfTweets.map(tweet => jsonStrToMap(tweet.getString(0)).get("rateLimitStatus").toString)
       // val dfTemp = dfTweets.map(tweet => tweet.getString(0))
        //val jsonString = """{"key_value":{"1":"1000", "2":"2000"}}"""
        */

        //lazy val listSentiment = dfTemp.map(tweet => findTweetSentiment(tweet))
        //listSentiment.foreach(result => println("The result is " + result))
        //dfSentiment.foreach(_=>println())
        //sentiment.foreach(_=>println())
        // Create MySQL Properties Object
        //dfTemp.show(20)
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
        /*val funcTweet: (String => String) = findTweetSentiment(_)

        val textUDF = udf(funcTweet)
        val finalDF = dfTweetInfo.withColumn("sentiment", textUDF(dfTweetInfo.col("text")))
        finalDF.show()*/

        var i = 0

        while (i < 5) {
          Thread.sleep(5000)
          println("Saving to table!")
          val df2 = spark.sql("select * from TEST_KAFKA")
          dfTweetInfo.show()
          println(dfTweetInfo.count())
          dfTweetInfo.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/mysql", "TWITTER", prop)
          println("Table saved")
          i += 1
        }

      }

      def getJsonObject(rawJson: String): JSONObject = {
        val jsonObject = new JSONObject(rawJson)
        println(jsonObject.get("text"))
        return jsonObject
      }

      def getDateFromMilliseoond(milliseconds : Long) : String = {
        /*val instant = Instant.ofEpochMilli(milliseconds)
        val zonedDateTimeUtc = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"))
        return zonedDateTimeUtc.toString*/
        val date = new Date(milliseconds)
        val sdf = new SimpleDateFormat("HH:mm:ss")
        val result = sdf.format(date)
        return result
      }

      def getSourceFromHTML(rawTag: String): String = {
        val source = Jsoup.parse(rawTag)
        val link = source.select("a").first()
        val result =  link.text()
        return result
      }

      def getLocation(user: JSONObject) : String = {
        val location = user.get("location")
        return location.toString
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

      /*
          def jsonStrToMap(jsonStr: String): Map[String, Any] = {
            implicit val formats = org.json4s.DefaultFormats

            println(jsonStr)

            /*val result = JSON.parseFull(jsonStr)

            val resultMap = result match {
              case Some(map: Map[String, Any]) => println(map)
              case None => println("Parsing failed!")
              case other => println("I don't know what happened!")
            }

            return resultMap.asInstanceOf[Map[String, Any]]*/

            val parseResult = parse(jsonStr)
            val parseMap = parseResult.right.map(_.toString())
            //return parseMap
          }

      */

    }


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
