import java.util.regex.Pattern

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.{Column, DataFrame, Row}


object TweetStream{
  // function for regex findall
  def regexp_extractAll = udf((job: String, exp: String, groupIdx: Int) => {
    val pattern = Pattern.compile(exp.toString)
    val m = pattern.matcher(job.toString)
    var result = Seq[String]()
    while (m.find) {
      val temp =
        result =result:+m.group(groupIdx)
    }
    result.toArray
  })

  // function for check string
  def parseToInt = udf((entry: String) => {
    try {
      entry.toInt
    } catch {
      case e: Exception => 0
    }
  })

  def cleansingTweet = udf((entry: String) => {
    // Remove HTML special entities (e.g. &amp;)
    val regexhtml ="\\&\\w*;".r
    val result1 = regexhtml.replaceAllIn(entry,"")

    //remove @username
    val regexAd = "@[^\\s]+".r
    val result2 = regexAd.replaceAllIn(result1,"")

    //remove RT
    val regexRT = "rt".r
    val result3 = regexRT.replaceAllIn(result2,"")

    //remove tickers
    val regextTicks = "\\$\\w*".r
    val result4 = regextTicks.replaceAllIn(result3,"")

    //convert to lower
    val result5 = result4.toLowerCase()

    //remove hyperlinks
    val regexHyper = "https?:\\/\\/.*\\/\\w*".r
    val result6 = regexHyper.replaceAllIn(result5,"")

    //remove hashtag
    val regexHastag = "#\\w*".r
    val result7 = regexHastag.replaceAllIn(result6,"")

    result7.trim()

  }
  )

  def main(args: Array[String]): Unit = {
    val spark = builder.master("local").appName("Spark Sentyment Analysyst").getOrCreate

    val jdbcHostname = "localhost"
    val jdbcPort = 3306
    var urlp="jdbc:postgresql://localhost:5432/asek"
    val user ="root"
    val pwd = "xxxxxxxx"
    val userp ="asek"
    val pwdp = "joss"

    val mysql = "com.mysql.jdbc.Driver"
    val postgres = "org.postgresql.Driver"
    val writerpost = new JDBCSink(urlp,userp, pwdp,postgres)

    val df_raw =  spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "tweet")
      .load()


    val df =
      df_raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val data =
      df.distinct.select("value")
        .groupBy("value").count()
        .withColumnRenamed("count","total")
        .orderBy(desc("total"))

    val query = data.writeStream.foreach(writerpost).outputMode(OutputMode.Complete()).start()

    query.awaitTermination()
  }
}