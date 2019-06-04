package Streaming

import org.apache.spark.sql.SparkSession.builder
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.streaming.OutputMode

object TweetStream{
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
