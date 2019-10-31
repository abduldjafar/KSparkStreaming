package Batch
import Jdbc.Jdbc._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
object ReadJson {

  def main(args: Array[String]): Unit = {
    val peopleDF = SparkReader.Reader.json("")
    val dffilterone = peopleDF.select("{country_subdivision}","{revenue}","{event_name}","{last_session_time}")
      .withColumnRenamed("{country_subdivision}","provinsi")
      .withColumnRenamed("{revenue}","revenue")
      .withColumnRenamed("{event_name}","event_name")
      .withColumnRenamed("{last_session_time}","last_session_time")
      .filter("revenue is not null").filter("provinsi is not null")
      .filter("last_session_time is not null").filter("event_name is not null")
      .withColumn("last_session_time",from_unixtime(new Column("last_session_time")))
      .withColumn("year",year(new Column("last_session_time")))
      .withColumn("month",month(new Column("last_session_time")))
      .groupBy("provinsi","year").pivot("month").sum("revenue")
      .na.fill(0)

    val writedb = Writedb(dffilterone)
    SavetoDB(writedb,"test")


  }
}
