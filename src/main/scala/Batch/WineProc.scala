package Batch
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.functions._


object WineProc {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val dataframe = SparkReader.Reader.csv("wine.csv")

    println("===================================Data Raw=================================================================")
    dataframe.show()

    println("average price from best winery")
    dataframe.filter("points >= 90").select("price","winery","variety")
      .withColumn("price",new Column("price").cast(DecimalType(4,2)))
      .groupBy("winery").pivot("variety").avg("price").na.fill(0)
      .show()

    println("Top 5 Provience for wine production with average price")
    dataframe.filter("points >= 90").select("province","points","price")
      .withColumn("points",new Column("points").cast(DecimalType(4,2)))
      .withColumn("price",new Column("price").cast(DecimalType(4,2)))
      .groupBy("province").avg("points","price")
      .orderBy(desc("avg(points)"))
      .show(5)

    println("Top 10 country for wine production with average price")
    dataframe.select("country","points","price")
      .withColumn("points",new Column("points").cast(DecimalType(4,2)))
      .withColumn("price",new Column("price").cast(DecimalType(4,2)))
      .groupBy("country").avg("points")
      .orderBy(desc("avg(points)"))
      .show(10)
  }
}
