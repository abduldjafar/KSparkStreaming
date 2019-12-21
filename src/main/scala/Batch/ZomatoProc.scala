package Batch
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DecimalType

object ZomatoProc {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val dataframe = SparkReader.Reader.csv("zomato.csv")
    dataframe.show()
    // price average for 2 peoples from zomato with ratings > 4.5
    // sources : https://www.kaggle.com/shrutimehta/zomato-restaurants-data#zomato.csv


    dataframe.withColumnRenamed("Aggregate rating","agg_rating")
      .withColumnRenamed("Average Cost for two","agg_cost_two")
      .filter("agg_rating >= 4.5")
      .withColumn("agg_cost_two",new Column("agg_cost_two").cast(DecimalType(4,2)))
      .select("Country Code","Currency","agg_cost_two").groupBy("Country Code").pivot("Currency")
      .avg("agg_cost_two").na.fill(0)
      .show()

  }
}
