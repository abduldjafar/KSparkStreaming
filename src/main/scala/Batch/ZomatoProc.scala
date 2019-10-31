package Batch

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DoubleType

object ZomatoProc {
  def main(args: Array[String]): Unit = {

    // rata - rata harga makanan per 2 org dari zomato yang ratingnya > 4.5
    // sumber data : https://www.kaggle.com/shrutimehta/zomato-restaurants-data#zomato.csv
    // hanya testing buat belajar

    val dataframe = SparkReader.Reader.csv("zomato.csv")
    dataframe.withColumnRenamed("Aggregate rating","agg_rating")
      .withColumnRenamed("Average Cost for two","agg_cost_two")
      .filter("agg_rating >= 4.5")
      .withColumn("agg_cost_two",new Column("agg_cost_two").cast(DoubleType))
      .select("Country Code","Currency","agg_cost_two").groupBy("Country Code").pivot("Currency")
      .avg("agg_cost_two").na.fill(0)
      .show()
  }
}
