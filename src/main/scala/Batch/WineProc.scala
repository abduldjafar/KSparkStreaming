package Batch
import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType


object WineProc {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def regexp_extractAll: UserDefinedFunction = udf(f = (job: String, exp: String, groupIdx: Int) => {
    val pattern = Pattern.compile(exp)
    val m = pattern.matcher(job)
    var result = Seq[String]()
    while (m.find) {
      val temp: Unit =
        result = result :+ m.group(groupIdx)
    }
    result.toArray
  })
  def main(args: Array[String]): Unit = {

    val dataframe = SparkReader.Reader.csv("wine.csv")
    println("===================================Data Raw=================================================================")
    dataframe.show()

    println("top 5 average price from best winery")
    val df = dataframe.filter("points >= 90").select("price","winery","variety","points")
      .withColumn("price",new Column("price").cast(DecimalType(4,2)))

    df.groupBy("winery").avg("price")
      .orderBy(desc("avg(price)"))
      .join(df.groupBy("winery").sum("points"),"winery")
      .orderBy(desc("sum(points)"))
      .show(5)

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

    println("Top 5 variety for base wine")
    dataframe.select("variety").groupBy("variety")
      .count().orderBy(desc("count"))
      .show(5)

  }
}
