package SparkReader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession.builder

object Reader {
  val spark = builder.master("local").appName("Batch Proces").getOrCreate

  def csv(filename: String): DataFrame = {
    spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(filename)
      .na.drop()
  }

  def json(filename: String): DataFrame ={
    spark.read.format("csv").option("inferSchema", "true").option("header","true").load(filename)
  }
}
