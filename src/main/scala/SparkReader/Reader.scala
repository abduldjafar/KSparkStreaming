package SparkReader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession.builder

object Reader {
  val spark = builder.master("local").appName("Batch Proces").getOrCreate

  def csv(filename: String): DataFrame = {
    spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("quote","\"")
      .option("escape","\"")
      .load(filename)
  }
  def json(path: String): DataFrame ={
    spark.read.json(path)
  }
}
