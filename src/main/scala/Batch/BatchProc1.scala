import org.apache.spark.sql.SparkSession.builder
import org.apache.spark.sql.functions._

object BatchProc1{
  def main(args: Array[String]): Unit = {
    val spark = builder.master("local").appName("Batch Proces").getOrCreate
    val dfraw = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("USvideos.csv")

    // get most views
    val mostviews = dfraw.filter("views > 100000")
      .sort(desc("views"))
      mostviews.printSchema()
      
  }
}
