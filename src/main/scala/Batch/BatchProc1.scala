package Batch
import Jdbc.Jdbc.{SavetoDB, Writedb}
import org.apache.spark.sql.SparkSession.builder


object BatchProc1{
  def main(args: Array[String]): Unit = {
    val spark = builder.master("local").appName("Batch Proces").getOrCreate
    val dfraw = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("USvideos.csv")

    val writedb = Writedb(dfraw)
    SavetoDB(writedb,"usvideos")

    dfraw.show(100)


  }
}
