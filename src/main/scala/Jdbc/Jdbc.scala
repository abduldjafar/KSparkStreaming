package Jdbc

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

object Jdbc {
  def Writedb(dataFrame: DataFrame): DataFrameWriter[Row] = {
    try{
      dataFrame.write
        .mode("overwrite")
    }catch {
      case unknown:Exception => {
        dataFrame.write
      }
    }
  }

  def SavetoDB(dataFrameWriter: DataFrameWriter[Row]): Unit = {
    dataFrameWriter.format("jdbc")
      .option("url", "jdbc:postgresql:postgres")
      .option("dbtable", "test")
      .option("user", "postgres")
      .option("password", "toor")
      .save()
  }
}

