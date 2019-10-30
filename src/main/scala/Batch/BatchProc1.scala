package Batch
import Jdbc.Jdbc.{SavetoDB, Writedb}
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession.builder
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.functions._


object BatchProc1{
  def main(args: Array[String]): Unit = {
    val spark = builder.master("local").appName("Batch Proces").getOrCreate
    val dfraw = spark.read.format("csv").option("inferSchema", "true").option("header","true").load("USvideos.csv")
      .na.drop()

    val writedb = Writedb(dfraw)
    SavetoDB(writedb,"usvideos")

    //best percentage for likes videos
    val likesvideos = dfraw.select("title","views","likes","dislikes")
      .withColumn("percentage_likes",(new Column("likes")/new Column("views"))*100)
      .withColumn("percentage_dislikes",(new Column("dislikes")/new Column("views"))*100)
      .withColumn("percentage_likes",new Column("percentage_likes").cast(DecimalType(4,2)))
      .withColumn("percentage_dislikes",new Column("percentage_dislikes"))
      .withColumn("percentage_dislikes",new Column("percentage_dislikes").cast(DecimalType(4,2)))
      .withColumn("percentage_nor",lit(100)-(new Column("percentage_likes")+ new Column("percentage_dislikes")))
      .withColumn("percentage_nor",new Column("percentage_nor").cast(DecimalType(4,2)))
      .select("title","percentage_likes","percentage_dislikes","percentage_nor")
      .orderBy(desc("percentage_likes"))
    SavetoDB(Writedb(likesvideos),"likesvideos")

    //best channel base on likes
    val likechannels = dfraw.select("channel_title","likes")
      .withColumn("likes",new Column("likes").cast(IntegerType))
      .groupBy("channel_title").sum("likes")
      .orderBy(desc("sum(likes)"))

    SavetoDB(Writedb(likechannels),"likechannels")

    //pivoting channel and title likes
    val pivotchannel = dfraw.select("channel_title","title","likes","views")
      .withColumn("likes",new Column("likes").cast(IntegerType))
      .withColumn("percentage_likes",(new Column("likes")/new Column("views"))*100).orderBy(desc("percentage_likes"))
      .limit(100)
      .groupBy("channel_title").pivot("title").sum("likes")
      .na.fill(0)

    SavetoDB(Writedb(pivotchannel),"pivotchannel")
    



  }


}
