package Batch
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, IntegerType}


object YoutubeVideosProc{
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val dfraw = SparkReader.Reader.csv("USvideos.csv")
    val dfstopwords = SparkReader.Reader.csv("stopwords-en.txt")
    val stopwords = dfstopwords.select("words").rdd.map(data => data(0)).collect().toList
    dfraw.show()

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


    //best channel base on likes
    val likechannels = dfraw.select("channel_title","likes")
      .withColumn("likes",new Column("likes").cast(IntegerType))
      .groupBy("channel_title").sum("likes")
      .orderBy(desc("sum(likes)"))
    likechannels.show()

    //pivoting channel and title likes
    val pivotchannel = dfraw.select("channel_title","title","likes","views")
      .withColumn("likes",new Column("likes").cast(IntegerType))
      .withColumn("percentage_likes",(new Column("likes")/new Column("views"))*100).orderBy(desc("percentage_likes"))
      .limit(100)
      .groupBy("channel_title").pivot("title").sum("likes")
      .na.fill(0)
    pivotchannel.show()

    // most category searching
    dfraw.select("title","tags","likes","views")
      .withColumn("percentage_likes",col("likes")/col("views"))
      .orderBy(desc("percentage_likes")).limit(1000)
      .select("tags").withColumn("words",Utils.regexp_extractAll(col("tags"),lit("\\w+"),lit(0)))
      .select("words").withColumn("words",explode(col("words"))).filter(length(col("words")) > 1)
      .groupBy("words").count().orderBy(desc("count"))
      .withColumn("words",lower(col("words")))
      .filter(!col("words").isin(stopwords:_*))
      .show()

    //save dataframe to DB
    //val writedb = Writedb(dfraw)
    //SavetoDB(writedb,"usvideos")
    //SavetoDB(Writedb(likesvideos),"likesvideos")
    //SavetoDB(Writedb(likechannels),"likechannels")
    //SavetoDB(Writedb(pivotchannel),"pivotchannel")

    /**
    pivotchannel.write.format("csv")
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ",")
      .option("quoteMode", "true")
      .save("hdfs://datanode:9000/hadoop/dfs/data/hasil.csv")
     **/

  }
}
