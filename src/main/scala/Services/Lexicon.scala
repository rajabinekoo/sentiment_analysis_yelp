package Services

import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


object Lexicon {
  def extractStatusWords(spark: SparkSession): (Seq[String], mutable.Map[String, String]) = {
    val statusOfSentimentWords: mutable.Map[String, String] = mutable.Map()
    var lexiconDF = spark.read
      .format("json")
      .option("inferSchema", "true")
      .csv(Configs.LexiconDataSource)
    lexiconDF = lexiconDF.filter(col("_c105") === 1 || col("_c106") === 1).select("_c0", "_c105", "_c106")

    import spark.implicits._
    val lexiconSeq: Seq[String] = lexiconDF.select("_c0").as[String].collect().toSeq
    val positiveSeq: Seq[String] = lexiconDF.select("_c105").as[String].collect().toSeq
    for (i <- lexiconSeq.indices) {
      statusOfSentimentWords(lexiconSeq(i).toLowerCase()) =
        if (positiveSeq(i) == "1") "positive" else "negative"
    }

    lexiconDF.unpersist()
    System.gc()
    (lexiconSeq, statusOfSentimentWords)
  }
}
