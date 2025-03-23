package Services

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}

object Tokenizer {
  def getExtractedDataframe(spark: SparkSession): DataFrame = {
    val df = spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", Configs.CassandraKeyspace)
      .option("table", Configs.BucketedReviewsTable)
      .load()

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("body")
      .setOutputCol("words")
      .setPattern("\\W+")

    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")

    val pipeline = new Pipeline().setStages(Array(regexTokenizer, remover))
    val model = pipeline.fit(df)
    val result = model.transform(df).drop("words").drop("body")

    df.unpersist()
    System.gc()
    result
  }
}
