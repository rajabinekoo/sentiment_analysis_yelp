package Database

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions => F}

object InitReviews {
  private val bucketSize = 7

  private val bucketIdUDF = (minTimestamp: Long) => F.udf((date: java.sql.Timestamp) => {
    1 + ((date.getTime - minTimestamp) / (bucketSize * 24 * 60 * 60 * 1000)).toInt
  })

  def init(spark: SparkSession): Unit = {
    var df = spark.read
      .format("json")
      .option("inferSchema", "true")
      .load(Configs.DataSource)
    
    df.select(
        col("text").alias("body"), col("review_id"), col("date"),
        col("business_id"), col("user_id"),
      ).write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "yelp_data_mining")
      .option("table", "reviews")
      .mode("append")
      .save()

    df = df.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
    import spark.implicits._
    val minTimestamp = df.select(F.min("date")).as[java.sql.Timestamp].collect()(0).getTime
    df = df.withColumn("bucket_id", bucketIdUDF(minTimestamp)(F.col("date")))

    df.select(
        col("text").alias("body"), col("review_id"), col("date"),
        col("business_id"), col("user_id"), col("bucket_id")
      ).write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "yelp_data_mining")
      .option("table", "reviews_bucketed")
      .mode("append")
      .save()
  }
}
