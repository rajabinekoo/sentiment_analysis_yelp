import Database.{InitCassandra, InitReviews}
import Services.{Lexicon, SentimentAnalysis, Tokenizer}
import org.apache.spark.sql.SparkSession

object Main extends App {
  private val spark = SparkSession.builder()
    .appName("Spark Sentiment Analysis")
    .master("local[*]")
    .config("spark.master", "local")
    .config("spark.cassandra.connection.port", Configs.CassandraPort)
    .config("spark.cassandra.connection.host", Configs.CassandraHost)
    .getOrCreate()

  InitCassandra.init(spark)
  if (Configs.SyncCassandraWithDataset) InitReviews.init(spark)
  private val dataset = Tokenizer.getExtractedDataframe(spark)
  private val (lexicon, statusOfSentimentWords) = Lexicon.extractStatusWords(spark)
  SentimentAnalysis.analysis(spark, dataset, lexicon, statusOfSentimentWords)
}
