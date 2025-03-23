package object Configs {
  val CassandraHost = "localhost"
  val CassandraPort = "9042"
  val CassandraKeyspace = "yelp_data_mining"
  val SyncCassandraWithDataset = false

  //  https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset
  val DataSource = "src/main/resources/yelp.json"

  //  https://www.kaggle.com/datasets/wjburns/nrc-emotion-lexicon
  val LexiconDataSource = "src/main/resources/NRC-Emotion-Lexicon.csv"

  val ReviewsTable = "reviews"
  val BucketedReviewsTable = "reviews_bucketed"
  val SentimentAnalysisTable = "sentiment_analysis"
}
