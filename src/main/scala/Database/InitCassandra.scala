package Database

import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector.cql.CassandraConnector

object InitCassandra {
  def init(spark: SparkSession): Unit = {
    val connector = CassandraConnector(spark.sparkContext.getConf)

    connector.withSessionDo { session =>
      session.execute(
        s"""
          CREATE KEYSPACE IF NOT EXISTS ${Configs.CassandraKeyspace}
          WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)

      session.execute(s"USE ${Configs.CassandraKeyspace};")

      session.execute(
        s"""
          CREATE TABLE IF NOT EXISTS ${Configs.BucketedReviewsTable} (
            bucket_id BIGINT,         -- Partition key
            date TIMESTAMP,           -- Clustering key
            review_id TEXT,
            business_id TEXT,
            user_id TEXT,
            body TEXT,
            PRIMARY KEY ((bucket_id), date, review_id)
          ) WITH CLUSTERING ORDER BY (date DESC);
        """)

      session.execute(
        s"""
          CREATE TABLE IF NOT EXISTS ${Configs.ReviewsTable} (
            business_id TEXT,         -- Partition key
            date TIMESTAMP,           -- Clustering key
            review_id TEXT,
            user_id TEXT,
            body TEXT,
            PRIMARY KEY ((business_id), date, review_id)
          ) WITH CLUSTERING ORDER BY (date DESC);
        """)

      session.execute(
        s"""
          CREATE TABLE IF NOT EXISTS ${Configs.SentimentAnalysisTable} (
            business_id TEXT,
            word TEXT,         
            count BIGINT,         
            status TEXT,         
            PRIMARY KEY ((business_id), count, word, status)
          ) WITH CLUSTERING ORDER BY (count DESC);
        """)

      session.close()
    }
  }
}
