package Services

import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.datastax.spark.connector.cql.CassandraConnector

object SentimentAnalysis {
  def analysis(
                spark: SparkSession,
                dataset: DataFrame,
                lexicon: Seq[String],
                statusOfSentimentWords: mutable.Map[String, String],
              ): Unit = {
    val connector = CassandraConnector(spark.sparkContext.getConf)

    connector.withSessionDo { session =>
      val businessWordCounts = dataset.select("business_id", "filtered").rdd
        .flatMap { row =>
          val businessId = row.getAs[String]("business_id")
          val tokens = Option(row.getAs[Seq[String]]("filtered"))
            .getOrElse(Seq.empty[String])
          tokens.map(_.toLowerCase)
            .filter(token => lexicon.contains(token))
            .map(word => ((businessId, word), 1))
        }
        .reduceByKey(_ + _)
        .map {
          case ((businessId, word), count) =>
            (businessId, word, count, statusOfSentimentWords(word.toLowerCase()))
        }

      businessWordCounts.collect().foreach {
        case (businessId, word, count, status) =>
          session.execute(
            s"""
              INSERT INTO ${Configs.SentimentAnalysisTable} (business_id, word, count, status)
              VALUES ('$businessId', '$word', $count, '$status')
            """)
      }

      session.close()
    }
  }
}
