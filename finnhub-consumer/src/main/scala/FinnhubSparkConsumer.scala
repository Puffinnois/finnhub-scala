import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FinnhubSparkConsumer {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Finnhub Spark Consumer")
      .master("local[*]")
      .getOrCreate()

    // ✅ Cette ligne DOIT être ici (après spark = ...)
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    // Lecture depuis le topic Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "stock-market-data")
      .load()

    // Convertir la colonne "value" (bytes) en string
    val messages = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

    // Filtrer uniquement les messages contenant "trade"
    val tradeMessages = messages.filter(_.contains("type\":\"trade"))

    // Écrire les résultats dans un dossier parquet
    val query = tradeMessages.writeStream
      .format("parquet")
      .option("path", "data/parquet")
      .option("checkpointLocation", "data/checkpoint")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}

