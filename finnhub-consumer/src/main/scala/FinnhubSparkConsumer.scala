// src/main/scala/FinnhubSparkConsumer.scala
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import scala.jdk.CollectionConverters._
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object FinnhubSparkConsumer {

  /** Déplace les CSV existants d’aggregated/ vers historical/ */
  private def archiveOldAggregatedFiles(): Unit = {
    val srcDir: Path = Paths.get("data/csv/aggregated")
    val dstDir: Path = Paths.get("data/historical")
    if (Files.exists(srcDir)) {
      Files.createDirectories(dstDir)
      Files.list(srcDir).iterator().asScala
        .filter(p => Files.isRegularFile(p) && p.toString.endsWith(".csv"))
        .foreach { p =>
          Files.move(p, dstDir.resolve(p.getFileName),
                     StandardCopyOption.REPLACE_EXISTING)
          println(s"[ARCHIVE] ${p.getFileName} → historical/")
        }
    }
  }

  def main(args: Array[String]): Unit = {
    archiveOldAggregatedFiles()

    val spark = SparkSession.builder()
      .appName("Finnhub Spark Consumer")
      .master("local[*]")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs","false")
      .config("spark.sql.sources.commitProtocolClass",
              "org.apache.spark.internal.io.HadoopMapReduceCommitProtocol")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // --- Schéma Finnhub -------------------------------------------------------
    val schema = new StructType()
      .add("timestamp", StringType)
      .add("data", new StructType()
        .add("type", StringType)
        .add("data", ArrayType(new StructType()
          .add("p", DoubleType)
          .add("s", StringType)
          .add("t", LongType)   // epoch-ms
          .add("v", DoubleType)
        ))
      )

    // --- Source Kafka ---------------------------------------------------------
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","stock-market-data")
      .load()

    // --- Parsing + enrichissement --------------------------------------------
    val parsedDF = kafkaDF
      .selectExpr("CAST(value AS STRING) AS json_str")
      .select(F.from_json($"json_str", schema).as("json"))
      .filter($"json.data.type" === "trade")
      .select(F.explode($"json.data.data").as("d"), $"json.timestamp".as("ingest_time"))
      .select(
        $"d.s".as("symbol"),
        $"d.p".as("price"),
        $"d.v".as("volume"),
        $"d.t".as("event_timestamp"),
        $"ingest_time"
      )
      // conversion précise en utilisant la fonction SQL
      .withColumn("timestamp_ts", F.expr("timestamp_millis(event_timestamp)"))
      .withColumn("minute", F.date_format($"timestamp_ts","yyyy-MM-dd HH:mm"))
      .withColumn("hour",   F.date_format($"timestamp_ts","yyyy-MM-dd HH"))

    // --- Agrégations glissantes ----------------------------------------------
    val aggregatedDF = parsedDF
      .withColumn("pxv", $"price" * $"volume")
      .withWatermark("timestamp_ts","2 minutes")
      .groupBy(
        $"symbol",
        F.window($"timestamp_ts","1 minute","15 seconds")
      )
      .agg(
        F.avg($"price").as("avg_price"),
        F.sum($"volume").as("total_volume"),
        F.count("*").as("trade_count"),
        F.sum($"pxv").as("sum_pxv"),
        F.max($"price").as("max_price"),
        F.min($"price").as("min_price"),
        F.stddev_samp($"price").as("std_price")
      )
      .withColumn("vwap", $"sum_pxv" / $"total_volume")
      .select(
        $"symbol",
        F.date_format($"window.start","yyyy-MM-dd HH:mm:ss").as("minute"),
        $"avg_price", $"max_price", $"min_price", $"std_price",
        $"total_volume", $"trade_count", $"vwap"
      )

    // --- Sinks ----------------------------------------------------------------
    parsedDF.writeStream
      .queryName("WriteParquet")
      .format("parquet")
      .option("path","data/parquet")
      .option("checkpointLocation","data/checkpoint_parquet")
      .partitionBy("symbol","hour")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()

    parsedDF.writeStream
      .queryName("WriteCSV")
      .format("csv")
      .option("path","data/csv/files")
      .option("checkpointLocation","data/csv/checkpoint")
      .option("header","true")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()

    aggregatedDF.writeStream
      .queryName("AggregatedCSV")
      .format("csv")
      .option("path","data/csv/aggregated")
      .option("checkpointLocation","data/csv/aggregated_checkpoint")
      .option("header","true")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()

    println(
      """|Consumer Spark Streaming lancé
         |  • Parquet brut      → data/parquet/
         |  • CSV brut          → data/csv/files/
         |  • CSV agrégé        → data/csv/aggregated/ (slide 15 s)
         |""".stripMargin)

    spark.streams.awaitAnyTermination()
  }
}