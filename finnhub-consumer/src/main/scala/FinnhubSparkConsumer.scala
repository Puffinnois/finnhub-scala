// -----------------------------------------------------------------------------
// src/main/scala/FinnhubSparkConsumer.scala
// -----------------------------------------------------------------------------
package consumer

import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object FinnhubSparkConsumer {

  // ────────────────────────── utilitaires I/O ────────────────────────────────
  private val TS_FMT     = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private val RUN_ID     = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
                      .format(LocalDateTime.now(ZoneId.systemDefault))

  private val ckRoot     = Paths.get("data/checkpoints").toAbsolutePath
  private val ckParsed   = ckRoot.resolve(s"run_$RUN_ID/parsed")
  private val ckAgg      = ckRoot.resolve(s"run_$RUN_ID/aggregated")

  private val rawOutDir  = Paths.get("data/csv/files").toAbsolutePath
  private val aggOutDir  = Paths.get("data/csv/aggregated").toAbsolutePath
  private val parquetDir = Paths.get("data/parquet").toAbsolutePath

  /** archive les agrégats déjà présents (avant nouveau run) */
  private def archiveOldAggregatedFiles(): Unit = {
    val dstDir = Paths.get("data/historical")
    if (Files.exists(aggOutDir)) {
      Files.createDirectories(dstDir)
      Files.list(aggOutDir).iterator().asScala
        .filter(p => Files.isRegularFile(p) && p.toString.endsWith(".csv"))
        .foreach { p =>
          val tgt = dstDir.resolve(p.getFileName)
          try {
            Files.move(p, tgt, StandardCopyOption.REPLACE_EXISTING)
            println(s"[ARCHIVE] ${p.getFileName} → historical/")
          } catch {
            case _: java.nio.file.FileSystemException =>
              println(s"[ARCHIVE] Ignoré (verrouillé) : ${p.getFileName}")
          }
        }
    }
  }

  /** écrit un batch en CSV puis log l’opération */
  private def writeCsvAndLog(df: DataFrame,
                             path: Path,
                             batchId: Long,
                             label: String,
                             partitionCols: Seq[String] = Seq.empty): Unit = {
    val writer = df.write.mode("append").option("header", "true")
    val finalWriter = if (partitionCols.nonEmpty) writer.partitionBy(partitionCols:_*) else writer
    finalWriter.csv(path.toString.replace("\\","/"))

    val ts   = TS_FMT.format(LocalDateTime.now)
    val rows = df.count()
    println(f"[$ts] CSV-$label%-8s batch=$batchId%04d  rows=$rows%,d  → $path")
  }

  // ───────────────────────────── programme ───────────────────────────────────
  def main(args: Array[String]): Unit = {
    archiveOldAggregatedFiles()

    val spark = SparkSession.builder()
      .appName("Finnhub Spark Consumer")
      .master("local[*]")
      .config("spark.sql.streaming.stateStore.maintenanceInterval", "30min")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // ─── schéma Finnhub brut ────────────────────────────────────────────────
    val schema = new StructType()
      .add("timestamp", StringType)
      .add("data", new StructType()
        .add("type", StringType)
        .add("data", ArrayType(new StructType()
          .add("p", DoubleType)
          .add("s", StringType)
          .add("t", LongType)   // epoch ms
          .add("v", DoubleType)
        ))
      )

    // ─── source Kafka ───────────────────────────────────────────────────────
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe",              "stock-market-data")
      .load()

    // ─── parsing + enrichissement ───────────────────────────────────────────
    val parsed = kafkaDF
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
      .withColumn("timestamp_ts", F.expr("timestamp_millis(event_timestamp)"))
      .withColumn("hour",   F.date_format($"timestamp_ts","yyyy-MM-dd HH"))
      .withColumn("minute", F.date_format($"timestamp_ts","yyyy-MM-dd HH:mm"))

    // ─── agrégats fenêtres glissantes 1 min / slide 15 s ────────────────────
    val aggregated = parsed
      .withColumn("pxv", $"price" * $"volume")
      .withWatermark("timestamp_ts","2 minutes")
      .groupBy($"symbol", F.window($"timestamp_ts", "1 minute", "15 seconds"))
      .agg(
        F.first($"price").as("open_price"),
        F.last($"price").as("close_price"),
        F.min($"price").as("min_price"),
        F.max($"price").as("max_price"),
        F.avg($"price").as("avg_price"),
        F.stddev_samp($"price").as("std_price"),
        F.sum($"volume").as("total_volume"),
        F.count("*").as("trade_count"),
        F.sum($"pxv").as("sum_pxv")
      )
      .withColumn("vwap", $"sum_pxv" / $"total_volume")
      .select(
        $"symbol",
        F.date_format($"window.start","yyyy-MM-dd HH:mm:ss").as("minute"),
        $"open_price", $"close_price",
        $"avg_price",  $"min_price", $"max_price", $"std_price",
        $"total_volume", $"trade_count", $"vwap"
      )

    // ─── sinks ───────────────────────────────────────────────────────────────

    // 1) CSV brut ------------------------------------------------------------
    parsed.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeCsvAndLog(
      batchDF.repartition($"symbol", $"hour"),
      rawOutDir, batchId, "RAW",
      partitionCols = Seq("symbol","hour")
    )
    }

      .option("checkpointLocation", ckParsed.toString.replace("\\","/"))
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // 2) CSV agrégé ----------------------------------------------------------
    aggregated.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      writeCsvAndLog(batchDF, aggOutDir, batchId, "AGG")
      }
      .option("checkpointLocation", ckAgg.toString.replace("\\","/"))
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // 3) Parquet brut -----------------
    parsed.writeStream
      .format("parquet")
      .option("path", parquetDir.toString.replace("\\","/"))
      .option("checkpointLocation", ckParsed.resolve("parquet").toString.replace("\\","/"))
      .partitionBy("symbol","hour")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    // ─── info démarrage ─────────────────────────────────────────────────────
    println(
      s"""
         |Consumer lancé (run $RUN_ID)
         |  CSV brut  → $rawOutDir
         |  CSV agrég → $aggOutDir   (fenêtre 1 min, slide 15 s)
         |  Checkpts  → $ckRoot/run_$RUN_ID/
         |""".stripMargin)

    spark.streams.awaitAnyTermination()
  }
}