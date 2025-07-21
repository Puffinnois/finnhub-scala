import java.io.{BufferedWriter, FileWriter}
import java.net.URI
import java.time.{LocalDateTime, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Properties

import scala.util.control.NonFatal

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import ujson._

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window

object FinnhubKafkaProducer extends App {

  // ───────────────────────────── Logger
  private val tsFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private def log(msg: String): Unit =
    println(s"[${LocalDateTime.now.format(tsFmt)}] $msg")

  // ───────────────────────────── Config
  private val ApiKey           = sys.env.getOrElse("FINNHUB_API_KEY", "cvns001r01qq3c7gupo0cvns001r01qq3c7gupog")
  private val StockSymbols     = List("AAPL", "TSLA", "GOOGL")
  private val BootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
  private val KafkaTopic       = "stock-market-data"
  private val CsvFilePath      = "data/backup_stock_data.csv"

  // ───────────────────────────── Kafka Producer
  private val props = new Properties()
  props.put("bootstrap.servers", BootstrapServers)
  props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  private val producer = new KafkaProducer[String, String](props)

  // ───────────────────────────── CSV Writer
  private val csvWriter = new BufferedWriter(new FileWriter(CsvFilePath, true)) // append mode

  // ───────────────────────────── WebSocket Finnhub
  private def createWebSocket(): WebSocketClient = {
    val wsUrl = s"wss://ws.finnhub.io?token=$ApiKey"

    new WebSocketClient(new URI(wsUrl)) {
      override def onOpen(handshake: ServerHandshake): Unit = {
        log("WebSocket connected")
        StockSymbols.foreach { s =>
          send(s"""{"type":"subscribe","symbol":"$s"}""")
          log(s"Subscribed to $s")
        }
      }

      override def onMessage(message: String): Unit = {
        log(s"Message received : $message")
        val ts = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        val out = s"""{"timestamp":"$ts","data":$message}"""
        val key = try read(message)("data")(0)("s").str catch { case _: Throwable => "unknown" }

        // Send to Kafka
        producer.send(new ProducerRecord[String, String](KafkaTopic, key, out),
          (_: RecordMetadata, ex: Exception) =>
            if (ex == null) log(s"Kafka OK : key=$key")
            else log(s"Kafka error : ${ex.getMessage}")
        )

        // Save CSV
        try {
          val data = read(message)("data")(0)
          val symbol = data("s").str
          val price = data("p").num
          csvWriter.write(s"$ts,$symbol,$price\n")
          csvWriter.flush()
        } catch {
          case ex: Exception => log(s"CSV error : ${ex.getMessage}")
        }
      }

      override def onClose(code: Int, reason: String, remote: Boolean): Unit = {
        log(s"WebSocket closed ($code) : $reason")
        reconnectWithBackoff()
      }

      override def onError(ex: Exception): Unit = {
        log(s"WebSocket error : ${ex.getMessage}")
        reconnectWithBackoff()
      }

      private def reconnectWithBackoff(): Unit = new Thread(() => {
        var delay = 1000
        var success = false
        while (!success) {
          try {
            log(s"Reconnection in ${delay / 1000}s...")
            Thread.sleep(delay)
            this.reconnectBlocking()
            success = true
            log("Reconnection successful")
          } catch {
            case NonFatal(_) =>
              delay = math.min(delay * 2, 30000)
          }
        }
      }, "ws-reconnect").start()
    }
  }

  // ───────────────────────────── Analyze Spark CSV
  private def analyzeBackupWithSpark(): Unit = {
    log("Analyzing backed-up data with Spark...")

    val spark = SparkSession.builder()
      .appName("AnalyzeStockBackup")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv(CsvFilePath)
      .toDF("timestamp", "symbol", "price")
      .withColumn("timestamp", F.to_timestamp($"timestamp"))

    println(s"Total rows : ${df.count()}")

    val windowSpec = Window.orderBy("timestamp")
    val dfWithRowNum = df.withColumn("row_num", F.row_number().over(windowSpec))

    dfWithRowNum.show(10, false)

    spark.stop()
  }

  // ───────────────────────────── Launch WebSocket
  val socket = createWebSocket()
  socket.connectBlocking()

  // ───────────────────────────── Shutdown clean
  sys.addShutdownHook {
    log("Shutdown requested")
    socket.close()
    csvWriter.close()
    producer.flush()
    producer.close()

    // Analyze CSV
    analyzeBackupWithSpark()
  }

  // Wait loop
  while (socket.isOpen) Thread.sleep(10000)
}
