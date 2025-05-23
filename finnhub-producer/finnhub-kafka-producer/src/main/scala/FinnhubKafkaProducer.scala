import java.net.URI
import java.time.{LocalDateTime, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.util.control.NonFatal
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import ujson._

object FinnhubKafkaProducer extends App {

  /* --------------------------------------------------------------------- *
   *  logs                                                   *
   * --------------------------------------------------------------------- */
  private val tsFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private def log(msg: String): Unit =
    println(s"[${LocalDateTime.now.format(tsFmt)}] $msg")

  /* --------------------------------------------------------------------- *
   *  parameters                                                          *
   * --------------------------------------------------------------------- */
  private val ApiKey           = sys.env.getOrElse("FINNHUB_API_KEY", "cvns001r01qq3c7gupo0cvns001r01qq3c7gupog")
  private val StockSymbols     = List("AAPL", "TSLA", "GOOGL")
  private val BootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
  private val KafkaTopic       = "stock-market-data"

  /* --------------------------------------------------------------------- *
   *  kafka producer                                                       *
   * --------------------------------------------------------------------- */
  private val props = new Properties()
  props.put("bootstrap.servers", BootstrapServers)
  props.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  private val producer = new KafkaProducer[String, String](props)

  /* --------------------------------------------------------------------- *
   *  websocket finnhub                                                    *
   * --------------------------------------------------------------------- */
  private def createWebSocket(): WebSocketClient = {
    val wsUrl = s"wss://ws.finnhub.io?token=$ApiKey"

    new WebSocketClient(new URI(wsUrl)) {

      override def onOpen(handshake: ServerHandshake): Unit = {
        log("WebSocket connecté")
        StockSymbols.foreach { s =>
          send(s"""{"type":"subscribe","symbol":"$s"}""")
          log(s"Abonnement à $s")
        }
      }

      override def onMessage(message: String): Unit = {
        log(s"Message Finnhub brut : $message")

        val ts  = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        val out = s"""{"timestamp":"$ts","data":$message}"""
        val key = try read(message)("data")(0)("s").str catch { case _: Throwable => null }

        producer.send(new ProducerRecord[String, String](KafkaTopic, key, out),
          (_: RecordMetadata, ex: Exception) =>
            if (ex == null) log(s"Envoi Kafka OK pour clé=$key")
            else             log(s"Erreur Kafka : ${ex.getMessage}")
        )
      }

      override def onClose(code: Int, reason: String, remote: Boolean): Unit = {
        log(s"WebSocket fermé ($code) : $reason")
        reconnectWithBackoff()
      }

      override def onError(ex: Exception): Unit = {
        log(s"Erreur WebSocket : ${ex.getMessage}")
        reconnectWithBackoff()
      }

      private def reconnectWithBackoff(): Unit = new Thread(() => {
        var delay = 1000
        var ok    = false
        while (!ok) {
          try {
            log(s"Nouvelle tentative dans ${delay / 1000}s")
            Thread.sleep(delay)
            this.reconnectBlocking()
            ok = true
            log("Reconnexion réussie")
          } catch {
            case NonFatal(_) => delay = math.min(delay * 2, 30000)
          }
        }
      }, "ws-reconnect").start()
    }
  }

  /* --------------------------------------------------------------------- *
   *  start & stop                                                   *
   * --------------------------------------------------------------------- */
  val socket = createWebSocket()
  socket.connectBlocking()

  sys.addShutdownHook {
    log("Arrêt demandé → fermeture des ressources")
    socket.close()
    producer.flush()
    producer.close()
  }

  while (socket.isOpen) Thread.sleep(10_000)
}