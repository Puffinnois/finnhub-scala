import java.net.URI
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.util.control.NonFatal
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake

object FinnhubKafkaProducer extends App {

  val API_KEY = "cvns001r01qq3c7gupo0cvns001r01qq3c7gupog"
  val KAFKA_TOPIC = "stock-market-data"
  val STOCK_SYMBOLS = List("AAPL", "TSLA", "GOOGL")
  val BOOTSTRAP_SERVERS = "localhost:9092"

  val props = new Properties()
  props.put("bootstrap.servers", BOOTSTRAP_SERVERS)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  def createWebSocket(): WebSocketClient = {
    val wsUrl = s"wss://ws.finnhub.io?token=$API_KEY"

    new WebSocketClient(new URI(wsUrl)) {

      override def onOpen(handshakedata: ServerHandshake): Unit = {
        println("WebSocket connecté à Finnhub")
        STOCK_SYMBOLS.foreach { symbol =>
          val subscribeMessage = s"""{"type":"subscribe","symbol":"$symbol"}"""
          send(subscribeMessage)
          println(s"Abonnement à $symbol")
        }
      }

      override def onMessage(message: String): Unit = {
        val timestamp = ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        val messageWithTimestamp = s"""{"timestamp":"$timestamp","data":$message}"""
        println(s"Donnée envoyée : $messageWithTimestamp")
        val record = new ProducerRecord[String, String](KAFKA_TOPIC, messageWithTimestamp)
        producer.send(record)
      }

      override def onClose(code: Int, reason: String, remote: Boolean): Unit = {
        println(s"WebSocket fermé : $reason (code=$code, remote=$remote)")
        reconnectWithBackoff()
      }

      override def onError(ex: Exception): Unit = {
        println(s"Erreur WebSocket : ${ex.getMessage}")
        reconnectWithBackoff()
      }

      def reconnectWithBackoff(): Unit = {
        new Thread(() => {
          var retryDelay = 1000
          var connected = false
          while (!connected) {
            try {
              println(s"Tentative de reconnexion dans ${retryDelay / 1000}s...")
              Thread.sleep(retryDelay)
              this.reconnectBlocking()
              connected = true
              println("Reconnexion réussie")
            } catch {
              case NonFatal(_) =>
                retryDelay = math.min(retryDelay * 2, 30000)
            }
          }
        }).start()
      }
    }
  }

  val socket = createWebSocket()
  socket.connect()

  scala.io.StdIn.readLine("Appuyez sur ENTRÉE pour quitter...\n")
  println("Fermeture en cours...")
  socket.close()
  producer.close()
}