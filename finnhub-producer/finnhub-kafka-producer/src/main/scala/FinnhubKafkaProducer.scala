import java.net.URI
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake

object FinnhubKafkaProducer extends App {

  val API_KEY = "cvns001r01qq3c7gupo0cvns001r01qq3c7gupog"
  val KAFKA_TOPIC = "stock-market-data"
  val STOCK_SYMBOLS = List("AAPL", "TSLA", "GOOGL")

  // Configuration Kafka
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  // Connexion WebSocket à Finnhub
  val wsUrl = s"wss://ws.finnhub.io?token=$API_KEY"

  val socket = new WebSocketClient(new URI(wsUrl)) {
    override def onOpen(handshakedata: ServerHandshake): Unit = {
      println("✅ WebSocket connecté à Finnhub")
      STOCK_SYMBOLS.foreach { symbol =>
        send(s"""{"type":"subscribe","symbol":"$symbol"}""")
      }
    }

    override def onMessage(message: String): Unit = {
      println(s"📨 Message reçu : $message")
      val record = new ProducerRecord[String, String](KAFKA_TOPIC, message)
      producer.send(record)
    }

    override def onClose(code: Int, reason: String, remote: Boolean): Unit = {
      println(s"❌ WebSocket fermé : $reason")
    }

    override def onError(ex: Exception): Unit = {
      println(s"⚠️ Erreur WebSocket : ${ex.getMessage}")
    }
  }

  socket.connect()
// Garde le programme en vie pour écouter les messages
while (true) {
  Thread.sleep(1000)
}
}
