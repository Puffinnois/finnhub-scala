name := "finnhub-kafka-producer"

version := "0.1"

scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.6.1",
  "org.java-websocket" % "Java-WebSocket" % "1.5.3"
)
