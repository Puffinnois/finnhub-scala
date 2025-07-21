ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "FinnhubKafkaProducer",
    version := "0.1",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "3.5.1",
      "org.java-websocket" % "Java-WebSocket" % "1.5.3",
      "com.lihaoyi" %% "ujson" % "3.1.3",
      "org.apache.spark" %% "spark-core" % "3.3.4",
      "org.apache.spark" %% "spark-sql" % "3.3.4",
      "org.slf4j" % "slf4j-simple" % "2.0.13"
    )
  )