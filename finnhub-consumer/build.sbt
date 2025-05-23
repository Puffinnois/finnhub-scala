ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "finnhub-spark-consumer",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.4.1",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.1",
      "org.apache.kafka" % "kafka-clients" % "3.5.1",
      "org.java-websocket" % "Java-WebSocket" % "1.5.2"
    )
  )