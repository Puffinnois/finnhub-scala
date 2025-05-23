ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "finnhub-kafka-producer",
    libraryDependencies ++= Seq(
      "org.apache.kafka"  %  "kafka-clients"   % "3.5.1",
      "org.java-websocket" % "Java-WebSocket"  % "1.5.2",
      "com.lihaoyi"      %% "ujson"            % "3.3.1"
    )
  )