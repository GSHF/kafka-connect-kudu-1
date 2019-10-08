name := "kafka-connect-kudu"

version := "0.1"

scalaVersion := "2.11.12"

lazy val confluentVersion = "5.2.1"
lazy val kafkaVersion = "2.2.0-cp2"
lazy val kuduVersion = "1.9.0"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "connect-api"     % kafkaVersion % "provided",
  "org.apache.kafka" % "connect-json"    % kafkaVersion % "provided",
  "org.apache.kafka" % "connect-runtime" % kafkaVersion % "provided",
  "io.confluent"     % "kafka-avro-serializer" % confluentVersion % "provided",
  "io.confluent"     % "kafka-connect-avro-converter" % confluentVersion % "provided",
  "org.apache.kudu" % "kudu-client" % kuduVersion,
  "org.apache.calcite" % "calcite-linq4j" % "1.8.0"
    exclude ("com.google.guava", "guava"),
  "com.datamountaineer" % "kafka-connect-common" % "1.1.8",
  "jakarta.ws.rs"    % "jakarta.ws.rs-api" % "2.1.5"
  ).map(_ exclude("javax.ws.rs", "javax.ws.rs-api"))

resolvers += "Confluent" at "http://packages.confluent.io/maven/"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
