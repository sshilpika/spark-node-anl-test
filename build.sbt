name := "sort-file-generator"

version := "1.0"

scalaVersion := "2.10.6"

resolvers += "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.6" % Test,
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided"
)

mainClass in assembly := Some("fileG")

//sources in Compile <<= (sources in Compile).map(_ filter(_.name == "fileG.scala"))
