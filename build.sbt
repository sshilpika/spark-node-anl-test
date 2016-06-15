name := "sort-file-generator"

version := "1.0"

scalaVersion := "2.10.6"

resolvers += "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.6" % Test,
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1" % "provided",
  "com.github.marklister" %% "product-collections" % "1.4.3",
  "com.github.scopt" %% "scopt" % "3.4.0"
)

mainClass in assembly := Some("gov.anl.alcf.fileG")

//sources in Compile <<= (sources in Compile).map(_ filter(_.name == "fileG.scala"))
