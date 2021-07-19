name := "spark_test"

version := "0.1"

//slickVersion  := "3.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "com.typesafe.slick" %% "slick" % "3.3.1",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.3.1",
  "mysql" % "mysql-connector-java" % "8.0.15",
  "com.typesafe" % "config" % "1.4.1"
)
