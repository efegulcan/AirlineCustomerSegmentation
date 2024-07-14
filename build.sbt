ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

ThisBuild / javacOptions ++= Seq("-source", "11", "-target", "11")

ThisBuild / scalacOptions ++= Seq("-target:jvm-11")

lazy val root = (project in file("."))
  .settings(
    name := "AirlineCustomerSegmentation",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.4.0",
      "org.apache.spark" %% "spark-sql" % "3.4.0"
    )
  )
