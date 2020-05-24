import sbt._

name := "ecommerce_practice"

scalacOptions ++= Seq("-deprecation")

resolvers += Resolver.sonatypeRepo("releases")

val enumeratumVersion = "1.6.0"

lazy val buildSettings = Seq(
  organization := "my.example.domain",
  version := "0.1.0",
  scalaVersion := "2.11.12"
)

val ecommerce_practice = (project in file("."))
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testSettings): _*)
  .settings(
    buildSettings,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.0",
      "org.apache.spark" %% "spark-sql" % "2.4.0",

      "org.scalatest" %% "scalatest" % "3.1.1" % "test, it"
    )
  )