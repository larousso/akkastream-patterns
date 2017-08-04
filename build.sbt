name := """akkastream-patterns"""
organization := "com.adelegue"

version := "1.0-SNAPSHOT"

lazy val root = (project in file("."))
  .enablePlugins(PlayScala)
  .enablePlugins(PlayJava)

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  guice,
  ws,
  "io.vavr" % "vavr" % "0.9.0",
  "org.reactivecouchbase" % "json-lib-javaslang" % "2.0.0",
  "com.softwaremill.macwire" %% "macros" % "2.3.0" % "provided",
  "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.0" % Test
)

resolvers ++= Seq(
  "jsonlib-repo" at "https://raw.githubusercontent.com/mathieuancelin/json-lib-javaslang/master/repository/releases"
)

// Adds additional packages into Twirl
//TwirlKeys.templateImports += "com.adelegue.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "com.adelegue.binders._"
