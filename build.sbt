lazy val root = (project in file("."))
  .settings(commonSettings)

val sparkVersion = settingKey[String]("Spark version")
val scalaTestVersion = settingKey[String]("ScalaTest version")

name := "spark-cef-reader"
version := "0.5-SNAPSHOT"
organization := "com.bp"
description := "CEF data source for Spark"
homepage := Some(url("https://github.com/bp/spark-cef-reader"))
licenses += ("Apache License, Version 2.0", url("https://www.apache.org/licenses/LICENSE-2.0"))
scmInfo := Some(ScmInfo(url("https://github.com/bp/spark-cef-reader"), "https://github.com/bp/spark-cef-reader.git"))
developers ++= List(
  Developer(id = "dazfuller", name = "Darren Fuller", email = "darren@elastacloud.com", url = url("https://github.com/elastacloud")),
  Developer(id = "azurecoder", name = "Richard Conway", email = "richard@elastacloud.com", url = url("https://github.com/elastacloud"))
)

target := file("target") / s"spark-${sparkVersion.value}"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  s"${artifact.name}-${sv.binary}_${sparkVersion.value}-${module.revision}.${artifact.extension}"
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion.value % Provided,
)

Compile / unmanagedSourceDirectories ++= {
  if (sparkVersion.value < "3.2.0") {
    Seq(baseDirectory.value / "src/main/3.0/scala")
  } else {
    Seq(baseDirectory.value / "src/main/3.2/scala")
  }
}

// Setup test dependencies and configuration
Test / parallelExecution := false
Test / fork := true

libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % scalaTestVersion.value % Test,
  "org.scalatest" %% "scalatest" % scalaTestVersion.value % Test
)

// Define common settings for the library
val commonSettings = Seq(
  sparkVersion := System.getProperty("sparkVersion", "3.2.0"),
  scalaVersion := {
    if (sparkVersion.value >= "3.2.0") {
      "2.12.14"
    } else {
      "2.12.10"
    }
  },
  scalaTestVersion := "3.2.10"
)
