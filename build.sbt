// phuonglh, May 3, 2020
// updated December 15, 2021 (upgrade to Spark 3.2.0 and BigDL 0.13.0)
// updated July 17, 2022 (keep only the VietnameseTokenizer)
val sparkVersion = "3.2.0"
val jobServerVersion = "0.11.1"

javacOptions ++= Seq("-encoding", "UTF-8", "-XDignore.symbol.file", "true")

scalacOptions ++= Seq(
  "-Xfatal-warnings",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps"
)

lazy val commonSettings = Seq(
  scalaVersion := "2.12.15",
  name := "vlp",
  organization := "tqtensor.com",
  version := "1.0",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
    "com.github.scopt" %% "scopt" % "3.7.1"
  ),
  run in Compile := Defaults
    .runTask(
      fullClasspath in Compile,
      mainClass in (Compile, run),
      runner in (Compile, run)
    )
    .evaluated,
  runMain in Compile := Defaults
    .runMainTask(fullClasspath in Compile, runner in (Compile, run))
    .evaluated
)
// root project
lazy val root = (project in file(".")).aggregate(
  tok
)

// tokenization module
lazy val tok = (project in file("tok"))
  .settings(
    commonSettings,
    mainClass in assembly := Some("vlp.tok.VietnameseTokenizer"),
    assemblyJarName in assembly := "tok.jar"
  )
