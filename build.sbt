import com.typesafe.startscript.StartScriptPlugin

seq(StartScriptPlugin.startScriptForClassesSettings: _*)

name := "hello"

version := "1.0"

scalaVersion := "2.8.1"

resolvers += "twitter-repo" at "http://maven.twttr.com"

libraryDependencies ++= Seq(
  "com.twitter" % "finagle-core" % "1.9.12",
  "com.twitter" % "finagle-http" % "1.9.12",
  "com.twitter" % "json" % "2.1.4"
)
