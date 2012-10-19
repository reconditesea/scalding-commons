name := "scalding-commons"

version := "0.1.0"

organization := "com.twitter"

scalaVersion := "2.9.2"

resolvers ++= Seq(
  "sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "sonatype-releases"  at "http://oss.sonatype.org/content/repositories/releases",
  "Clojars Repository" at "http://clojars.org/repo",
  "Conjars Repository" at "http://conjars.org/repo",
  "Twitter SVN Maven" at "https://svn.twitter.biz/maven-public"
)

scalacOptions ++= Seq("-unchecked")

libraryDependencies ++= Seq(
  "backtype" % "dfs-datastores-cascading" % "1.2.1",
  "backtype" % "dfs-datastores" % "1.2.0",
  "commons-io" % "commons-io" % "2.4",
  "com.google.protobuf" % "protobuf-java" % "2.4.1",
  "com.twitter" % "util-core" % "5.3.10",
  "com.twitter" %% "algebird" % "0.1.2",
  "com.twitter" %% "scalding" % "0.8.0",
  "com.twitter" %% "chill" % "0.0.2",
  "com.twitter.elephantbird" % "elephant-bird-cascading2" % "3.0.2",
  "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.16",
  "org.apache.thrift" % "libthrift" % "0.7.0",
  "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
  "org.scala-tools.testing" % "specs_2.9.0-1" % "1.6.8" % "test"
)

parallelExecution in Test := true

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
