
/* SETUP
 * 
 * This project depends on Flatbuffers which, unfortunately, must be manually installed.
 * To do so, download the flatbuffers source code, run "cmake ." from within the root
 * directory of the project, and then "make install" and "mvn install". This will
 * install the flatc utility as /usr/local/bin/flatc and create the flatbuffers jar file
 * in <flatbuffer_root>/target. Copy the jar file to the 'lib' folder of this project and
 * you should be good to go.
 *
 * Scala IDE project files can be generated with 'sbt eclipse'
 * 
 */

import scala.sys.process._

lazy val root = (project in file(".")).
  settings(
    name         := "aspen",
    version      := "0.1",
    scalaVersion := "2.12.3",
    organization := "com.ibm",
      
    scalacOptions ++= Seq("-feature", "-deprecation"),

    resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/",

    libraryDependencies ++= Seq(
      "org.scalatest"       %% "scalatest"      % "3.0.4" % "test",
      "com.github.blemale"  %% "scaffeine"      % "2.5.0" % "compile",
      "org.zeromq"          %  "jeromq"         % "0.4.3",
      "org.rocksdb"         %  "rocksdbjni"     % "5.5.1",
      "org.json4s"          %% "json4s-native"  % "3.6.0-M2",
      "org.json4s"          %% "json4s-jackson" % "3.6.0-M2",
      "org.xerial.snappy"   %  "snappy-java"    % "1.1.7.1",
      "net.java.dev.jna"    %  "jna"            % "4.5.0",
      "net.java.dev.jna"    %  "jna-platform"   % "4.5.0",
      "org.yaml"            %  "snakeyaml"      % "1.21",
    )
  )
  
testOptions  in Test += Tests.Argument(TestFrameworks.ScalaTest, "-W", "10", "5")

sourceGenerators in Compile += Def.task {
  val base = (sourceManaged in Compile).value
  
  // Network Protocol
  val net_out_dir = (sourceManaged in Compile).value / "com" / "ibm" / "aspen" / "core" / "network" / "protocol"

  val net_schema = file("schema") / "network_protocol.fbs"

  val net_generate = !net_out_dir.exists() || net_out_dir.listFiles().exists( f => net_schema.lastModified() > f.lastModified() )

  if (net_generate) {
    println(s"Generating Network Protocol Source Files")
    val stdout:Int = s"flatc --java -o $base schema/network_protocol.fbs".!
    println(s"Result: $stdout")  
  }
  
  // Aspen Base
  val abase_out_dir = (sourceManaged in Compile).value / "com" / "ibm" / "aspen" / "base" / "impl" / "codec"

  val abase_schema = file("schema") / "aspen_base.fbs"

  val abase_generate = !abase_out_dir.exists() || abase_out_dir.listFiles().exists( f => abase_schema.lastModified() > f.lastModified() )

  if (abase_generate) {
    println(s"Generating Aspen Base Serialization Source Files")
    val stdout:Int = s"flatc --java -o $base schema/aspen_base.fbs".!
    println(s"Result: $stdout")  
  }

  net_out_dir.listFiles().toSeq ++ abase_out_dir.listFiles().toSeq 
}.taskValue


EclipseKeys.withSource := true
EclipseKeys.withJavadoc := true
EclipseKeys.executionEnvironment := Some(EclipseExecutionEnvironment.JavaSE18)
EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.ManagedSrc


