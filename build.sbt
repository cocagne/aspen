
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

lazy val root = (project in file(".")).
  settings(
    name         := "aspen",
    version      := "0.1",
    scalaVersion := "2.11.8",
    organization := "com.ibm",
      
    scalacOptions ++= Seq("-feature", "-deprecation"),

    resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/",

    libraryDependencies ++= Seq(
      "org.scalatest"       %% "scalatest"      % "3.0.1" % "test",
      "org.zeromq"          % "jeromq"          % "0.4.1",
      "org.rocksdb"         % "rocksdbjni"      % "5.5.1"
    )
  )

sourceGenerators in Compile += Def.task {
  val base = (sourceManaged in Compile).value
  val out_dir = (sourceManaged in Compile).value / "com" / "ibm" / "aspen" / "core" / "network" / "protocol"

  val schema = file("schema") / "protocol.fbs"

  val generate = !out_dir.exists() || out_dir.listFiles().exists( f => schema.lastModified() > f.lastModified() )

  if (generate) {
    println(s"Generating Files")
    val stdout = s"flatc --java -o $base schema/protocol.fbs".!
    println(s"Result: $stdout")  
  }
  
  out_dir.listFiles().toSeq
}.taskValue


EclipseKeys.withSource := true
EclipseKeys.withJavadoc := true
EclipseKeys.executionEnvironment := Some(EclipseExecutionEnvironment.JavaSE18)
EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.ManagedSrc


