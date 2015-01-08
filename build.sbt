import SonatypeKeys._

// Import default settings. This changes `publishTo` settings to use the Sonatype repository and add several commands for publishing.
sonatypeSettings

//todo maybe remove release
releaseSettings

name := "EnronGraph"

organization := "net.walend"

// Project version. Only release version (w/o SNAPSHOT suffix) can be promoted.
version := "0.0.0"

isSnapshot := true

scalaVersion := "2.11.4"

//javaHome := Some(new File("/Library/Java/JavaVirtualMachines/jdk1.8.0_05.jdk/Contents/Home"))

resolvers += "Sonatype releases" at "http://oss.sonatype.org/content/repositories/releases/"

libraryDependencies += "net.walend" %% "scalagraphminimizer" % "0.1.1"

libraryDependencies += "com.h2database" % "h2" % "1.4.184"

libraryDependencies ++= Seq(
  "com.typesafe.slick" %% "slick" % "2.1.0",
  "org.slf4j" % "slf4j-nop" % "1.6.4"
)



fork in test := true

javaOptions in test += "-server" //does hotspot optimizations earlier

fork in run := true

javaOptions in run += "-server" //does hotspot optimizations earlier

javaOptions in run += "-Xmx4G" //default is at 1 GB for contemporary hardware

scalacOptions ++= Seq("-unchecked", "-deprecation","-feature")

// Your project orgnization (package name)
organization := "net.walend.enron"

// Your profile name of the sonatype account. The default is the same with the organization
profileName := "net.walend"

// To sync with Maven central, you need to supply the following information:
// TODO
//pomExtra := {
//  <url>https://github.com/dwalend/ScalaGraphMinimizer</url>
//    <licenses>
//      <license>
//        <name>MIT License</name>
//        <url>http://www.opensource.org/licenses/mit-license.php</url>
//      </license>
//    </licenses>
//    <scm>
//      <connection>scm:git:github.com:dwalend/ScalaGraphMinimizer.git</connection>
//      <url>github.com/dwalend/ScalaGraphMinimizer.git</url>
//    </scm>
//    <developers>
//      <developer>
//        <id>dwalend</id>
//        <name>David Walend</name>
//        <url>https://github.com/dwalend</url>
//      </developer>
//    </developers>
//}
