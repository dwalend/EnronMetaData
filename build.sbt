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

scalaVersion := "2.11.2"

resolvers += "Sonatype releases" at "http://oss.sonatype.org/content/repositories/releases/"

//libraryDependencies += "com.github.verbalexpressions" %% "ScalaVerbalExpression" % "1.0.1" % "test" //for loading the Enron graph

libraryDependencies += "org.parboiled" %% "parboiled" % "2.0.1"

libraryDependencies += "net.walend" %% "scalagraphminimizer" % "0.1.1"

fork in test := true

javaOptions in test += "-server" //does hotspot optimizations earlier

fork in run := true

javaOptions in run += "-server" //does hotspot optimizations earlier

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
