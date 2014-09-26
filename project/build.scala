import sbt._
import Keys._

// object BuildSettings {
//   val paradiseVersion = "2.0.1"
//   val buildSettings = Defaults.defaultSettings ++ Seq(
//     organization := "org.scalamacros",
//     version := "1.0.0",
//     scalacOptions ++= Seq(),
//     scalaVersion := "2.11.2",
//     crossScalaVersions := Seq("2.10.2", "2.10.3", "2.10.4", "2.11.0", "2.11.1", "2.11.2"),
//     resolvers += Resolver.sonatypeRepo("snapshots"),
//     resolvers += Resolver.sonatypeRepo("releases"),
//     addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)
//   )
// }

object BigDfBuild extends Build {
 // import BuildSettings._
 val paradiseVersion = "2.10"
  lazy val root = Project(id = "root", base = file(".")).settings(
		name := "bigdf",
		version := "0.2",
		scalaVersion := "2.10.3", 
    libraryDependencies  ++= Seq(
            "org.scalanlp" % "breeze-natives_2.10" % "0.7",
            "org.apache.commons" % "commons-math3" % "3.0",
            "commons-io" % "commons-io" % "2.4",
            "joda-time" % "joda-time" % "2.0",
            "org.joda" % "joda-convert" % "1.3.1",
            "com.quantifind" %% "sumac" % "0.3.0",
            "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
            "org.apache.commons" % "commons-csv" % "1.0"
    ),   
    resolvers ++= Seq(
                // other resolvers here
                // if you want to use snapshot builds (currently 0.8-SNAPSHOT), use this.
                "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
                "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
    )
  ) dependsOn(macroSub)

  lazy val macroSub = Project("macro", file("macro")).settings(
    organization := "org.scalamacros",
    version := "1.0.0",
    scalacOptions ++= Seq(),
    // NOTE: everything except macros is compiled with vanilla scalac 2.10
    scalaVersion := "2.10.3-SNAPSHOT",
    resolvers ++= Seq("Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"),
    scalaOrganization := "org.scala-lang.macro-paradise",
    libraryDependencies <+= (scalaVersion)("org.scala-lang.macro-paradise" % "scala-reflect" % _)
  )

}

// object MacroBuild extends Build {
//    lazy val main = Project("main", file(".")) dependsOn(macroSub)

// }

