name := "bigdf"

version := "0.2"

scalaVersion := "2.10.3"

libraryDependencies  ++= Seq(
            "org.scalanlp" % "breeze-natives_2.10" % "0.7",
	    "org.apache.commons" % "commons-math3" % "3.0",
	    "commons-io" % "commons-io" % "2.4",
	    "joda-time" % "joda-time" % "2.0",
	    "org.joda" % "joda-convert" % "1.3.1",
	    "com.quantifind" %% "sumac" % "0.3.0",
	    "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
	    "org.apache.commons" % "commons-csv" % "1.0"
)

resolvers ++= Seq(
            // other resolvers here
            // if you want to use snapshot builds (currently 0.8-SNAPSHOT), use this.
            //"Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
            "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/releases/"
)

