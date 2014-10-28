import AssemblyKeys._

assemblySettings

test in assembly := {}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
 cp filter { x => 
 	x.data.getName.matches(".*spark.*")  ||
		x.data.getName.matches(".*spire.*macros.*")	
	}
}
