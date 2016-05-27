// libraryDependencies += "com.xjeffrose" % "xio" % "0.10.0-SNAPSHOT"
// libraryDependencies += "rocksdb" % "org.rocksdb" % "1.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "org.apache.curator" % "curator-framework" % "2.9.1" exclude("log4j", "log4j")
libraryDependencies += "org.apache.curator" % "curator-recipes" % "2.9.1" exclude("log4j", "log4j")
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.7.3"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.7.3"
// Testing
libraryDependencies += "junit" % "junit" % "4.12" % Test
libraryDependencies += "com.netflix.curator" % "curator-test" % "1.3.3" % Test
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test
libraryDependencies += "org.codehaus.groovy" % "groovy-all" % "2.4.1" % Test
// http://mvnrepository.com/artifact/org.slf4j/log4j-over-slf4j
libraryDependencies += "org.slf4j" % "log4j-over-slf4j" % "1.7.21"
// http://mvnrepository.com/artifact/ch.qos.logback/logback-classic
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7" % Test


lazy val Serial = config("serial") extend(Test)

parallelExecution in Serial := false

parallelExecution := false

fork := false