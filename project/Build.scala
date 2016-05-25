import sbt.Keys._
import sbt._

object Chicago extends Build {
  val branch = Process("git" :: "rev-parse" :: "--abbrev-ref" :: "HEAD" :: Nil).!!.trim
  val suffix = if (branch == "master") "" else "-SNAPSHOT"

  val libVersion = "0.3.0" + suffix
  val jacksonVersion = "2.7.3"
  val jacksonLibs = Seq(
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
  )

  val test = Seq (
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7" % Test,
    libraryDependencies += "junit" % "junit" % "4.12" % Test,
    libraryDependencies +=  "com.netflix.curator" % "curator-test" % "1.3.3" % Test,
    libraryDependencies +=  "com.novocode" % "junit-interface" % "0.11" % Test,
    libraryDependencies += "org.codehaus.groovy" % "groovy-all" % "2.4.1" % Test
  )

  val core = Seq(
    libraryDependencies += "com.typesafe" % "config" % "1.3.0",
    libraryDependencies +="com.xjeffrose" % "xio" % "0.10.0-SNAPSHOT",
    libraryDependencies +="org.apache.curator" % "curator-framework" % "2.9.1",
    libraryDependencies +="org.apache.curator" % "curator-recipes" % "2.9.1"
  )

  lazy val m2Repo =
    Resolver.file("m2-local",
      Path.userHome / ".m2" / "repository")

  val sharedSettings = Seq(
    version := libVersion,
    organization := "com.xjeffrose",
    resolvers += m2Repo,
    resolvers += Resolver.mavenLocal,
    javaOptions in Test := Seq("-DSKIP_FLAKY=1"),

    ivyXML :=
      <dependencies>
        <exclude org="com.sun.jmx" module="jmxri"/>
        <exclude org="com.sun.jdmk" module="jmxtools"/>
        <exclude org="javax.jms" module="jms"/>
      </dependencies>,

    scalacOptions ++= Seq("-encoding", "utf8"),
    scalacOptions += "-deprecation",
    scalacOptions += "-language:_",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javacOptions in doc := Seq("-source", "1.8"),

    // This is bad news for things like com.twitter.util.Time
    parallelExecution in Test := false,

    resourceGenerators in Compile <+=
      (resourceManaged in Compile, name, version) map { (dir, name, ver) =>
        val file = dir / "com" / "xjeffrose" / name / "build.properties"
        val buildRev = Process("git" :: "rev-parse" :: "HEAD" :: Nil).!!.trim
        val buildName = new java.text.SimpleDateFormat("yyyyMMdd-HHmmss").format(new java.util.Date)
        val contents = (
          "name=%s\nversion=%s\nbuild_revision=%s\nbuild_name=%s"
          ).format(name, ver, buildRev, buildName)
        IO.write(file, contents)
        Seq(file)
      }
  )

  lazy val chicago = (project in file(".")).
    settings(
      sharedSettings,
      core,
      jacksonLibs,
      test
    )
}
