package xjdrBuild

import sbt._, Keys._, Defaults.sbtPluginExtra
import sbt.complete._, DefaultParsers._

object Build extends sbt.Build {

   lazy val m2Repo =
    Resolver.file("m2-local",
      Path.userHome / ".m2" / "repository")

   val test = Seq (
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7" % Test,
    libraryDependencies += "junit" % "junit" % "4.12" % Test,
    libraryDependencies +=  "com.netflix.curator" % "curator-test" % "1.3.3" % Test,
    libraryDependencies +=  "com.novocode" % "junit-interface" % "0.11" % Test,
    libraryDependencies += "org.codehaus.groovy" % "groovy-all" % "2.4.1" % Test
  )

  val core = Seq(
    libraryDependencies += "com.typesafe" % "config" % "1.3.0",
    libraryDependencies += "com.google.guava" % "guava" % "19.0"
  )

  def xjdrStandardSettings = Seq(
                     version :=  "0.1.0-SNAPSHOT",
                organization :=  "com.xjeffrose",
    scalacOptions in Compile ++= Seq("-Yno-predef", "-Yno-imports", "-Yno-adapted-args"),
    scalacOptions in console :=  Seq("-Yno-adapted-args"),
                    licenses ++= Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
                   resolvers +=  m2Repo,
                   resolvers += Resolver.mavenLocal
  )

  lazy val demoProject = Seq(
                            name :=  "demoProject",
                     description :=  "Demo to establish new workflow",
                       sbtPlugin :=  true
  )

  lazy val root = (project in file(".")).
    settings(
      xjdrStandardSettings,
      demoProject,
      core,
      test
    )
}
