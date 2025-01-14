ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
    .settings(
        name := "scalable"
    )


Compile / mainClass := Some("Main")


libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.12" % "3.5.4",
)


artifactName := {
    (sv: ScalaVersion, module: ModuleID, artifact: Artifact) => f"scalable_2.12-0.1.0.jar"
}

