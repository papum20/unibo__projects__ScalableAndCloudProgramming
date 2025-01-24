ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.15"


lazy val root = (project in file("."))
    .settings(
        name := "scalable",
        assembly / mainClass := Some("Main2"),
        assembly / assemblyJarName := f"scalable_2.12-0.1.0.jar",
    )


Compile / mainClass := Some("Main2")


libraryDependencies ++= Seq(
    "com.google.cloud" % "google-cloud-nio" % "0.127.29",
    "com.google.cloud" % "google-cloud-storage" % "2.47.0",
    "org.apache.spark" % "spark-core_2.12" % "3.5.4",
    "org.apache.spark" %% "spark-sql" % "3.5.4",
)


artifactName := {
    (sv: ScalaVersion, module: ModuleID, artifact: Artifact) => f"scalable_2.12-0.1.0.jar"
}


ThisBuild / assemblyMergeStrategy := {
    case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case "application.conf"                            => MergeStrategy.concat
    case "unwanted.txt"                                => MergeStrategy.discard
    case x =>
        val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
        oldStrategy(x)
}

assembly / assemblyMergeStrategy := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _                        => MergeStrategy.first
}
