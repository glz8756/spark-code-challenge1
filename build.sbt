name := "spark-code-challenge"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.2.1"

//dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
//dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
//dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"


// test dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.holdenkarau" %% "spark-testing-base" % "3.2.0_1.1.1" % "test"
)

resolvers += Resolver.typesafeRepo("releases")

