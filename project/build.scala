//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

import sbt._
import Keys._

import com.twitter.scrooge.ScroogeSBT._

import sbtassembly.Plugin._, AssemblyKeys._

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._
import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._

import au.com.cba.omnia.humbug.HumbugSBT._

object build extends Build {
  type Sett = Def.Setting[_]

  val thermometerVersion = "0.5.2-20141114023512-17afb81"
  val ebenezerVersion    = "0.11.1-20141117235330-fe04130"
  val omnitoolVersion    = "1.3.0-20141009013528-4a9aa95"

  lazy val standardSettings: Seq[Sett] =
    Defaults.defaultSettings ++
    uniformDependencySettings ++
    uniform.docSettings("https://github.com/CommBank/maestro")

  lazy val all = Project(
    id = "all"
  , base = file(".")
  , settings =
       standardSettings
    ++ uniform.project("maestro-all", "au.com.cba.omnia.maestro")
    ++ uniform.ghsettings
    ++ Seq[Sett](
         publishArtifact := false
       , addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.0" cross CrossVersion.full)
    )
  , aggregate = Seq(core, macros, api, test, schema)
  )

  lazy val api = Project(
    id = "api"
  , base = file("maestro-api")
  , settings =
       standardSettings
    ++ uniform.project("maestro", "au.com.cba.omnia.maestro.api")
    ++ Seq[Sett](
      libraryDependencies ++= depend.hadoop() ++ depend.testing()
    )
  ).dependsOn(core)
   .dependsOn(macros)

  lazy val core = Project(
    id = "core"
  , base = file("maestro-core")
  , settings =
       standardSettings
    ++ uniformThriftSettings
    ++ uniform.project("maestro-core", "au.com.cba.omnia.maestro.core")
    ++ Seq[Sett](
      scroogeThriftSourceFolder in Test <<=
        (sourceDirectory) { _ / "test" / "thrift" / "scrooge" },
      libraryDependencies ++= Seq(
        "com.google.code.findbugs" % "jsr305"    % "2.0.3" // Needed for guava.
      , "com.google.guava"         % "guava"     % "16.0.1"
      ) ++ depend.scalaz() ++ depend.scalding() ++ depend.hadoop()
        ++ depend.shapeless() ++ depend.testing() ++ depend.time()
        ++ depend.omnia("ebenezer-hive", ebenezerVersion)
        ++ depend.omnia("edge",          "3.1.0-20141102235054-95794f6")
        ++ depend.omnia("permafrost",    "0.1.0-20141102230603-b90058b")
        ++ depend.omnia("humbug-core",   "0.3.0-20140918054014-3066286")
        ++ depend.omnia("omnitool-time", omnitoolVersion)
        ++ depend.omnia("omnitool-file", omnitoolVersion)
        ++ depend.omnia("parlour",       "1.5.0-20141110224249-7f2b947")
        ++ Seq(
          "commons-validator"  % "commons-validator" % "1.4.0",
          "org.apache.commons" % "commons-compress"  % "1.8.1",
          "org.apache.hadoop"  % "hadoop-tools"      % depend.versions.hadoop % "provided",
          "au.com.cba.omnia"  %% "thermometer-hive"  % thermometerVersion     % "test",
          "org.scalikejdbc"   %% "scalikejdbc"       % "2.1.2"                % "test",
          "org.hsqldb"         % "hsqldb"            % "1.8.0.10"             % "test",
          "com.twitter"        % "parquet-hive"      % "1.2.5-cdh4.6.0"       % "test",
          "com.twitter"        % "parquet-cascading" % "1.2.5-cdh4.6.0-p337"  % "provided"
        ),
      parallelExecution in Test := false
    )
  )

  lazy val macros = Project(
    id = "macros"
  , base = file("maestro-macros")
  , settings =
       standardSettings
    ++ uniform.project("maestro-macros", "au.com.cba.omnia.maestro.macros")
    ++ Seq[Sett](
         libraryDependencies <++= scalaVersion.apply(sv => Seq(
           "org.scala-lang"   % "scala-compiler" % sv
         , "org.scala-lang"   % "scala-reflect"  % sv
         , "org.scalamacros" %% "quasiquotes"    % "2.0.0"
         ) ++ depend.testing())
       , addCompilerPlugin("org.scalamacros" % "paradise" % "2.0.0" cross CrossVersion.full)
    )
  ).dependsOn(core)
   .dependsOn(test % "test")

  lazy val schema = Project(
    id = "schema"
  , base = file("maestro-schema")
  , settings =
       standardSettings
    ++ uniform.project("maestro-schema", "au.com.cba.omnia.maestro.schema")
    ++ uniformAssemblySettings
    ++ Seq[Sett](
          libraryDependencies <++= scalaVersion.apply(sv => Seq(
            "com.quantifind"     %% "sumac"         % "0.3.0"
          , "org.scala-lang"     %  "scala-reflect" % sv
          , "org.apache.commons" %  "commons-lang3" % "3.3.2"
          ) ++ depend.scalding() ++ depend.hadoop())
       )
    )

  lazy val example = Project(
    id = "example"
  , base = file("maestro-example")
  , settings =
       standardSettings
    ++ uniform.project("maestro-example", "au.com.cba.omnia.maestro.example")
    ++ uniformAssemblySettings
    ++ uniformThriftSettings
    ++ Seq[Sett](
         libraryDependencies ++= depend.hadoop() ++ Seq(
           "com.twitter" % "parquet-hive"      % "1.2.5-cdh4.6.0"      % "test",
           "com.twitter" % "parquet-cascading" % "1.2.5-cdh4.6.0-p337" % "provided"
         )
       , parallelExecution in Test := false
    )
  ).dependsOn(core)
   .dependsOn(macros)
   .dependsOn(api)
   .dependsOn(schema)
   .dependsOn(test % "test")

  lazy val benchmark = Project(
    id = "benchmark"
  , base = file("maestro-benchmark")
  , settings =
       standardSettings
    ++ uniform.project("maestro-benchmark", "au.com.cba.omnia.maestro.benchmark")
    ++ uniformThriftSettings
    ++ Seq[Sett](
      libraryDependencies ++= Seq(
        "com.github.axel22" %% "scalameter" % "0.4"
      ) ++ depend.testing()
    , testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")
    , parallelExecution in Test := false
    , logBuffered := false
    )
  ).dependsOn(core)
   .dependsOn(macros)
   .dependsOn(api)

  lazy val test = Project(
    id = "test"
  , base = file("maestro-test")
  , settings =
       standardSettings
    ++ uniform.project("maestro-test", "au.com.cba.omnia.maestro.test")
    ++ uniformThriftSettings
    ++ humbugSettings
    ++ Seq[Sett](
         scroogeThriftSourceFolder in Compile <<= (sourceDirectory) { _ / "main" / "thrift" / "scrooge" }
       , humbugThriftSourceFolder  in Compile <<= (sourceDirectory) { _ / "main" / "thrift" / "humbug" }
       , (humbugIsDirty in Compile) <<= (humbugIsDirty in Compile) map { (_) => true }
       , libraryDependencies ++= Seq (
           "org.specs2"               %% "specs2"                        % depend.versions.specs
         , "org.scalacheck"           %% "scalacheck"                    % depend.versions.scalacheck
         , "org.scalaz"               %% "scalaz-scalacheck-binding"     % depend.versions.scalaz
         ) ++ depend.omnia("ebenezer-test", ebenezerVersion)
           ++ depend.omnia("thermometer-hive", thermometerVersion)
           ++ depend.hadoop()
    )
  ).dependsOn(core)
}
