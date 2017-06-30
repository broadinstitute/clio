package org.broadinstitute.clio.sbt

object Compilation {

  /**
    * Various compiler tweaks. More info available via:
    * - http://tpolecat.github.io/2017/04/25/scalac-flags.html
    * - https://blog.threatstack.com/useful-scalac-options-for-better-scala-development-part-1
    * - sbt 'set scalacOptions in Compile += "-help"' compile
    * - sbt 'set scalacOptions in Compile += "-X"' compile
    * - sbt 'set scalacOptions in Compile += "-Y"' compile
    */
  val CompilerSettings: Seq[String] = Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-explaintypes",
    "-feature",
    "-target:jvm-1.8",
    "-unchecked",
    "-Xcheckinit",
    "-Xfatal-warnings",
    "-Xfuture",
    "-Xlint",
    "-Xmax-classfile-name",
    "200",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-extra-implicit",
    "-Ywarn-inaccessible",
    "-Ywarn-infer-any",
    "-Ywarn-nullary-override",
    "-Ywarn-nullary-unit",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused",
    "-Ywarn-unused-import",
    "-Ywarn-value-discard"
  )

  /** sbt console warnings should not be fatal. */
  val ConsoleSettings: Seq[String] = CompilerSettings filterNot Set(
    "-Xfatal-warnings",
    "-Xlint",
    "-Ywarn-unused",
    "-Ywarn-unused-import"
  )

  /**
    * Don't generate warnings for missing links.
    *
    * Since warnings are now errors, using this override until someone discovers a way to fix links.
    * http://stackoverflow.com/questions/31488335/scaladoc-2-11-6-fails-on-throws-tag-with-unable-to-find-any-member-to-link#31497874
    */
  val DocSettings: Seq[String] = Seq(
    "-no-link-warnings"
  )
}
