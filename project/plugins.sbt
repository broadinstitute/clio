// Enable versioning with git hashes
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.9.2")
// via: https://github.com/sbt/sbt-git/tree/v0.9.2#known-issues
// (Version doesn't have to match build.sbt, but would be nice to keep in sync.)
libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.25"

// Enable building executable jars
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.4")

// Enable building docker images
addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.4.1")
