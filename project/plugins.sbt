// For more info on these plugins, see https://broadinstitute.atlassian.net/wiki/pages/viewpage.action?pageId=114531509

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.9.2")
libraryDependencies += "org.slf4j" % "slf4j-nop" % "1.7.25"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.4")

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.4.1")
