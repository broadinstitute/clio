# Clio

Metadata Manager

## Getting Started

Clio requires Java 8 or version 1.8 of the Java JDK to run.

We suggest [OpenJDK](https://openjdk.java.net/).

Build Clio with [sbt](https://www.scala-sbt.org/).

The `sbt` script must support version `0.13.17` to build Clio.

## Set up

Install Java 1.8.

```
tbl@wmf05-d86 ~/Broad/clio # brew search jdk | grep 8
openjdk@8
adoptopenjdk/openjdk/adoptopenjdk8
adoptopenjdk/openjdk/adoptopenjdk8-jre
adoptopenjdk/openjdk/adoptopenjdk8-openj9
adoptopenjdk/openjdk/adoptopenjdk8-openj9-jre
adoptopenjdk/openjdk/adoptopenjdk8-openj9-jre-large
adoptopenjdk/openjdk/adoptopenjdk8-openj9-large
tbl@wmf05-d86 ~/Broad/clio # brew install adoptopenjdk8
...
tbl@wmf05-d86 ~/Broad/clio #
```

Install sbt.

```
tbl@wmf05-d86 ~/Broad/clio # brew install sbt
==> Downloading https://ghcr.io/v2/homebrew/core/sbt/manifests/1.6.2
Already downloaded: /Users/tbl/Library/Caches/Homebrew/downloads/82c620e3822787903eda3ada0109c5b8c5f09f992572b21ef98a412d34319770--sbt-1.6.2.bottle_manifest.json
==> Downloading https://ghcr.io/v2/homebrew/core/sbt/blobs/sha256:40fe7bdc9663bf3e
Already downloaded: /Users/tbl/Library/Caches/Homebrew/downloads/bba98327226feadc4f486cbdfb8d040cf99e68c2761726842f428245538c07d4--sbt--1.6.2.all.bottle.tar.gz
==> Pouring sbt--1.6.2.all.bottle.tar.gz
==> Caveats
You can use $SBT_OPTS to pass additional JVM options to sbt.
Project specific options should be placed in .sbtopts in the root of your project.
Global settings should be placed in /usr/local/etc/sbtopts

Homebrew's installation does not include `sbtn`.
==> Summary
🍺  /usr/local/Cellar/sbt/1.6.2: 8 files, 3.7MB
==> Running `brew cleanup sbt`...
Disable this behaviour by setting HOMEBREW_NO_INSTALL_CLEANUP.
Hide these hints with HOMEBREW_NO_ENV_HINTS (see `man brew`).
tbl@wmf05-d86 ~/Broad/clio #
```

Export `JAVA_HOME` for `1.8`.

```
export JAVA_HOME='/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home'
```

Run `sbt` in a clone of the Clio `git` repository
to verify that your installation supports Clio.

```
tbl@wmf05-d86 ~/Broad/clio # /usr/local/bin/sbt --version
sbt version in this project: 0.13.17
sbt script version:          1.6.2
tbl@wmf05-d86 ~/Broad/clio #
```

### Build

`sbt compile`

### Run tests

`sbt test`

### Quickly run a server

`sbt clio-server/run`

### Connect to the running server

`curl localhost:8080`

### Build executable jars

`sbt assembly`

### Build docker images

`sbt docker`

### Build docker image of only clio-server

`sbt clio-server/docker`

### Run all integration tests against a local Docker image for the current commit

`sbt "it:testOnly *Docker*Spec"`

### Build and then use docker-compose to run the docker images

`sbt docker "it:testOnly *Docker*Spec"`

### Run the server docker image

`docker run --rm -it -p 8080:8080 broadinstitute/clio-server:<version>`

### Test that the code is consistently formatted

`sbt scalafmt::test test:scalafmt::test sbt:scalafmt::test`

### Format the code with scalafmt

`sbt scalafmt test:scalafmt`

## More information

Internal documentation [here](https://broadinstitute.atlassian.net/wiki/pages/viewpage.action?pageId=114531509).
