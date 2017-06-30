# Clio

Metadata Manager

## Getting Started

### Build

`sbt compile`

### Run tests

`sbt test`

### Quickly run a server

`sbt run`

### Connect to the running server

`curl localhost:8080`

### Build an executable jar

`sbt assembly`

### Build docker image

`sbt docker`

### Test the built docker image

`sbt testDocker`

### Build and then test the docker image

`sbt docker testDocker`

### Run the docker image

`docker run --rm -it -p 8080:8080 broadinstitute/clio:<version>`

### Test that the code is consistently formatted

`sbt scalafmt::test test:scalafmt::test sbt:scalafmt::test`

### Format the code with scalafmt

`sbt scalafmt test:scalafmt`

## More information

Internal documentation [here](https://broadinstitute.atlassian.net/wiki/pages/viewpage.action?pageId=114531509).
