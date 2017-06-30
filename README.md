# Clio

Metadata Manager

## Getting Started

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

### Test the existing built docker images

`sbt testDocker`

### Build and then use docker-compose to run the docker images

`sbt docker testDocker`

### Run the server docker image

`docker run --rm -it -p 8080:8080 broadinstitute/clio-server:<version>`

### Test that the code is consistently formatted

`sbt scalafmt::test test:scalafmt::test sbt:scalafmt::test`

### Format the code with scalafmt

`sbt scalafmt test:scalafmt`

## More information

Internal documentation [here](https://broadinstitute.atlassian.net/wiki/pages/viewpage.action?pageId=114531509).
