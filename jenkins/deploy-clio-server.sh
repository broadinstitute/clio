#!/bin/sh

# turn on verbose
set -ex

# program
PROG_NAME=$(basename $0)
CLIO_DIR=$(cd $(dirname $(dirname $0)) && pwd)

# Run this script on Jenkins to deploy Clio to a host.

# Steps:
#   Build and push a docker image for clio-server
#   Gather configurations associated with app to deploy
#   Render any ctmpls
#   Copy configs to node
#   Stop any currently running container
#   Re-start container using new configs
#   Verify that the new container reports the correct version

# The script expects the following ENV vars set to non-null values
#   ENV: environment to deploy to, one of "dev", "staging", or "prod"
#   CLIO_HOST: hostname to deploy clio to

# The script also supports the optional ENV vars
#  DOCKER_TAG: docker tag to apply to the built and deployed instance
#              if not specified, the default SNAP version is used
#  RUN_TESTS: if set to "false", unit tests will not be run as part of
#             building the docker image

# verify required vars are set to actual values
if [ -z "$ENV" ]
then
   echo "Error: ENV not set!!"
   exit 2
fi

if [ -z "$CLIO_HOST" ]
then
   echo "Error: CLIO_HOST not set!!"
   exit 2
fi

CONFIG_DIR=clio-server/config

if [ ! -d "$CONFIG_DIR" ]
then
    echo "Missing configuration directory ($CONFIG_DIR) - Exiting "
    exit 1
fi

# set VAULT token location
if [ -f /etc/vault-token-dsde ]
then
   TOKEN_FILE="/etc/vault-token-dsde"
elif [ -f ${HOME}/.vault-token ]
then
   TOKEN_FILE="${HOME}/.vault-token"
else
   echo "Missing VAULT TOKEN file - Exiting"
   exit 1
fi
   
# initialize vars used in multiple config templates
# location on host where configs should get copied
APP_DIR=${APP_DIR:-"/app"}
# location in the clio container where configs will be mounted
CLIO_CONF_DIR=/etc
# location in the clio container where logs will be mounted
CLIO_LOG_DIR=/logs
# name of the application config used by the clio instance
CLIO_APP_CONF=clio.conf
# name of the logback config used by the clio instance
CLIO_LOGBACK_CONF=clio-logback.xml
# name of the env file used to set up clio with docker-compose
CLIO_ENV_FILE=clio.env
# port to expose for clio on the host VM
HOST_CLIO_PORT=80
# port to expose for clio in the clio container
CONTAINER_CLIO_PORT=${HOST_CLIO_PORT}

# SSH options
SSH_USER=jenkins
SSHOPTS="-o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no"
SSHCMD="ssh $SSHOPTS ${SSH_USER}@${CLIO_HOST}"
SCPCMD="scp $SSHOPTS"

# sbt command to disable tests in the docker build
if [[ "$RUN_TESTS" = "false" ]]; then
    SET_TESTS="set test in assembly in LocalProject(\"clio-server\") := {}"
fi
# sbt command to override the default docker tag
if [[ ! -z "$DOCKER_TAG" ]]; then
    SET_VERSION="set version := \"$DOCKER_TAG\""
fi

# build the clio-server docker image
docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v ${CLIO_DIR}:/clio \
    -w="/clio" \
    broadinstitute/scala:scala-2.11.8 sbt "$SET_TESTS" "$SET_VERSION" "clio-server/docker"

# if no tag was given, read the default tag from the generated resource file
if [[ -z "$DOCKER_TAG" ]]; then
    DOCKER_TAG=$(cat ${CLIO_DIR}/clio-server/target/scala-2.12/resource_managed/main/clio-server-version.conf | perl -pe 's/clio\.server\.version:\s+//g')
fi

# push the docker image
docker push broadinstitute/clio-server:${DOCKER_TAG}

# TMPDIR
TMPDIR=$(mktemp -d ${CLIO_DIR}/${PROG_NAME}-XXXXXX)
# temporary env file for rendering ctmpls
CTMPL_ENV_FILE="${TMPDIR}/env-vars.txt"

# populate temp env.txt file with necessary environment
for var in "ENV" "DOCKER_TAG" "APP_DIR" "CLIO_CONF_DIR" "CLIO_LOG_DIR" "CLIO_APP_CONF" "CLIO_LOGBACK_CONF" "CLIO_ENV_FILE" "HOST_CLIO_PORT" "CONTAINER_CLIO_PORT"; do
    echo "${var}=${!var}" >> ${CTMPL_ENV_FILE}
done

# copy configs to temp dir
cp ${CONFIG_DIR}/* ${TMPDIR}/

# render all ctmpls
docker run --rm \
    -v ${TMPDIR}:/working \
    -v ${CLIO_DIR}/jenkins:/scripts \
    -v ${TOKEN_FILE}:/root/.vault-token:ro \
    --env-file="${CTMPL_ENV_FILE}" \
    broadinstitute/dsde-toolbox:latest /scripts/render-ctmpl.sh
rm -f ${CTMPL_ENV_FILE}

stopcontainer() {
    # stop running container on host
    ${SSHCMD} "test -f ${APP_DIR}/docker-compose.yml && docker-compose -p clio-server -f ${APP_DIR}/docker-compose.yml stop || echo"

    # docker-compose rm -f
    ${SSHCMD} "test -f ${APP_DIR}/docker-compose.yml && docker-compose -p clio-server -f ${APP_DIR}/docker-compose.yml rm -f || echo"
}

startcontainer() {
    # docker-compose pull
    ${SSHCMD} "docker-compose -p clio-server -f ${APP_DIR}/docker-compose.yml pull"

    # docker-compose up -d
    ${SSHCMD} "docker-compose -p clio-server -f ${APP_DIR}/docker-compose.yml up -d"
}

# copy configs to temp directory on host
# rm -rf /app.new
# mkdir /app.new
${SSHCMD} "sudo rm -rf ${APP_DIR}.new && sudo mkdir ${APP_DIR}.new && sudo chgrp ${SSH_USER} ${APP_DIR}.new && sudo chmod g+w ${APP_DIR}.new"

# copy configs to /app.new
${SCPCMD} -r ${TMPDIR}/* ${SSH_USER}@${CLIO_HOST}:${APP_DIR}.new/

# stop running the existing container
stopcontainer

# move /app.old to /app.old.old
# move /app to /app.old
# move /app.new to /app
${SSHCMD} <<-EOF
    ([ ! -d ${APP_DIR}.old ] || sudo mv ${APP_DIR}.old ${APP_DIR}.old.old) &&
    ([ ! -d ${APP_DIR} ] || sudo mv ${APP_DIR} ${APP_DIR}.old) &&
    sudo mv ${APP_DIR}.new ${APP_DIR}
EOF

# start running the new container
startcontainer

# wait for the server to come up before checking its health
sleep 10

# hit the health and version endpoints to check the deployment status
STATUS=$(curl -s "${CLIO_HOST}:${HOST_CLIO_PORT}/health" | python -c "import sys, json; print json.load(sys.stdin)['search']" || echo)
VERSION=$(curl -s "${CLIO_HOST}:${HOST_CLIO_PORT}/version" | python -c "import sys, json; print json.load(sys.stdin)['version']" || echo)

if [[ "$STATUS" == "OK" && "$VERSION" == "$DOCKER_TAG" ]]; then
    # rm /app.old.old
    ${SSHCMD} "sudo rm -rf ${APP_DIR}.old.old"
    exit 0
else
    # roll back the deployment
    # stop running the new container
    stopcontainer

    # rm /app
    # move /app.old to /app
    # move /app.old.old to /app.old
    ${SSHCMD} <<-EOF
        sudo rm -rf ${APP_DIR} &&
        ([ ! -d ${APP_DIR}.old ] || sudo mv ${APP_DIR}.old ${APP_DIR}) &&
        ([ ! -d ${APP_DIR}.old.old ] || sudo mv ${APP_DIR}.old.old ${APP_DIR}.old)
EOF

    # start running the old container
    startcontainer

    exit 2
fi
