#!/bin/bash

# turn on verbose
set -ex

# program
PROG_NAME=$(basename $0)
CLIO_DIR=$(cd $(dirname $(dirname $0)) && pwd)

# Run this script on Jenkins to deploy Clio to a host.

# Steps:
#   Render clio-server ctmpls into configs for the given environment
#   Copy configs to node
#   Stop any currently running container
#   Re-start container using new configs
#   Verify that the new container reports the correct version

# The script expects the following ENV vars set to non-null values
#   ENV: environment to deploy to, one of "dev", "staging", or "prod"

# verify required vars are set to actual values
if [ -z "$ENV" ]
then
  >&2 echo "Error: ENV not set!!"
  exit 1
else
  case "$ENV" in
    "dev"|"staging"|"prod")
      ;;
    *)
      >&2 echo "Error: Invalid environment '$ENV'!!"
      exit 1
      ;;
  esac
fi

CONFIG_DIR="${CLIO_DIR}/clio-server/config"

if [ ! -d "$CONFIG_DIR" ]
then
  >&2 echo "Missing configuration directory ($CONFIG_DIR) - Exiting "
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
  >&2 echo "Missing VAULT TOKEN file - Exiting"
  exit 1
fi

# compute the tag of the Docker image to deploy based on the current commit
# assumes the image for the current commit has already been built & pushed
BASE_VERSION=$(cat "${CLIO_DIR}/.clio-version" | tr -d '\n')
SHORT_SHA=$(git rev-parse HEAD | cut -c1-7)
DOCKER_TAG="${BASE_VERSION}-g${SHORT_SHA}-SNAP"

# initialize vars used in multiple config templates
# location on host where configs should get copied
APP_DIR=${APP_DIR:-"/app"}
# location in the clio container where configs will be mounted
CLIO_CONF_DIR=/etc
# location on the clio host where logs will be stored
HOST_LOG_DIR=/local/clio_logs
# location in the clio container where logs will be mounted
CLIO_LOG_DIR=/logs
# relative path within the log dir where the fluentd pos logfile should be stored
POS_LOG_DIR=pos
# name of the application config used by the clio instance
CLIO_APP_CONF=clio.conf
# name of the logback config used by the clio instance
CLIO_LOGBACK_CONF=clio-logback.xml
# port to expose for clio in the clio container
CONTAINER_CLIO_PORT=8080

# SSH options
CLIO_HOST="clio101.gotc-${ENV}.broadinstitute.org"
SSH_USER=jenkins
SSHOPTS="-o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no"
SSHCMD="ssh $SSHOPTS ${SSH_USER}@${CLIO_HOST}"
SCPCMD="scp $SSHOPTS"

# create a temp dir to store rendered configs
TMPDIR=$(mktemp -d ${CLIO_DIR}/${PROG_NAME}-XXXXXX)
CTMPL_ENV_FILE="${TMPDIR}/env-vars.txt"

# populate temp env.txt file with necessary environment for rendering configs
for var in ENV CLIO_HOST DOCKER_TAG APP_DIR CLIO_CONF_DIR HOST_LOG_DIR CLIO_LOG_DIR POS_LOG_DIR CLIO_APP_CONF CLIO_LOGBACK_CONF CONTAINER_CLIO_PORT; do
  echo "${var}=${!var}" >> ${CTMPL_ENV_FILE}
done

# copy configs to temp dir
# the render-ctmpl script will replace them with their rendered variants
cp -r ${CONFIG_DIR}/* ${TMPDIR}/

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
# rm -rf /app.new and /app.old.old
# mkdir /app.new
${SSHCMD} "sudo rm -rf ${APP_DIR}.new && sudo rm -rf ${APP_DIR}.old.old && sudo mkdir ${APP_DIR}.new && sudo chgrp ${SSH_USER} ${APP_DIR}.new && sudo chmod g+w ${APP_DIR}.new"

# copy configs to /app.new
${SCPCMD} -r ${TMPDIR}/* ${SSH_USER}@${CLIO_HOST}:${APP_DIR}.new/

# stop running the existing container
stopcontainer

# move /app.old to /app.old.old
# move /app to /app.old
# move /app.new to /app
# create pos log directory if it doesn't already exist
${SSHCMD} <<-EOF
  ([ ! -d ${APP_DIR}.old ] || sudo mv ${APP_DIR}.old ${APP_DIR}.old.old) &&
  ([ ! -d ${APP_DIR} ] || sudo mv ${APP_DIR} ${APP_DIR}.old) &&
  sudo mv ${APP_DIR}.new ${APP_DIR} &&
  sudo mkdir -p ${HOST_LOG_DIR}/${POS_LOG_DIR}
EOF

# start running the new container
startcontainer

STATUS=
REPORTED_VERSION=

# try 3 times at 20 second intervals
attempts=1

while [ -z "$STATUS" ] && [ ${attempts} -le 3 ]
do
  sleep 20
  echo "Attempt $attempts"
  STATUS=$(curl -fs https://"${CLIO_HOST}/health" | jq -r .search)
  REPORTED_VERSION=$(curl -fs https://"${CLIO_HOST}/version" | jq -r .version)
  attempts=$((attempts + 1))
done

if [[ "$STATUS" == "OK" && "$REPORTED_VERSION" == "$DOCKER_TAG" ]]; then
  # rm /app.old.old
  ${SSHCMD} "sudo rm -rf ${APP_DIR}.old.old"

  # tag the current commit with the name of the environment that was just deployed to
  # this is a floating tag, so we have to use -f
  #
  # NOTE: we can't push the tag here because Jenkins has to run this script with its
  # GCE SSH key, which doesn't match its GitHub key. We push the tag as a follow-up
  # step using the git publisher.
  git tag -f "$ENV"

  exit 0
else
  >&2 echo "Error: Clio failed to report expected health and version, rolling back deploy!!"

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

  exit 1
fi
