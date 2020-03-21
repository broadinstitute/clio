#!/usr/bin/env bash

set -e

declare -r PROG_NAME=$(basename $0)
declare -r CLIO_DIR=$(dirname $(cd $(dirname $0) && pwd))
declare -r CTMPL_DIR=${CLIO_DIR}/clio-server/config

declare -r -a VAULT_TOKEN_FILE_OPTIONS=(/etc/vault-token-dsde ${HOME}/.vault-token)

# Location on host where configs will be copied.
declare -r APP_DIR=/app
declare -r APP_STAGING_DIR=${APP_DIR}.new
declare -r APP_BACKUP_DIR=${APP_DIR}.old
declare -r APP_BACKUP_BACKUP_DIR=${APP_BACKUP_DIR}.old

# Location on host where logs will be stored.
declare -r HOST_LOG_DIR=/local/clio_logs
# Relative path within the log dir where the fluentd pos logfile will be stored.
declare -r POS_LOG_DIR=pos

declare -r SSH_USER=jenkins
declare -r -a SSH_OPTS=(-o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no)

# Expected path on the host where the docker-compose file will be placed.
declare -r COMPOSE_FILE=${APP_DIR}/docker-compose.yml
declare -r -a DOCKER_COMPOSE=(docker-compose -p clio-server -f ${COMPOSE_FILE})

# Shamelessly ripped from StackOverflow: https://stackoverflow.com/a/17841619
join_by() { local d=$1; shift; echo -n "$1"; shift; printf "%s" "${@/#/$d}"; }

usage() {
  cat <<EOF >&2
USAGE: ENV=<env> CLIO_HOST_NAME=<hostname> $PROG_NAME
Run this script on Jenkins to deploy Clio to a host in an environment.
Steps:
  1. Render clio-server ctmpls into configs for the given environment
  2. Copy rendered configs to the host
  3. Stop any currently-running clio container network on the host
  4. Start a new network using the new configs and docker image
  5. Verify that the new Clio server reports OK health and the expected version
  6. Roll back the deploy if the new server fails to report its health or version

Note: requires the following ENV vars to be set to non-null values:

  ENV:            Environment to deploy to, one of "dev", "staging", or "prod"
  CLIO_HOST_NAME: Name of the host within the network specified by 'ENV' to deploy to,
                  for example: "clio101", "clio201", "clio3", "clio".

The server will be deployed to the host at '<CLIO_HOST_NAME>.gotc-<ENV>.broadinstitute.org'

Also requires that a file containing a vault token be placed at one of the following paths:

  $(join_by $'\n  ' ${VAULT_TOKEN_FILE_OPTIONS[@]})
EOF
}

check_usage() {
  local ret=0

  if [ -z ${ENV} ]; then
    >&2 echo Error: ENV not set!
    ret=1
  else
    case ${ENV} in
      dev|staging|prod)
        ;;
      *)
        >&2 echo Error: Invalid environment ${ENV}!
        ret=1
        ;;
    esac
  fi

  if [ -z ${CLIO_HOST_NAME} ]; then
    >&2 echo Error: CLIO_HOST_NAME not set!
    ret=1
  fi

  if [ ! -d ${CTMPL_DIR} ]; then
    >&2 echo Error: Missing ctmpl source directory ${CTMPL_DIR} - Was it accidentally deleted?
    ret=1
  fi

  if [ ${ret} -ne 0 ]; then
    usage
    exit 1
  fi
}

find_vault_token() {
  local vault_token_file

  for token_location in ${VAULT_TOKEN_FILE_OPTIONS[@]}; do
    if [ -f ${token_location} ]; then
      vault_token_file=$token_location
    fi
  done

  if [ -z ${vault_token_file} ]; then
    >&2 echo Error: Vault token file not found at any of $(join_by ', ' ${VAULT_TOKEN_FILE_OPTIONS[@]})
    usage
    exit 1
  else
    echo ${vault_token_file}
  fi
}

# Build a "conventional" fully-qualified domain name from a hostname and environment.
build_fqdn() {
  echo ${1}.gotc-${ENV}.broadinstitute.org
}

# Copy ctmpls to a destination directory, then render them into configs.
# Rendering has the side-effect of deleting the copied ctmpl files.
render_ctmpls() {
  local -r clio_fqdn=$1 docker_tag=$2 config_dir=$3
  local -r vault_token_file=$(find_vault_token)

  local -A env_map
  env_map[ENV]=${ENV}
  env_map[APP_DIR]=${APP_DIR}
  env_map[HOST_LOG_DIR]=${HOST_LOG_DIR}
  env_map[POS_LOG_DIR]=${POS_LOG_DIR}
  env_map[DOCKER_TAG]=${docker_tag}
  env_map[CLIO_FQDN]=${clio_fqdn}
  # Pull the most-significant digit from the clio node's ID,
  # for building URIs to the nodes of the corresponding ES cluster.
  env_map[CLUSTER_NUM]=$(echo ${clio_fqdn} | sed -E "s/clio([0-9]).*/\1/")
  # Location in the containers where configs will be mounted.
  env_map[CLIO_CONF_DIR]=/etc
  # Location in the Clio container where logs will be mounted.
  env_map[CLIO_LOG_DIR]=/logs
  # Name of the application config used by the Clio instance.
  env_map[CLIO_APP_CONF]=clio.conf
  # Name of the logback config used by the Clio instance.
  env_map[CLIO_LOGBACK_CONF]=clio-logback.xml
  # Name of the service account JSON file used by the Clio instance.
  env_map[CLIO_SERVICE_ACCOUNT_JSON]=clio-service-account.json
  # Port to expose for Clio in the container.
  env_map[CONTAINER_CLIO_PORT]=8080
  # JProfiler config
  env_map[CONTAINER_JPROFILER_CONF]=clio-profile.xml

  # Populate an env file with variables for rendering configs.
  local -r ctmpl_env_file=${config_dir}/env-vars.txt
  for key in ${!env_map[@]}; do
    echo ${key}=${env_map[$key]} >> ${ctmpl_env_file}
  done

  cp -r ${CTMPL_DIR}/* ${config_dir}/
  docker run --rm \
    -v ${config_dir}:/working \
    -v ${CLIO_DIR}/jenkins:/scripts \
    -v ${vault_token_file}:/root/.vault-token:ro \
    --env-file=${ctmpl_env_file} \
    broadinstitute/dsde-toolbox:latest /scripts/render-ctmpl.sh

  rm ${ctmpl_env_file}
}

rollback_deploy() {
  local -r clio_fqdn=$1

  stop_containers ${clio_fqdn}
  ssh ${SSH_OPTS[@]} ${SSH_USER}@${clio_fqdn} <<-EOF
  sudo rm -r ${APP_DIR} &&
  ([ ! -d ${APP_BACKUP_DIR} ] || sudo mv ${APP_BACKUP_DIR} ${APP_DIR}) &&
  ([ ! -d ${APP_BACKUP_BACKUP_DIR} ] || sudo mv ${APP_BACKUP_BACKUP_DIR} ${APP_BACKUP_DIR})
EOF
  start_containers ${clio_fqdn}
}

# Poll the Clio server every 20 seconds for 5 minutes (15 times).
poll_clio_health() {
  local -r clio_fqdn=$1 docker_tag=$2

  local status
  local reported_version
  local attempts=1

  while [ ${attempts} -le 15 ]; do
    sleep 20
    echo Checking Clio status...
    status=$(curl -fs https://${clio_fqdn}/health | jq -r .clio)
    reported_version=$(curl -fs https://${clio_fqdn}/version | jq -r .version)

    echo Got status: ${status}, version: ${reported_version}
    case ${status} in
      Recovering)
        ;;
      Started)
        if [ "${reported_version}" = ${docker_tag} ]; then
          return 0
        else
          return 1
        fi
        ;;
      *)
        attempts=$((attempts + 1))
        ;;
    esac
  done

  return 1
}

activate_service_account() {
  VAULT_TOKEN=$(cat /etc/vault-token-dsde)
  SA_VAULT_PATH="secret/dsde/gotc/dev/common/ci-deployer-service-account.json"

  docker run --rm -e VAULT_TOKEN="${VAULT_TOKEN}" \
    broadinstitute/dsde-toolbox:dev vault read -format=json ${SA_VAULT_PATH} \
    | jq '.data' > service-account.json

  # activate SA
  gcloud auth activate-service-account --key-file=service-account.json
}

get_clio_instance() {
  CLIO_PROJECT=$1

  # Get the instance marked with the Clio and Active labels (There should only be one)
  CLIO_INSTANCE_LIST=$(gcloud --format="table[no-heading](Name)" compute \
                         --project ${CLIO_PROJECT} instances list \
                         --filter="labels.app=clio")

  # Verify that there is only one instance
  if [[ ${#CLIO_INSTANCE_LIST[@]} -ne 1 ]];
  then
    exit 1
  fi
}

get_clio_fqdn() {
  CLIO_PROJECT=$1
  CLIO_INSTANCE=$2

  local -r gce_name=$(gcloud compute --project ${CLIO_PROJECT} \
                                     ssh ${CLIO_INSTANCE} \
                                     --zone=us-central1-a \
                                     --command="uname -n")
  local -r clio_cluster_number_long=$(echo "${gce_name}" | sed -E "s/clio-([0-9]{3})-([0-9]{2})/\1/")
  local -r clio_cluster_number=${clio_cluster_number_long:0:1}
  local -r instance_number=$(echo "${gce_name}" | sed -E "s/clio-([0-9]{3})-([0-9]{2})/\2/")
  local -r clio_fqdn=$(build_fqdn "clio${clio_cluster_number}${instance_number}")
  echo "${clio_fqdn}"
}

stop_clio_containers() {
  CLIO_PROJECT=$1
  CLIO_INSTANCE=$2

  gcloud compute --project ${CLIO_PROJECT} \
                 ssh ${CLIO_INSTANCE} \
                 --zone=us-central1-a \
                 --command="test -f ${COMPOSE_FILE} &&
                              ${DOCKER_COMPOSE[*]} stop || echo &&
                            test -f ${COMPOSE_FILE} &&
                              ${DOCKER_COMPOSE[*]} rm -f || echo"
}

start_clio_containers() {
  CLIO_PROJECT=$1
  CLIO_INSTANCE=$2

  gcloud compute --project ${CLIO_PROJECT} \
                 ssh ${CLIO_INSTANCE} \
                 --zone=us-central1-a \
                 --command="${DOCKER_COMPOSE[*]} pull &&
                            ${DOCKER_COMPOSE[*]} up -d"
}

deploy_clio_containers() {
  CLIO_PROJECT=$1
  CLIO_INSTANCE=$2

  local -r clio_fqdn=$1 config_src=$2

  # Clean up any leftover state from previous failed deploys.
  gcloud compute --project ${CLIO_PROJECT} \
                 ssh ${CLIO_INSTANCE} \
                 --zone=us-central1-a \
                 --command="sudo rm -rf ${APP_STAGING_DIR} ${APP_BACKUP_BACKUP_DIR} &&
                            sudo mkdir ${APP_STAGING_DIR} &&
                            sudo chgrp ${SSH_USER} ${APP_STAGING_DIR} &&
                            sudo chmod g+w ${APP_STAGING_DIR}"

  # Copy rendered configs to the staging directory.
  gcloud compute --project ${CLIO_PROJECT} \
                 scp -r \
                 --zone=us-central1-a \
                 ${config_src}/* \
                 ${CLIO_INSTANCE}:/${APP_STAGING_DIR}

  # Stop anything that's running.
  stop_clio_containers ${CLIO_PROJECT} ${CLIO_INSTANCE}

  # Create backups, in case we need to roll back.
  gcloud compute --project ${CLIO_PROJECT} \
                 ssh ${CLIO_INSTANCE} \
                 --zone=us-central1-a \
                 --command="([ ! -d ${APP_BACKUP_DIR} ] || sudo mv ${APP_BACKUP_DIR} ${APP_BACKUP_BACKUP_DIR}) &&
                            ([ ! -d ${APP_DIR} ] || sudo mv ${APP_DIR} ${APP_BACKUP_DIR}) &&
                            sudo mv ${APP_STAGING_DIR} ${APP_DIR} &&
                            sudo mkdir -p ${HOST_LOG_DIR}/${POS_LOG_DIR}"

  # Start up the new containers.
  start_clio_containers ${CLIO_PROJECT} ${CLIO_INSTANCE}
}

main() {
  check_usage

  local -r docker_tag=$(git rev-parse HEAD)
#  clio_host_fqdn="$(build_fqdn "${CLIO_HOST_NAME}")"
#  local -r clio_fqdn=$(get_real_clio_name "${clio_host_fqdn}")
#  echo "CLIO FQDN: ${clio_fqdn}"
  CLIO_PROJECT="broad-gotc-${ENV}"

  activate_service_account

  CLIO_INSTANCE=$(get_clio_instance ${CLIO_PROJECT})

  echo "CLIO INSTANCE IS: ${CLIO_INSTANCE}"
  local -r clio_fqdn=$(get_clio_fqdn ${CLIO_PROJECT} ${CLIO_INSTANCE})

#  # Temporary directory to store rendered configs.
#  local -r tmpdir=$(mktemp -d ${CLIO_DIR}/${PROG_NAME}-XXXXXX)
#  trap "rm -rf ${tmpdir}" ERR EXIT HUP INT TERM
#
#  render_ctmpls ${clio_fqdn} ${docker_tag} ${tmpdir}
#
#  deploy_clio_containers ${CLIO_PROJECT} ${CLIO_INSTANCE}

#  if poll_clio_health ${clio_fqdn} ${docker_tag}; then
#    ssh ${SSH_OPTS[@]} ${SSH_USER}@${clio_fqdn} "sudo rm -rf ${APP_BACKUP_BACKUP_DIR}"
#  else
#    >&2 echo Error: Clio failed to report expected health / version, rolling back deploy!
#    rollback_deploy ${clio_fqdn}
#    exit 1
#  fi
#
#  # Tag the current commit with the name of the environment that was just deployed to.
#  # This is a floating tag, so we have to use -f.
#  #
#  # NOTE: we can't push the tag here because Jenkins has to run this script with its
#  # GCE SSH key, which doesn't match its GitHub key. We push the tag as a follow-up
#  # step using the git publisher.
#  git tag -f ${ENV}
}

main
