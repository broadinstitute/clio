#!/usr/bin/env bash

set -ex

declare -r PROG_NAME=$(basename $0)
declare -r CLIO_DIR=$(cd $(dirname $(dirname $0)) && pwd)

declare -r -a VAULT_TOKEN_FILE_OPTIONS=(/etc/vault-token-dsde ${HOME}/.vault-token)

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
                  for example: "clio101", "clio201", "clio3", "clio"

Also requires that a file containing a vault token be placed at one of the following paths:

  $(join_by $'\n  ' ${VAULT_TOKEN_FILE_OPTIONS[@]})
EOF
}

check_env_args() {
  local ret=0

  if [ -z "$ENV" ]; then
    >&2 echo "Error: ENV not set!"
    ret=1
  else
    case "$ENV" in
      dev|staging|prod)
        ;;
      *)
        >&2 echo "Error: Invalid environment '$ENV'!"
        ret=1
        ;;
    esac
  fi

  if [ -z "$CLIO_HOST_NAME" ]; then
    >&2 echo "Error: CLIO_HOST_NAME not set!"
    ret=1
  fi

  if [ $ret -ne 0 ]; then
    usage
    exit 1
  fi
}

render_ctmpls() {
  local -r docker_tag=$1 clio_fqdn=$2 tmp=$3 app_dir=$4 host_log_dir=$5 pos_log_dir=$6

  local -r ctmpl_dir="${CLIO_DIR}/clio-server/config"
  local vault_token_file

  if [ ! -d "$ctmpl_dir" ]; then
    >&2 echo "Error: Missing ctmpl source directory ($ctmpl_dir) - Was it accidentally deleted?"
    exit 1
  fi

  # Try to find the vault token location.
  for token_location in ${VAULT_TOKEN_FILE_OPTIONS[@]}; do
    if [ -f $token_location ]; then
      vault_token_file=$token_location
    fi
  done

  if [ -z "$vault_token_file" ]; then
    >&2 echo "Error: Vault token file not found at any of $(join_by ', ' ${VAULT_TOKEN_FILE_OPTIONS[@]})"
    exit 1
  fi

  # Location in the containers where configs will be mounted.
  local -r clio_conf_dir=/etc
  # Location in the Clio container where logs will be mounted.
  local -r clio_log_dir=/logs

  # Name of the application config used by the Clio instance.
  local -r clio_app_conf=clio.conf
  # Name of the logback config used by the Clio instance.
  local -r clio_logback_conf=clio-logback.xml
  # Name of the service account JSON file used by the Clio instance.
  local -r clio_service_account_json=clio-service-account.json

  # Port to expose for Clio in the container.
  local -r container_clio_port=8080

  # Populate an env file with variables for rendering configs.
  local -r ctmpl_env_file="${tmp}/env-vars.txt"
  for var in ENV clio_fqdn docker_tag app_dir clio_conf_dir host_log_dir clio_log_dir pos_log_dir clio_app_conf clio_logback_conf clio_service_account_json container_clio_port; do
    # We use lower_snake_case for local variables in our script here, but our templates expect UPPER_SNAKE_CASE
    # (the script originally shared the same convention, and I haven't changed all the configs because I feel
    #  that uppercase makes the parts which should be filled in with an env variable more obvious).
    #
    # This line of bash-fu is used to avoid tediously listing out an `echo "UPPER_CASE_NAME=${lower_case_name}"`
    # command for each variable we want to pass along to the templates.
    #
    # In bash 4.X, ${var^^} substitutes to the uppercase form of the value stored in `var`. Ex:
    #
    #   var=some_variable
    #   echo "${var^^}" # Echoes "SOME_VARIABLE"
    #
    # ${!var} performs a double-substitution: first it substitutes `var` for its current value,
    # then it substitutes that value for its own current value. Ex:
    #
    #   some_variable=foo
    #   var=some_variable
    #   echo "${!var}" # Echoes "foo"
    #
    echo "${var^^}=${!var}" >> ${ctmpl_env_file}
  done

  # Copy ctmpls to temp space, then render them into configs.
  # Rendering has the side-effect of deleting the ctmpl files.
  cp -r ${ctmpl_dir}/* ${tmp}/
  docker run --rm \
    -v ${tmp}:/working \
    -v ${CLIO_DIR}/jenkins:/scripts \
    -v ${vault_token_file}:/root/.vault-token:ro \
    --env-file=${ctmpl_env_file} \
    broadinstitute/dsde-toolbox:latest /scripts/render-ctmpl.sh

  rm ${ctmpl_env_file}
}

compose_cmd() {
  echo "docker-compose -p clio-server -f ${1}/docker-compose.yml $2"
}

stop_containers() {
  local -r ssh_cmd=$1 app_dir=$2

  # Stop any running Clio network on the host.
  ${ssh_cmd} "test -f ${app_dir}/docker-compose.yml && $(compose_cmd ${app_dir} stop) || echo"

  # Remove any stopped containers.
  ${ssh_cmd} "test -f ${app_dir}/docker-compose.yml && $(compose_cmd ${app_dir} 'rm -f') || echo"
}

start_containers() {
  local -r ssh_cmd=$1 app_dir=$2

  # Pull the image versions specified in the compose file.
  ${ssh_cmd} "$(compose_cmd ${app_dir} pull)"

  # Bring up the container network in the background.
  ${ssh_cmd} "$(compose_cmd ${app_dir} 'up -d')"
}

deploy_containers() {
  local -r ssh_user=$1 clio_fqdn=$2 ssh_cmd=$3 scp_cmd=$4 tmp=$5 app_dir=$6 log_init_path=$7

  local -r new_app_staging_dir="${app_dir}.new"
  local -r previous_app_dir="${app_dir}.old"
  local -r previous_app_backup="${previous_app_dir}.old"

  # Clean up any leftover state from previous failed deploys.
  ${ssh_cmd} <<-EOF
  sudo rm -rf ${new_app_staging_dir} ${previous_app_backup} &&
  sudo mkdir ${new_app_staging_dir} &&
  sudo chgrp ${ssh_user} ${new_app_staging_dir} &&
  sudo chmod g+w ${new_app_staging_dir}
EOF

  # Copy rendered configs to the staging directory.
  ${scp_cmd} -r ${tmp}/* ${ssh_user}@${clio_fqdn}:${new_app_staging_dir}

  # Stop anything that's running.
  stop_containers "${ssh_cmd}" ${tmp}

  # Create backups, in case we need to roll back.
  ${ssh_cmd} <<-EOF
  ([ ! -d ${previous_app_dir} ] || sudo mv ${previous_app_dir} ${previous_app_backup}) &&
  ([ ! -d ${app_dir} ] || sudo mv ${app_dir} ${previous_app_dir}) &&
  sudo mv ${new_app_staging_dir} ${app_dir} &&
  sudo mkdir -p ${log_init_path}
EOF

  # Start up the new containers.
  start_containers "${ssh_cmd}" ${app_dir}
}

poll_clio_health() {
  local -r clio_fqdn=$1 docker_tag=$2

  local status
  local reported_version
  local attempts=1

  # Poll the Clio server every 20 seconds for 3 hours (540 times).
  # TODO: The server could be smarter about reporting recovery vs. error, allowing us
  # to shrink this polling time.
  while [[ "$status" != "OK" || "$reported_version" != "$docker_tag" ]]; do
    if [ ${attempts} -le 540 ]; then
      sleep 20
      echo "Attempt $attempts"
      status=$(curl -fs "https://${clio_fqdn}/health" | jq -r .search)
      reported_version=$(curl -fs "https://${clio_fqdn}/version" | jq -r .version)
      attempts=$((attempts + 1))
    else
      return 1
    fi
  done

  return 0
}

main() {
  check_env_args

  # Initialize vars used across multiple functions.

  # Compute the tag of the Docker image to deploy based on the current commit.
  # Assumes the image for the current commit has already been built & pushed.
  local -r base_version=$(cat "${CLIO_DIR}/.clio-version" | tr -d '\n')
  local -r short_sha=$(git rev-parse HEAD | cut -c1-7)
  local -r docker_tag="${base_version}-g${short_sha}-SNAP"

  # SSH options.
  local -r clio_fqdn="${CLIO_HOST_NAME}.gotc-${ENV}.broadinstitute.org"
  local -r ssh_user=jenkins
  local -r ssh_opts='-o UserKnownHostsFile=/dev/null -o CheckHostIP=no -o StrictHostKeyChecking=no'
  local -r ssh_cmd="ssh $ssh_opts ${ssh_user}@${clio_fqdn}"
  local -r scp_cmd="scp $ssh_opts"

  # Location on host where configs should be copied.
  local -r app_dir=/app
  # Location on the host where logs will be stored.
  local -r host_log_dir=/local/clio_logs
  # Relative path within the log dir where the fluentd pos logfile will be stored.
  local -r pos_log_dir=pos

  # Temporary directory to store rendered configs.
  local -r tmpdir=$(mktemp -d ${CLIO_DIR}/${PROG_NAME}-XXXXXX)
  trap "rm -r ${tmpdir}" ERR EXIT HUP INT TERM

  render_ctmpls ${docker_tag} ${clio_fqdn} ${tmpdir} ${app_dir} ${host_log_dir} ${pos_log_dir}

  deploy_containers ${ssh_user} ${clio_fqdn} "${ssh_cmd}" "${scp_cmd}" ${tmpdir} ${app_dir} "${host_log_dir}/${pos_log_dir}"

  if poll_clio_health ${clio_fqdn} ${docker_tag}; then
    ${ssh_cmd} "sudo rm -r ${app_dir}.old.old"
  else
    >&2 echo "Error: Clio failed to report expected health / version, rolling back deploy!"

    stop_containers "${ssh_cmd}" ${app_dir}
    ${ssh_cmd} <<-EOF
    sudo rm -r ${app_dir} &&
    ([ ! -d ${app_dir}.old || sudo mv ${app_dir}.old ${app_dir}) &&
    ([ ! -d ${app_dir}.old.old || sudo mv ${app_dir}.old.old ${app_dir}.old)
EOF
    start_containers "${ssh_cmd}" ${app_dir}

    exit 1
  fi

  # Tag the current commit with the name of the environment that was just deployed to.
  # This is a floating tag, so we have to use -f.
  #
  # NOTE: we can't push the tag here because Jenkins has to run this script with its
  # GCE SSH key, which doesn't match its GitHub key. We push the tag as a follow-up
  # step using the git publisher.
  git tag -f "$ENV"
}

main
