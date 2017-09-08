#!/bin/bash

set -e

# Show usage and fail if there is a problem with the command line.
#
function help {
    local -r av0="$1" environment="$2" verb="$3"
    local -r -a environments=(dev prod staging)
    local ok=no t v
    local -r -a tools=(curl date jq) verbs=(configure list take)
    local -r -a  usage=(
        ''
        "$av0: Snapshot an Elasticsearch (Es) cluster into GCS."
        ''
        "Usage: $av0 <environment> <verb>"
        ''
        "Where: <environment> is one of: ${environments[*]}"
        "       <verb> is one of: ${verbs[@]}."
        "       configure configures snapshots in the environment"
        "       list      lists the snapshots"
        "       take      takes a snapshot now"
        ''
        "Example: $av0 dev list"
        ''
        "BTW, you ran: $*"
        ''
    )
    for v in ${environments[*]}; do test $v = "$environment" && ok=yes; done
    if test $ok = no
    then
        echo 1>&2 "${av0}: Unrecognized environment: '$environment'"
    fi
    ok=no
    for v in ${verbs[*]}; do test $v = "$verb" && ok=yes; done
    if test $ok = no
    then
        echo 1>&2 "${av0}: Unrecognized verb: '$verb'"
    fi
    for t in "${tools[@]}"; do which $t >/dev/null || ok=no; done
    if test $ok = no
    then
        echo 1>&2 "${av0}: You need these tools on PATH: ${tools[*]}"
    fi
    if test $ok = no
    then
        for line in "${usage[@]}"; do echo "$line" 1>&2; done
        exit 1
    fi
}

# Report that $av0 is running the rest of arguments as a command line.
#
function echoRunJq () {
    local -r av0="$1" ; shift
    local -a command
    local arg result
    for arg in "$@"; do command+=("'$arg'"); done
    echo $av0: Running "${command[@]}"
    result=$("$@")
    echo "$result" | jq .
}

# Configure snapshots to $bucket in $environment for $es cluster.
#
function snapshot_configure () {
    local -r av0="$1" environment=$2 es=$3 repo=$4
    local -r bucket=broad-gotc-$environment-clio-elasticsearch
    local -r url=$es/_snapshot/$repo
    local -r data='
    {
        "type": "gcs",
        "settings": {
            "bucket": "'$bucket'",
            "service_account": "_default_",
            "compress": true,
            "application_name": "clio-elasticsearch"
        }
    }'
    local -a -r command=(curl -s -X PUT --data "$data" $url)
    echoRunJq "$av0" "${command[@]}"
}

# List the $repo snapshots in $environment for $es cluster.
#
function snapshot_list () {
    local -r av0="$1" environment=$2 es=$3 repo=$4
    local -a -r command=(curl -s $es/_snapshot/$repo/_all)
    echoRunJq "$av0" "${command[@]}"
}

# Take a snapshot in $environment for $es cluster.
#
function snapshot_take () {
    local -r av0="$1" environment=$2 es=$3 repo=$4
    local -r jq='keys|.[]|select(startswith(".")|not)'
    local -a -r indexes=($(curl -s $es/_aliases | jq --raw-output $jq))
    local -r now=clio-$(date -u +%Y-%m-%d-%H-%M-%S)
    local -r url=$es/_snapshot/$repo/$now?wait_for_completion=true
    local index indices comma
    for index in ${indexes[@]}; do indices=$indices$comma$index; comma=,; done
    local -r data='
    {
        "indices": "'$indices'",
        "include_global_state": false
    }'
    local -a -r command=(curl -s -X PUT --data "$data" $url)
    echoRunJq "$av0" "${command[@]}"
}

function main () {
    local -r av0="${0##*/}" environment="$1" verb="$2"
    local -r repo=clio-gcs-repository
    local -r domain=gotc-$environment.broadinstitute.org
    local -r es=http://elasticsearch1.$domain:9200
    help "$av0" "$@"
    snapshot_$verb "$av0" $environment $es $repo
}

main "$@"
