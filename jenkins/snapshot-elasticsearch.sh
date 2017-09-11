#!/bin/bash

set -e

# Show usage and fail if there is a problem with the command line.
#
function help {
    local -r av0="$1" environment="$2" verb="$3"
    local -r -a environments=(dev prod staging)
    local -r -a tools=(curl date jq)
    local -r -a verbs=(configure list take delete)
    local -r -a usage=(
        ''
        "$av0: Snapshot an Elasticsearch cluster into GCS."
        ''
        "Usage: $av0 <environment> <verb> [<snapshot> ...]"
        ''
        "Where: <environment> is one of: ${environments[*]}"
        "       <verb> is one of: ${verbs[*]}, and"
        "       <snapshot> is a snapshot name from 'list'"
        "       and where:"
        "       configure configures snapshots in environment,"
        "       list      lists the snapshots already taken, and"
        "       take      takes a snapshot now, and"
        "       delete    deletes the named snapshots."
        ''
        "Example: $av0 dev list"
        ''
        "BTW, you ran: $*"
        ''
    )
    local ok=no x
    for x in ${environments[*]}; do test $x = "$environment" && ok=yes; done
    if test $ok = no
    then
        echo 1>&2 "${av0}: Unrecognized environment: '$environment'"
    fi
    ok=no
    for x in ${verbs[*]}; do test $x = "$verb" && ok=yes; done
    if test $ok = no
    then
        echo 1>&2 "${av0}: Unrecognized verb: '$verb'"
    fi
    for x in "${tools[@]}"; do which $x >/dev/null || ok=no; done
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
# Then run that command line and pipe it through 'jq .'.
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

# Configure snapshots to $bucket in $environment for the $es cluster.
#
function snapshot_configure () {
    local -r av0="$1" environment=$2 es=$3 repo=$4
    local -r bucket=broad-gotc-$environment-clio-elasticsearch
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
    echoRunJq "$av0" curl -s -X PUT --data "$data" $es/_snapshot/$repo
}

# List the $repo snapshots in $environment for $es cluster.
#
function snapshot_list () {
    local -r av0="$1" environment=$2 es=$3 repo=$4
    echoRunJq "$av0" curl -s $es/_snapshot/$repo/_all
}

# Take a snapshot in $environment for $es cluster.
#
function snapshot_take () {
    local -r av0="$1" environment=$2 es=$3 repo=$4
    local -r jq='keys|.[]|select(startswith(".")|not)'
    local -r snapshot=clio-$(date -u +%Y-%m-%d-%H-%M-%S)
    local -r url=$es/_snapshot/$repo/$snapshot?wait_for_completion=true
    local -a -r curl=(curl -s $es/_aliases)
    local -r indexes=$("${curl[@]}" | jq --raw-output $jq)
    if test -z "$indexes"
    then
        echo 1>&2 $av0: No document indexes in $es
        echoRunJq "$av0" "${curl[@]}"
        exit 2
    fi
    local index indices comma
    for index in $indexes; do indices=$indices$comma$index; comma=,; done
    local -r data='
    {
        "indices": "'$indices'",
        "include_global_state": false
    }'
    echoRunJq "$av0" curl -s -X PUT --data "$data" $url
}

# Delete the snapshots named in $5 ... and so on.
#
function snapshot_delete () {
    local -r av0="$1" environment=$2 es=$3 repo=$4 ; shift 4
    for snap in "$@"
    do
        echoRunJq "$av0" curl -s -X DELETE $es/_snapshot/$repo/$snap
    done
}

function main () {
    local -r av0="${0##*/}" environment="$1" verb="$2"
    help "$av0" "$@" ; shift 2
    local -r domain=gotc-$environment.broadinstitute.org
    local -r es=http://elasticsearch1.$domain:9200
    snapshot_$verb "$av0" $environment $es clio-gcs-repository "$@"
}

main "$@"
